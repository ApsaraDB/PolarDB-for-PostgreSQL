/*-------------------------------------------------------------------------
 *
 * method_io_uring.c
 *    AIO - perform AIO using Linux' io_uring
 *
 * For now we create one io_uring instance for each backend. These io_uring
 * instances have to be created in postmaster, during startup, to allow other
 * backends to process IO completions, if the issuing backend is currently
 * busy doing other things. Other backends may not use another backend's
 * io_uring instance to submit IO, that'd require additional locking that
 * would likely be harmful for performance.
 *
 * We likely will want to introduce a backend-local io_uring instance in the
 * future, e.g. for FE/BE network IO.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *    src/backend/storage/aio/method_io_uring.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

/* included early, for IOMETHOD_IO_URING_ENABLED */
#include "storage/aio.h"

#ifdef IOMETHOD_IO_URING_ENABLED

#include <sys/mman.h>
#include <unistd.h>

#include <liburing.h>

#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "utils/wait_event.h"


/* number of completions processed at once */
#define PGAIO_MAX_LOCAL_COMPLETED_IO 32


/* Entry points for IoMethodOps. */
static size_t pgaio_uring_shmem_size(void);
static void pgaio_uring_shmem_init(bool first_time);
static void pgaio_uring_init_backend(void);
static int	pgaio_uring_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);
static void pgaio_uring_wait_one(PgAioHandle *ioh, uint64 ref_generation);

/* helper functions */
static void pgaio_uring_sq_from_io(PgAioHandle *ioh, struct io_uring_sqe *sqe);


const IoMethodOps pgaio_uring_ops = {
	/*
	 * While io_uring mostly is OK with FDs getting closed while the IO is in
	 * flight, that is not true for IOs submitted with IOSQE_ASYNC.
	 *
	 * See
	 * https://postgr.es/m/5ons2rtmwarqqhhexb3dnqulw5rjgwgoct57vpdau4rujlrffj%403fls6d2mkiwc
	 */
	.wait_on_fd_before_close = true,

	.shmem_size = pgaio_uring_shmem_size,
	.shmem_init = pgaio_uring_shmem_init,
	.init_backend = pgaio_uring_init_backend,

	.submit = pgaio_uring_submit,
	.wait_one = pgaio_uring_wait_one,
};

/*
 * Per-backend state when using io_method=io_uring
 *
 * Align the whole struct to a cacheline boundary, to prevent false sharing
 * between completion_lock and prior backend's io_uring_ring.
 */
typedef struct pg_attribute_aligned (PG_CACHE_LINE_SIZE)
PgAioUringContext
{
	/*
	 * Multiple backends can process completions for this backend's io_uring
	 * instance (e.g. when the backend issuing IO is busy doing something
	 * else).  To make that safe we have to ensure that only a single backend
	 * gets io completions from the io_uring instance at a time.
	 */
	LWLock		completion_lock;

	struct io_uring io_uring_ring;
} PgAioUringContext;

/*
 * Information about the capabilities that io_uring has.
 *
 * Depending on liburing and kernel version different features are
 * supported. At least for the kernel a kernel version check does not suffice
 * as various vendors do backport features to older kernels :(.
 */
typedef struct PgAioUringCaps
{
	bool		checked;
	/* -1 if io_uring_queue_init_mem() is unsupported */
	int			mem_init_size;
} PgAioUringCaps;


/* PgAioUringContexts for all backends */
static PgAioUringContext *pgaio_uring_contexts;

/* the current backend's context */
static PgAioUringContext *pgaio_my_uring_context;

static PgAioUringCaps pgaio_uring_caps =
{
	.checked = false,
	.mem_init_size = -1,
};

static uint32
pgaio_uring_procs(void)
{
	/*
	 * We can subtract MAX_IO_WORKERS here as io workers are never used at the
	 * same time as io_method=io_uring.
	 */
	return MaxBackends + NUM_AUXILIARY_PROCS - MAX_IO_WORKERS;
}

/*
 * Initializes pgaio_uring_caps, unless that's already done.
 */
static void
pgaio_uring_check_capabilities(void)
{
	if (pgaio_uring_caps.checked)
		return;

	/*
	 * By default io_uring creates a shared memory mapping for each io_uring
	 * instance, leading to a large number of memory mappings. Unfortunately a
	 * large number of memory mappings slows things down, backend exit is
	 * particularly affected.  To address that, newer kernels (6.5) support
	 * using user-provided memory for the memory, by putting the relevant
	 * memory into shared memory we don't need any additional mappings.
	 *
	 * To know whether this is supported, we unfortunately need to probe the
	 * kernel by trying to create a ring with userspace-provided memory. This
	 * also has a secondary benefit: We can determine precisely how much
	 * memory we need for each io_uring instance.
	 */
#if defined(HAVE_LIBURING_QUEUE_INIT_MEM) && defined(IORING_SETUP_NO_MMAP)
	{
		struct io_uring test_ring;
		size_t		ring_size;
		void	   *ring_ptr;
		struct io_uring_params p = {0};
		int			ret;

		/*
		 * Liburing does not yet provide an API to query how much memory a
		 * ring will need. So we over-estimate it here. As the memory is freed
		 * just below that's small temporary waste of memory.
		 *
		 * 1MB is more than enough for rings within io_max_concurrency's
		 * range.
		 */
		ring_size = 1024 * 1024;

		/*
		 * Hard to believe a system exists where 1MB would not be a multiple
		 * of the page size. But it's cheap to ensure...
		 */
		ring_size -= ring_size % sysconf(_SC_PAGESIZE);

		ring_ptr = mmap(NULL, ring_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
		if (ring_ptr == MAP_FAILED)
			elog(ERROR,
				 "mmap(%zu) to determine io_uring_queue_init_mem() support failed: %m",
				 ring_size);

		ret = io_uring_queue_init_mem(io_max_concurrency, &test_ring, &p, ring_ptr, ring_size);
		if (ret > 0)
		{
			pgaio_uring_caps.mem_init_size = ret;

			elog(DEBUG1,
				 "can use combined memory mapping for io_uring, each ring needs %d bytes",
				 ret);

			/* clean up the created ring, it was just for a test */
			io_uring_queue_exit(&test_ring);
		}
		else
		{
			/*
			 * There are different reasons for ring creation to fail, but it's
			 * ok to treat that just as io_uring_queue_init_mem() not being
			 * supported. We'll report a more detailed error in
			 * pgaio_uring_shmem_init().
			 */
			errno = -ret;
			elog(DEBUG1,
				 "cannot use combined memory mapping for io_uring, ring creation failed: %m");

		}

		if (munmap(ring_ptr, ring_size) != 0)
			elog(ERROR, "munmap() failed: %m");
	}
#else
	{
		elog(DEBUG1,
			 "can't use combined memory mapping for io_uring, kernel or liburing too old");
	}
#endif

	pgaio_uring_caps.checked = true;
}

/*
 * Memory for all PgAioUringContext instances
 */
static size_t
pgaio_uring_context_shmem_size(void)
{
	return mul_size(pgaio_uring_procs(), sizeof(PgAioUringContext));
}

/*
 * Memory for the combined memory used by io_uring instances. Returns 0 if
 * that is not supported by kernel/liburing.
 */
static size_t
pgaio_uring_ring_shmem_size(void)
{
	size_t		sz = 0;

	if (pgaio_uring_caps.mem_init_size > 0)
	{
		/*
		 * Memory for rings needs to be allocated to the page boundary,
		 * reserve space. Luckily it does not need to be aligned to hugepage
		 * boundaries, even if huge pages are used.
		 */
		sz = add_size(sz, sysconf(_SC_PAGESIZE));
		sz = add_size(sz, mul_size(pgaio_uring_procs(),
								   pgaio_uring_caps.mem_init_size));
	}

	return sz;
}

static size_t
pgaio_uring_shmem_size(void)
{
	size_t		sz;

	/*
	 * Kernel and liburing support for various features influences how much
	 * shmem we need, perform the necessary checks.
	 */
	pgaio_uring_check_capabilities();

	sz = pgaio_uring_context_shmem_size();
	sz = add_size(sz, pgaio_uring_ring_shmem_size());

	return sz;
}

static void
pgaio_uring_shmem_init(bool first_time)
{
	int			TotalProcs = pgaio_uring_procs();
	bool		found;
	char	   *shmem;
	size_t		ring_mem_remain = 0;
	char	   *ring_mem_next = 0;

	/*
	 * We allocate memory for all PgAioUringContext instances and, if
	 * supported, the memory required for each of the io_uring instances, in
	 * one ShmemInitStruct().
	 */
	shmem = ShmemInitStruct("AioUringContext", pgaio_uring_shmem_size(), &found);
	if (found)
		return;

	pgaio_uring_contexts = (PgAioUringContext *) shmem;
	shmem += pgaio_uring_context_shmem_size();

	/* if supported, handle memory alignment / sizing for io_uring memory */
	if (pgaio_uring_caps.mem_init_size > 0)
	{
		ring_mem_remain = pgaio_uring_ring_shmem_size();
		ring_mem_next = (char *) shmem;

		/* align to page boundary, see also pgaio_uring_ring_shmem_size() */
		ring_mem_next = (char *) TYPEALIGN(sysconf(_SC_PAGESIZE), ring_mem_next);

		/* account for alignment */
		ring_mem_remain -= ring_mem_next - shmem;
		shmem += ring_mem_next - shmem;

		shmem += ring_mem_remain;
	}

	for (int contextno = 0; contextno < TotalProcs; contextno++)
	{
		PgAioUringContext *context = &pgaio_uring_contexts[contextno];
		int			ret;

		/*
		 * Right now a high TotalProcs will cause problems in two ways:
		 *
		 * - RLIMIT_NOFILE needs to be big enough to allow all
		 * io_uring_queue_init() calls to succeed.
		 *
		 * - RLIMIT_NOFILE needs to be big enough to still have enough file
		 * descriptors to satisfy set_max_safe_fds() left over. Or, even
		 * better, have max_files_per_process left over FDs.
		 *
		 * We probably should adjust the soft RLIMIT_NOFILE to ensure that.
		 *
		 *
		 * XXX: Newer versions of io_uring support sharing the workers that
		 * execute some asynchronous IOs between io_uring instances. It might
		 * be worth using that - also need to evaluate if that causes
		 * noticeable additional contention?
		 */

		/*
		 * If supported (c.f. pgaio_uring_check_capabilities()), create ring
		 * with its data in shared memory. Otherwise fall back io_uring
		 * creating a memory mapping for each ring.
		 */
#if defined(HAVE_LIBURING_QUEUE_INIT_MEM) && defined(IORING_SETUP_NO_MMAP)
		if (pgaio_uring_caps.mem_init_size > 0)
		{
			struct io_uring_params p = {0};

			ret = io_uring_queue_init_mem(io_max_concurrency, &context->io_uring_ring, &p, ring_mem_next, ring_mem_remain);

			ring_mem_remain -= ret;
			ring_mem_next += ret;
		}
		else
#endif
		{
			ret = io_uring_queue_init(io_max_concurrency, &context->io_uring_ring, 0);
		}

		if (ret < 0)
		{
			char	   *hint = NULL;
			int			err = ERRCODE_INTERNAL_ERROR;

			/* add hints for some failures that errno explains sufficiently */
			if (-ret == EPERM)
			{
				err = ERRCODE_INSUFFICIENT_PRIVILEGE;
				hint = _("Check if io_uring is disabled via /proc/sys/kernel/io_uring_disabled.");
			}
			else if (-ret == EMFILE)
			{
				err = ERRCODE_INSUFFICIENT_RESOURCES;
				hint = psprintf(_("Consider increasing \"ulimit -n\" to at least %d."),
								TotalProcs + max_files_per_process);
			}
			else if (-ret == ENOSYS)
			{
				err = ERRCODE_FEATURE_NOT_SUPPORTED;
				hint = _("Kernel does not support io_uring.");
			}

			/* update errno to allow %m to work */
			errno = -ret;

			ereport(ERROR,
					errcode(err),
					errmsg("could not setup io_uring queue: %m"),
					hint != NULL ? errhint("%s", hint) : 0);
		}

		LWLockInitialize(&context->completion_lock, LWTRANCHE_AIO_URING_COMPLETION);
	}
}

static void
pgaio_uring_init_backend(void)
{
	Assert(MyProcNumber < pgaio_uring_procs());

	pgaio_my_uring_context = &pgaio_uring_contexts[MyProcNumber];
}

static int
pgaio_uring_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	struct io_uring *uring_instance = &pgaio_my_uring_context->io_uring_ring;
	int			in_flight_before = dclist_count(&pgaio_my_backend->in_flight_ios);

	Assert(num_staged_ios <= PGAIO_SUBMIT_BATCH_SIZE);

	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		struct io_uring_sqe *sqe;

		sqe = io_uring_get_sqe(uring_instance);

		if (!sqe)
			elog(ERROR, "io_uring submission queue is unexpectedly full");

		pgaio_io_prepare_submit(ioh);
		pgaio_uring_sq_from_io(ioh, sqe);

		/*
		 * io_uring executes IO in process context if possible. That's
		 * generally good, as it reduces context switching. When performing a
		 * lot of buffered IO that means that copying between page cache and
		 * userspace memory happens in the foreground, as it can't be
		 * offloaded to DMA hardware as is possible when using direct IO. When
		 * executing a lot of buffered IO this causes io_uring to be slower
		 * than worker mode, as worker mode parallelizes the copying. io_uring
		 * can be told to offload work to worker threads instead.
		 *
		 * If an IO is buffered IO and we already have IOs in flight or
		 * multiple IOs are being submitted, we thus tell io_uring to execute
		 * the IO in the background. We don't do so for the first few IOs
		 * being submitted as executing in this process' context has lower
		 * latency.
		 */
		if (in_flight_before > 4 && (ioh->flags & PGAIO_HF_BUFFERED))
			io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);

		in_flight_before++;
	}

	while (true)
	{
		int			ret;

		pgstat_report_wait_start(WAIT_EVENT_AIO_IO_URING_SUBMIT);
		ret = io_uring_submit(uring_instance);
		pgstat_report_wait_end();

		if (ret == -EINTR)
		{
			pgaio_debug(DEBUG3,
						"aio method uring: submit EINTR, nios: %d",
						num_staged_ios);
		}
		else if (ret < 0)
		{
			/*
			 * The io_uring_enter() manpage suggests that the appropriate
			 * reaction to EAGAIN is:
			 *
			 * "The application should wait for some completions and try
			 * again"
			 *
			 * However, it seems unlikely that that would help in our case, as
			 * we apply a low limit to the number of outstanding IOs and thus
			 * also outstanding completions, making it unlikely that we'd get
			 * EAGAIN while the OS is in good working order.
			 *
			 * Additionally, it would be problematic to just wait here, our
			 * caller might hold critical locks. It'd possibly lead to
			 * delaying the crash-restart that seems likely to occur when the
			 * kernel is under such heavy memory pressure.
			 *
			 * Update errno to allow %m to work.
			 */
			errno = -ret;
			elog(PANIC, "io_uring submit failed: %m");
		}
		else if (ret != num_staged_ios)
		{
			/* likely unreachable, but if it is, we would need to re-submit */
			elog(PANIC, "io_uring submit submitted only %d of %d",
				 ret, num_staged_ios);
		}
		else
		{
			pgaio_debug(DEBUG4,
						"aio method uring: submitted %d IOs",
						num_staged_ios);
			break;
		}
	}

	return num_staged_ios;
}

static void
pgaio_uring_completion_error_callback(void *arg)
{
	ProcNumber	owner;
	PGPROC	   *owner_proc;
	int32		owner_pid;
	PgAioHandle *ioh = arg;

	if (!ioh)
		return;

	/* No need for context if a backend is completing the IO for itself */
	if (ioh->owner_procno == MyProcNumber)
		return;

	owner = ioh->owner_procno;
	owner_proc = GetPGProcByNumber(owner);
	owner_pid = owner_proc->pid;

	errcontext("completing I/O on behalf of process %d", owner_pid);
}

static void
pgaio_uring_drain_locked(PgAioUringContext *context)
{
	int			ready;
	int			orig_ready;
	ErrorContextCallback errcallback = {0};

	Assert(LWLockHeldByMeInMode(&context->completion_lock, LW_EXCLUSIVE));

	errcallback.callback = pgaio_uring_completion_error_callback;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/*
	 * Don't drain more events than available right now. Otherwise it's
	 * plausible that one backend could get stuck, for a while, receiving CQEs
	 * without actually processing them.
	 */
	orig_ready = ready = io_uring_cq_ready(&context->io_uring_ring);

	while (ready > 0)
	{
		struct io_uring_cqe *cqes[PGAIO_MAX_LOCAL_COMPLETED_IO];
		uint32		ncqes;

		START_CRIT_SECTION();
		ncqes =
			io_uring_peek_batch_cqe(&context->io_uring_ring,
									cqes,
									Min(PGAIO_MAX_LOCAL_COMPLETED_IO, ready));
		Assert(ncqes <= ready);

		ready -= ncqes;

		for (int i = 0; i < ncqes; i++)
		{
			struct io_uring_cqe *cqe = cqes[i];
			PgAioHandle *ioh;

			ioh = io_uring_cqe_get_data(cqe);
			errcallback.arg = ioh;
			io_uring_cqe_seen(&context->io_uring_ring, cqe);

			pgaio_io_process_completion(ioh, cqe->res);
			errcallback.arg = NULL;
		}

		END_CRIT_SECTION();

		pgaio_debug(DEBUG3,
					"drained %d/%d, now expecting %d",
					ncqes, orig_ready, io_uring_cq_ready(&context->io_uring_ring));
	}

	error_context_stack = errcallback.previous;
}

static void
pgaio_uring_wait_one(PgAioHandle *ioh, uint64 ref_generation)
{
	PgAioHandleState state;
	ProcNumber	owner_procno = ioh->owner_procno;
	PgAioUringContext *owner_context = &pgaio_uring_contexts[owner_procno];
	bool		expect_cqe;
	int			waited = 0;

	/*
	 * XXX: It would be nice to have a smarter locking scheme, nearly all the
	 * time the backend owning the ring will consume the completions, making
	 * the locking unnecessarily expensive.
	 */
	LWLockAcquire(&owner_context->completion_lock, LW_EXCLUSIVE);

	while (true)
	{
		pgaio_debug_io(DEBUG3, ioh,
					   "wait_one io_gen: %" PRIu64 ", ref_gen: %" PRIu64 ", cycle %d",
					   ioh->generation,
					   ref_generation,
					   waited);

		if (pgaio_io_was_recycled(ioh, ref_generation, &state) ||
			state != PGAIO_HS_SUBMITTED)
		{
			/* the IO was completed by another backend */
			break;
		}
		else if (io_uring_cq_ready(&owner_context->io_uring_ring))
		{
			/* no need to wait in the kernel, io_uring has a completion */
			expect_cqe = true;
		}
		else
		{
			int			ret;
			struct io_uring_cqe *cqes;

			/* need to wait in the kernel */
			pgstat_report_wait_start(WAIT_EVENT_AIO_IO_URING_EXECUTION);
			ret = io_uring_wait_cqes(&owner_context->io_uring_ring, &cqes, 1, NULL, NULL);
			pgstat_report_wait_end();

			if (ret == -EINTR)
			{
				continue;
			}
			else if (ret != 0)
			{
				/* see comment after io_uring_submit() */
				errno = -ret;
				elog(PANIC, "io_uring wait failed: %m");
			}
			else
			{
				Assert(cqes != NULL);
				expect_cqe = true;
				waited++;
			}
		}

		if (expect_cqe)
		{
			pgaio_uring_drain_locked(owner_context);
		}
	}

	LWLockRelease(&owner_context->completion_lock);

	pgaio_debug(DEBUG3,
				"wait_one with %d sleeps",
				waited);
}

static void
pgaio_uring_sq_from_io(PgAioHandle *ioh, struct io_uring_sqe *sqe)
{
	struct iovec *iov;

	switch (ioh->op)
	{
		case PGAIO_OP_READV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
			if (ioh->op_data.read.iov_length == 1)
			{
				io_uring_prep_read(sqe,
								   ioh->op_data.read.fd,
								   iov->iov_base,
								   iov->iov_len,
								   ioh->op_data.read.offset);
			}
			else
			{
				io_uring_prep_readv(sqe,
									ioh->op_data.read.fd,
									iov,
									ioh->op_data.read.iov_length,
									ioh->op_data.read.offset);

			}
			break;

		case PGAIO_OP_WRITEV:
			iov = &pgaio_ctl->iovecs[ioh->iovec_off];
			if (ioh->op_data.write.iov_length == 1)
			{
				io_uring_prep_write(sqe,
									ioh->op_data.write.fd,
									iov->iov_base,
									iov->iov_len,
									ioh->op_data.write.offset);
			}
			else
			{
				io_uring_prep_writev(sqe,
									 ioh->op_data.write.fd,
									 iov,
									 ioh->op_data.write.iov_length,
									 ioh->op_data.write.offset);
			}
			break;

		case PGAIO_OP_INVALID:
			elog(ERROR, "trying to prepare invalid IO operation for execution");
	}

	io_uring_sqe_set_data(sqe, ioh);
}

#endif							/* IOMETHOD_IO_URING_ENABLED */
