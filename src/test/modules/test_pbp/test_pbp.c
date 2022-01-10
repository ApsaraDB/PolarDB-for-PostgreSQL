#include "postgres.h"

#include "access/xlog.h"
#include "fmgr.h"
#include "storage/bufmgr.h"
#include "storage/polar_bufmgr.h"
#include "storage/polar_flushlist.h"
#include "storage/polar_pbp.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

static Buffer
get_buffer_from_flushlist(void)
{
	int	first;

	/* Do not hold this lock too much time. */
	SpinLockAcquire(&polar_flush_list_ctl->flushlist_lock);
	first = polar_flush_list_ctl->first_flush_buffer;

	/* Flushlist is empty. */
	if (first == POLAR_FLUSHNEXT_END_OF_LIST)
	{
		SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);
		return InvalidBuffer;
	}
	SpinLockRelease(&polar_flush_list_ctl->flushlist_lock);
	return first;
}

static void
check_buffer_with_exclusive_lock(void)
{
	Buffer	buf;
	bool	res;
	BufferDesc *buf_desc;
	XLogRecPtr oldest_lsn;

	/* Get one buffer from flush list. */
	buf = get_buffer_from_flushlist();

	if (BufferIsInvalid(buf))
		return;

	buf_desc = GetBufferDescriptor(buf);

	/* Try to lock the buffer with LW_EXCLUSIVE */
	res = polar_conditional_lock_buffer_ext(buf, false);

	if (!res)
		return;

	/* lock successfully */

	/* The buffer should have unrecoverable flag. */
	Assert(buf_desc->polar_flags & POLAR_BUF_UNRECOVERABLE);
	oldest_lsn = polar_buffer_get_oldest_lsn(buf_desc);

	if (!XLogRecPtrIsInvalid(oldest_lsn))
		Assert(oldest_lsn >= polar_buffer_pool_ctl->last_checkpoint_lsn);

	Assert(!XLogRecPtrIsInvalid(BufferGetLSN(buf_desc)));

	polar_lock_buffer_ext(buf, BUFFER_LOCK_UNLOCK, false);
}

static void
check_buffer_with_share_lock(void)
{
	Buffer	buf;
	bool	res;
	BufferDesc	*buf_desc;
	XLogRecPtr	oldest_lsn;
	XLogRecPtr	latest_lsn;
	uint32 		state;

	buf = get_buffer_from_flushlist();

	if (BufferIsInvalid(buf))
		return;

	buf_desc = GetBufferDescriptor(buf);

	/* Try to lock the buffer with LW_SHARE */
	res = LWLockConditionalAcquire(BufferDescriptorGetContentLock(buf_desc),
								   LW_SHARED);
	if (!res)
		return;

	/* lock successfully */
	state = LockBufHdr(buf_desc);
	UnlockBufHdr(buf_desc, state);

	oldest_lsn = polar_buffer_get_oldest_lsn(buf_desc);
	if (!XLogRecPtrIsInvalid(oldest_lsn))
		Assert(oldest_lsn >= polar_buffer_pool_ctl->last_checkpoint_lsn);

	Assert(!(buf_desc->polar_flags & POLAR_BUF_UNRECOVERABLE));

	latest_lsn = BufferGetLSN(buf_desc);
	if (latest_lsn > polar_buffer_pool_ctl->last_flush_lsn)
		Assert(!polar_buffer_can_be_reused(buf_desc, state));

	XLogFlush(latest_lsn);
	polar_buffer_pool_ctl_set_last_flush_lsn(latest_lsn);

#define FLAGS_NO_REUSED (BM_IO_IN_PROGRESS|BM_LOCKED|BM_IO_ERROR|BM_PIN_COUNT_WAITER)

	if (state & BUF_FLAG_MASK & FLAGS_NO_REUSED)
		Assert(!polar_buffer_can_be_reused(buf_desc, state));

	/* clean flags that make buffer can not be reused. */
	state &= ~FLAGS_NO_REUSED;

#define FLAGS_MUST_BE_SET (BM_VALID | BM_TAG_VALID | BM_PERMANENT)
	if ((state & FLAGS_MUST_BE_SET) != FLAGS_MUST_BE_SET)
		Assert(!polar_buffer_can_be_reused(buf_desc, state));

	/* Set flags that must be set. */
	state |= FLAGS_MUST_BE_SET;

	/*
	 * Now, the buffer can be reused, but we do not really change the state
	 * at buffer, only use a fake state value.
	 */
	Assert(polar_buffer_can_be_reused(buf_desc, state));

	polar_lock_buffer_ext(buf, BUFFER_LOCK_UNLOCK, false);
}

#define verify_pbp_ctl(name) \
	do { \
		polar_buffer_pool_ctl->name = old_ctl->name + 1; \
		Assert(!polar_verify_buffer_pool_ctl()); \
		polar_buffer_pool_ctl->name = old_ctl->name; \
	} while(0)

static void
check_buffer_pool_ctl(void)
{
	Size pbp_ctl_size = 0;
	polar_buffer_pool_ctl_t *old_ctl = NULL;
	uint32 state;

	Assert(polar_verify_buffer_pool_ctl());

	pbp_ctl_size = sizeof(polar_buffer_pool_ctl_padded);
	old_ctl = (polar_buffer_pool_ctl_t *) malloc(pbp_ctl_size);
	memcpy(old_ctl, polar_buffer_pool_ctl, pbp_ctl_size);

	SpinLockAcquire(&polar_buffer_pool_ctl->lock);
	verify_pbp_ctl(version);
	verify_pbp_ctl(header_magic);
	verify_pbp_ctl(system_identifier);
	verify_pbp_ctl(last_checkpoint_lsn);
	verify_pbp_ctl(nbuffers);
	verify_pbp_ctl(buf_desc_size);
	verify_pbp_ctl(buffer_pool_ctl_shmem_size);
	verify_pbp_ctl(buffer_pool_shmem_size);
	verify_pbp_ctl(copy_buffer_shmem_size);
	verify_pbp_ctl(flush_list_ctl_shmem_size);
	verify_pbp_ctl(strategy_shmem_size);
	verify_pbp_ctl(tail_magic);

	/* verify state */
	Assert(polar_buffer_pool_ctl_get_node_type() == POLAR_MASTER);
	state = polar_buffer_pool_ctl->state;

	polar_buffer_pool_ctl->state = 0;
	polar_buffer_pool_ctl_set_node_type(POLAR_UNKNOWN);
	Assert(!polar_verify_buffer_pool_ctl());

	polar_buffer_pool_ctl->state = 0;
	polar_buffer_pool_ctl_set_node_type(POLAR_REPLICA);
	Assert(!polar_verify_buffer_pool_ctl());

	polar_buffer_pool_ctl->state = 0;
	polar_buffer_pool_ctl_set_node_type(POLAR_STANDBY);
	Assert(!polar_verify_buffer_pool_ctl());

	polar_local_node_type = POLAR_STANDBY;
	Assert(polar_verify_buffer_pool_ctl());

	polar_buffer_pool_ctl->state = 0;
	polar_buffer_pool_ctl_set_node_type(POLAR_STANDALONE_DATAMAX);
	Assert(polar_verify_buffer_pool_ctl());

	/* Reset the old state. */
	polar_buffer_pool_ctl->state = state;
	Assert(polar_verify_buffer_pool_ctl());

	Assert(polar_buffer_pool_ctl_get_error_state() == 0);
	polar_buffer_pool_ctl_set_error_state();
	Assert(!polar_verify_buffer_pool_ctl());
	polar_buffer_pool_ctl->state = state;
	SpinLockRelease(&polar_buffer_pool_ctl->lock);

	free(old_ctl);

	Assert(polar_verify_buffer_pool_ctl());
}

PG_FUNCTION_INFO_V1(test_pbp);
/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_pbp(PG_FUNCTION_ARGS)
{
	if (!polar_enable_shared_storage_mode ||
		!polar_flush_list_enabled())
		PG_RETURN_VOID();

	Assert(polar_buffer_pool_is_inited);
	Assert(polar_verify_buffer_pool_ctl());

	check_buffer_with_exclusive_lock();
	check_buffer_with_share_lock();
	check_buffer_pool_ctl();

	PG_RETURN_VOID();
}
