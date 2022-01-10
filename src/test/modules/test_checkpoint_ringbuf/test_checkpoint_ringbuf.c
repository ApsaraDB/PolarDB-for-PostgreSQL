#include "postgres.h"

#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "fmgr.h"
#include "catalog/pg_control.h"
#include "access/polar_checkpoint_ringbuf.h"

#define TEST_SIZE 10

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_checkpoint_ringbuf);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_checkpoint_ringbuf(PG_FUNCTION_ARGS)
{
	
	polar_checkpoint_ringbuf_t ringbuf;
	int i;
	XLogRecPtr redoBase, startBase, endBase, gap;
	CheckPoint checkpoint[TEST_SIZE];
	XLogRecPtr start[TEST_SIZE];
	XLogRecPtr end[TEST_SIZE];
	CheckPoint checkpointReturn;
	XLogRecPtr startReturn;
	XLogRecPtr endReturn;

	memset(&ringbuf, 0, sizeof(polar_checkpoint_ringbuf_t));

	checkpoint[0].redo	= 0x5000000;
	start[0]			= 0x50001000;
	end[0]				= 0x50002000;
	srand(time(NULL));

	for (i = 0; i < TEST_SIZE; i++) 
	{
		gap = rand() % 0x990000 + 0x10000;
		redoBase += gap;
		startBase += gap;
		endBase += gap;
		checkpoint[i].redo = redoBase;
		start[i] = startBase;
		end[i] = endBase;
	}

	polar_checkpoint_ringbuf_init(&ringbuf);
	polar_checkpoint_ringbuf_shrink(&ringbuf, 0);
	// after shrink, ringbuf: []
	Assert(ringbuf.size == 0);

	polar_checkpoint_ringbuf_insert(&ringbuf, start[0], end[0], &checkpoint[0]);
	// after insert, ringbuf: [0]
	polar_checkpoint_ringbuf_shrink(&ringbuf, 0);
	// after shrink, ringbuf: [0]
	Assert(ringbuf.size == 1);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[0].redo);
	Assert(startReturn == start[0]);
	Assert(endReturn == end[0]);

	for (i = 1; i < 5; i++) 
		polar_checkpoint_ringbuf_insert(&ringbuf, start[i], end[i], &checkpoint[i]);
	// after insert, ringbuf: [0,1,2,3,4]
	Assert(ringbuf.size == 5);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[0].redo);
	Assert(startReturn == start[0]);
	Assert(endReturn == end[0]);

	polar_checkpoint_ringbuf_shrink(&ringbuf, checkpoint[4].redo);
	// after shrink, ringbuf: [4]
	Assert(ringbuf.size == 1);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[4].redo);
	Assert(startReturn == start[4]);
	Assert(endReturn == end[4]);

	polar_checkpoint_ringbuf_shrink(&ringbuf, checkpoint[5].redo);
	// after shrink, ringbuf: [4]
	Assert(ringbuf.size == 1);

	polar_checkpoint_ringbuf_insert(&ringbuf, start[0], end[0], &checkpoint[0]);
	Assert(ringbuf.size == 1);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[4].redo);
	Assert(startReturn == start[4]);
	Assert(endReturn == end[4]);

	for (; i < TEST_SIZE; i++) 
		polar_checkpoint_ringbuf_insert(&ringbuf, start[i], end[i], &checkpoint[i]);
	// after insert, ringbuf: [4,5,6,7,9] 
	// (8 will be evicted, only work under the current eviction strategy)
	Assert(ringbuf.size == 5);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[4].redo);
	Assert(startReturn == start[4]);
	Assert(endReturn == end[4]);

	polar_checkpoint_ringbuf_shrink(&ringbuf, checkpoint[7].redo);
	// after shrink, ringbuf: [7,9]
	Assert(ringbuf.size == 2);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[7].redo);
	Assert(startReturn == start[7]);
	Assert(endReturn == end[7]);

	polar_checkpoint_ringbuf_shrink(&ringbuf, checkpoint[8].redo);
	// after shrink, ringbuf: [7,9]
	Assert(ringbuf.size == 2);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[7].redo);
	Assert(startReturn == start[7]);
	Assert(endReturn == end[7]);

	polar_checkpoint_ringbuf_shrink(&ringbuf, checkpoint[9].redo);
	// after shrink, ringbuf: [9]
	Assert(ringbuf.size == 1);

	polar_checkpoint_ringbuf_front(&ringbuf, &startReturn, &endReturn, &checkpointReturn);
	Assert(checkpointReturn.redo == checkpoint[9].redo);
	Assert(startReturn == start[9]);
	Assert(endReturn == end[9]);

	PG_RETURN_VOID();
}
