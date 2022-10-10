#!/usr/bin/awk -f

# Copyright (c) 2021, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");

# This is an example to analyze the trace from Time Tags embedded in commands/copy.c (version: PG 12)
# flush function: CopyMultiInsertInfoFlush
# Copy From function: CopyFrom 
# Copy From read attributes: CopyReadAttributesText
# The output includes the time of reading one record, reading its attributes, and potential flush. 

# Here is sample code added in copy.c
#static int
#CopyReadAttributesText(CopyState cstate)
#{
#	char		delimc = cstate->delim[0];
#	int			fieldno;
#	char	   *output_ptr;
#	char	   *cur_ptr;
#	char	   *line_end_ptr;

#	timedTrace(TimeTagCopyFromNextReadAttTextStart,0,0,0);
#...
#	timedTrace(TimeTagCopyFromNextReadAttTextEnd,0,0,0);
#	return fieldno;
#}

#static inline void
#CopyMultiInsertInfoFlush(CopyMultiInsertInfo *miinfo, ResultRelInfo *curr_rri)
#{
#	ListCell   *lc;
#	timedTrace(TimeTagCopyFromFlushStart,0,0,0);
# ...
#		CopyMultiInsertBufferCleanup(miinfo, buffer);
#		miinfo->multiInsertBuffers = list_delete_first(miinfo->multiInsertBuffers);
#	}
#	timedTrace(TimeTagCopyFromFlushEnd,0,0,0);
#}

#in CopyFrom()
#	for (;;)
#	{
#		TupleTableSlot *myslot;
#		bool		skip_tuple;		
#		CHECK_FOR_INTERRUPTS();
#		initTimedTrace((uint64_t)processed);
#		initTimeT = true;
#		timedTrace(TimeTagCopyFromStartRecord,0,0,0);
# ...
#					/* AFTER ROW INSERT Triggers */
#					ExecARInsertTriggers(estate, resultRelInfo, myslot,
#										 recheckIndexes, cstate->transition_capture);
#					list_free(recheckIndexes);
#				}
#			}
#			timedTrace(TimeTagCopyFromEndRecord,0,0,0);


BEGIN   {
    traceId = "";
    startTime = 0;

    CopyFromStartRecord = 0;
    numCopyFromRecord = 0;
    CopyFromRecord = 0;

    CopyFromReadAttTextStart = 0;
    numCopyFromReadAttText = 0;
    CopyFromReadAttText = 0;

    CopyFromFlushStart = 0
    numCopyFromFlush = 0;
    CopyFromFlush = 0;

    print "Trace,  Total,  #CopyFromRecord,  TotalCopyFromRecord, #CopyFromReadAttText, TotalCopyFromReadAttText, #CopyFromFlush, TotalCopyFromFlush"
}

{
# DEBUG STMTS
#    print
#    printf("%d\n", numCitusExe);

    if ($1 != traceId) {
#close previous trace
        if (traceId != "") {
            printf("Error Trace %s ended with %s\n", traceId, curTag);
        }

#start new trace
        traceId = $1;
        startTime = $2;

        CopyFromStartRecord = 0;
        numCopyFromRecord = 0;
        CopyFromRecord = 0;

        CopyFromReadAttTextStart = 0;
        numCopyFromReadAttText = 0;
        CopyFromReadAttText = 0;

        CopyFromFlushStart = 0
        numCopyFromFlush = 0;
        CopyFromFlush = 0;
    }

    prevTime = curTime;
    curTime = $2;

    prevTag = curTag;
    curTag = $3;
}

/TimeTagCopyFromStartRecord/   {
    if (CopyFromStartRecord != 0) {
        printf("Error Trace %s missing TimeTagCopyFromEndRecord\n", traceId);
    }
    CopyFromStartRecord = curTime;
}


/TimeTagCopyFromEndRecord/    {
    if (CopyFromStartRecord == 0) {
        printf("Error Trace %s missing TimeTagCopyFromStartRecord\n", traceId);
    } else {
        CopyFromRecord += curTime - CopyFromStartRecord;
        numCopyFromRecord++;
        CopyFromStartRecord = 0;
    }
}

/TimeTagCopyFromNextReadAttTextStart/   {
    if (CopyFromReadAttTextStart != 0) {
        printf("Error Trace %s missing TimeTagCopyFromNextReadAttTextEnd\n", traceId);
    }
    CopyFromReadAttTextStart = curTime;
}


/TimeTagCopyFromNextReadAttTextEnd/   {
    if (CopyFromReadAttTextStart == 0) {
        printf("Error Trace %s missing TimeTagCopyFromNextReadAttTextStart\n", traceId);
    } else {
        CopyFromReadAttText += curTime - CopyFromReadAttTextStart;
        numCopyFromReadAttText++;
        CopyFromReadAttTextStart = 0;
    }
}

/TimeTagCopyFromFlushStart/ {
    if (CopyFromFlushStart != 0) {
        printf("Error Trace %s missing TimeTagCopyFromFlushEnd\n", traceId);
    }
    CopyFromFlushStart = curTime;
}

/TimeTagCopyFromFlushEnd/   {
    if (CopyFromFlushStart == 0) {
        printf("Error Trace %s missing TimeTagCopyFromFlushStart\n", traceId);
    } else {
        CopyFromFlush += curTime - CopyFromFlushStart;
        numCopyFromFlush++;
        CopyFromFlushStart = 0;
    }
}

/(TimeTagCopyFromEndRecord)/  {
    printf("%s %6d, %2d, %4d, %2d, %4d, %2d, %4d \n",
           traceId, curTime - startTime, numCopyFromRecord, CopyFromRecord, 
                                        numCopyFromReadAttText,CopyFromReadAttText,
                                        numCopyFromFlush, CopyFromFlush);
    traceId = "";
}
