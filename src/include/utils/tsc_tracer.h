/*-------------------------------------------------------------------------
 *
 * tsc_tracer.h
 *
 * An instrumentation library for diagnosing performance problems,
 * emphasizing low runtime cost so that it can be used in CPU
 * intensive situations. Two important points in lowering
 * instrumentation cost:
 * 
 * 1. Time Source
 * 
 * Time source usually is the biggest cost in performance
 * instrumentations. We use Time Stamp Counter (TSC) as a high
 * performance time source. Recent Intel CPUs support invariant
 * TSC that runs at a constant rate in all ACPI P-, C-. and
 * T-states. As a result, invariant TSC can be used in a timer
 * service.
 * 
 * https://software.intel.com/en-us/forums/intel-isa-extensions/topic/280440
 * 
 * Intel offers two instructions to read the TSC, rdtsc and
 * rdtscp. The difference is that rdtscp is a serializing call
 * that prevents reordering around the call. Note that the
 * __volatile__ keywords prevents compiler from re-ordering the
 * instructions, CPU can still re-order them. So using rdtscp
 * gives you more precise result at a higher cost.
 * 
 * 2. Data Processing
 * 
 * To avoid costly operations, we stick to integer operations
 * as much as we can. Try our best to define aligned data structures.
 * We shy away from string and any variable length data structure
 * maniupulation in the critical path. When outputing the
 * instrumentation result to the log stream, we can not avoid
 * strings anymore, but still try to avoid string formatting.
 * 
 * 
 * This library is easily portable to other programs just by
 * coping tsc_tracer.h tsc_tracer.c and tsctest.c files.
 * All PG specific parts are explicitily labeled by comments
 * 
 * 3. How to use
 * 
 * At the starting point of the trace, call
 *  void initTimedTrace(uint64_t traceId)
 * For instance, to trace an transaction, call it at the begining
 * of the transaction, use a trace id that should be unique
 * to that transaction.  This function would make an random
 * sampling decision whether to actually start a trace. If
 * the decision is yes, the process local trace data buffer
 * is initialized, and the following tracing information
 * would be saved.
 * 
 * And at each program point you want to instrument, call:
 *  timedTrace(TimeTagName, ...)
 * This function would take a time stamp, and save all the
 * parameters with the time stamp into the tracing buffer,
 * if the tracing is enabled (see above). It's a good
 * idea to pick a descriptive time tag name and add it in
 * the macro FOREACH_TAG_NAME below. This way you know
 * at what time the program reached a certain program
 * points.
 * 
 * At the end of the trace, call
 *  logTimedTrace()
 * This will output the tracing buffer (if enabled) via
 * the error log stream, and clear the buffer. Since postgres
 * error logging is pretty heavy weight, this is the single
 * most expensive call in this library. So please try
 * to avoid place this call in a critical path.
 * 
 * The above are online processes. We tried hard to make it
 * simple and low interferences, and leave the analysis part
 * offline, see src/bin/tsc_parse/README for offline tools
 * for trace analysis.
 * 
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * Portions Copyright (c) 2012-2018, PostgreSQL Global Development Group
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef TSC_TRACER_H
#define TSC_TRACER_H

#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>

#define TAG_BUF_SIZE 2000

/**
 * Constant table to encode binary data into hex string, avoiding
 * expensive string formatting operations
 */
extern const char hexTable[256][2];

/**
 * Intel instruction wrappers
 * TODO!! add cpu info checkings to make sure these instructions are supported!
 */

static inline void instCpuinfo(unsigned i, uint32_t regs[]) {
  __asm__ __volatile__
    ("cpuid" : "=a" (regs[0]), "=b" (regs[1]), "=c" (regs[2]), "=d" (regs[3])
       : "a" (i), "c" (0));
}

static inline uint64_t instRdtsc(){
    unsigned int lo,hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}

static inline uint64_t instRdtscp(){
    unsigned int lo,hi;
    __asm__ __volatile__ ("rdtscp" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
}

/**
 * Timing data entry of a instrumentation point.
 * tagName: a named constant representing a unique program point, e.g.
 * begin sending request to workers. 
 */
typedef struct {
    uint64_t tsc;
    uint64_t traceId;
    uint16_t tagName;
    uint16_t pad1;
    uint32_t pad2;
    uint64_t pad3;
} TimeTagEntry;

/** Encode a time tag as hex text for output. 
Caller must provide a text buffer txt at least 2 * sizeof(TimeTagEntry)
in size;
@param[in]	pTag  pointer to the tag needs to be encoded
@param[out]	txt	  caller provided txt buffer
@return void 
 */
static inline void timeTag2Txt(const TimeTagEntry* pTag, char* txt) {
  /* TODO!! verify txt buffer size */
  const unsigned char* p = (const unsigned char*)(pTag);
  int i;
  for (i=0; i < sizeof(TimeTagEntry); i++) {
    *txt = hexTable[*p][0];
    txt++;
    *txt = hexTable[*p][1];
    txt++;
    p++;
  }
}

/**
 * All the time tag name should be declared here.
 * DO NOT REORDER EXISTING NAMES, add new ones right before TimeTagMaxValue
 */
#define FOREACH_TAG_NAME(N_DEC) \
    /********** PG Specific ********/  \
        N_DEC(TimeTagTxnStart) \
        N_DEC(TimeTagTxnId)   \
        N_DEC(TimeTagTxnIdAssign)   \
        N_DEC(TimeTagTxnPreparing) \
        N_DEC(TimeTagTxnPrepared) \
        N_DEC(TimeTagFinishingPrepared) \
        N_DEC(TimeTagFinishedPrepared) \
        N_DEC(TimeTagTxnProcArrayLock) \
        N_DEC(TimeTagTxnProcArrayRls) \
        N_DEC(TimeTagTxnCommit) \
        N_DEC(TimeTagTxnAbort) \
        N_DEC(TimeTagTxnCleanup)   \
        N_DEC(TimeTagCopyFromStart) \
        N_DEC(TimeTagCopyFromEnd) \
        N_DEC(TimeTagCopyFromFlushStart) \
        N_DEC(TimeTagCopyFromFlushEnd) \
        N_DEC(TimeTagCopyFromBeforeLoop) \
        N_DEC(TimeTagCopyFromStartRecord) \
        N_DEC(TimeTagCopyFromEndRecord) \
        N_DEC(TimeTagCopyFromEndInsertBuffer) \
        N_DEC(TimeTagCopyFromEndMaterialize) \
        N_DEC(TimeTagCopyFromStartMaterialize) \
        N_DEC(TimeTagCopyFromBeforeInsert) \
        N_DEC(TimeTagCopyFromEndNextCopyFrom) \
        N_DEC(TimeTagCopyFromStartNextCopyFrom) \
        N_DEC(TimeTagCopyFromNextRawFieldStart) \
        N_DEC(TimeTagCopyFromNextRawFieldEnd) \
        N_DEC(TimeTagCopyFromNextReadAttTextStart) \
        N_DEC(TimeTagCopyFromNextReadAttTextEnd) \
        N_DEC(TimeTagCopyFromNextReadAttCSVStart) \
        N_DEC(TimeTagCopyFromNextReadAttCSVEnd) \
        N_DEC(TimeTagCopyFromNextInputFuncBefore) \
        N_DEC(TimeTagCopyFromNextDefaultColBefore) \
        N_DEC(TimeTagCopyFromNextReadlineEnd) \
        N_DEC(TimeTagCopyFromNextmbstrlenBefore) \
        N_DEC(TimeTagCopyFromNextmbstrlenAfter) \
        N_DEC(TimgTagCopyFromNextReadlineTextStart) \
        N_DEC(TimgTagCopyFromNextReadlineTextEnd) \
    /********** End PG Specific ********/  \
        N_DEC(TimeTagTraceTooLong) \
        N_DEC(TimeTagMaxValue)

#define GENERATE_ENUM(ENUM) ENUM,
#define GENERATE_STRING(STRING) #STRING,

enum TimeTagNameEnum {
    FOREACH_TAG_NAME(GENERATE_ENUM)
};

/**
 * Utility for taking a collection of timestamps
 * in a single thread.
 * 
 * Ideally this should be in a thread local data
 * structure, use addTimeTag() to take a series
 * of timestamps, and call the output method
 * to output the timestamps to text representation
 * and write them to log.
 * 
 * DO NOT access these fields directly, use the 
 * functions defined below.
 */
typedef struct {

  TimeTagEntry tagBuffer[TAG_BUF_SIZE];

  uint32_t nextPos;
  bool enabled;

} HTimeTags;

static inline void initTimeTags(HTimeTags* pTags, bool enabled) {
  pTags->enabled = enabled;
  pTags->nextPos = 0;
}

/**
 * Instrumentation point for taking a timestamp, and record it with
 * other parameters into a time tag entry。
 */
static inline void appendTimeTag(HTimeTags* pTags, uint64_t traceId, uint16_t tagName,
                  uint16_t pad1, uint32_t pad2, uint64_t pad3) {
    uint64_t tsc;
    TimeTagEntry *pEntry;

    if (!pTags->enabled) { return; }

    //TODO!! change to rdtscp() if more precise timing is needed
    // or add an option to let the caller choose.
    tsc = instRdtsc();

    if (pTags->nextPos >= TAG_BUF_SIZE) {
 //     pTags->enabled = false;
 //     return;
 //     logTimedTrace();
      // TODO!!, print out existing tags or report error!
      pEntry = &(pTags->tagBuffer[0]);
      pEntry->tsc = tsc;
      pEntry->traceId = traceId;
      pEntry->tagName = TimeTagTraceTooLong;
      pEntry->pad1 = 0;
      pEntry->pad2 = 0;
      pEntry->pad3 = 0;
      pTags->nextPos = 1;
    }

    pEntry = &(pTags->tagBuffer[pTags->nextPos]);

    pEntry->tsc = tsc;
    pEntry->traceId = traceId;
    pEntry->tagName = tagName;
    pEntry->pad1 = pad1;
    pEntry->pad2 = pad2;
    pEntry->pad3 = pad3;

    pTags->nextPos++;
}

/**
 * Returns the size of the serialized string for all tags
 * collected so far.
 */
static inline uint64_t txtSize(const HTimeTags* pTags) {
  // 1 entry takes 2 * sizeof(TimeTagEntry), plus a \n
  // and the string starts with a \n, ends with \0
  return (2 * sizeof(TimeTagEntry) + 1) * pTags->nextPos + 2;
}

/**
 * Serialize all tags into a string and stored in the result
 * parameter. parameter result must points to a buffer size
 * no smaller than txtSize(pTags)
 */
static inline void outputTagsForLogging(const HTimeTags* pTags, char* result) {
  char* p = result;
  int entry;

  *p = '\n';
  p++;
  for (entry = 0; entry < pTags->nextPos; entry++) {
    timeTag2Txt(&(pTags->tagBuffer[entry]), p);
    p += 2 * sizeof(TimeTagEntry);
    *p = '\n';
    p++;
  }
  *p = 0;
  // assert p - res == (2 * sizeof(TimeTagEntry) + 1) * pTags->nextPos + 2;
}


/***************** PG Specific **********************/

/**
 * Since process handles a client session, so
 * tag storage can simply be a global variable, instead
 * of thread-local in multi-thread settings.
 * We are taking advantage of the auto-initialization
 * of global variables, so that the tracing is turned
 * off by default.
 */
extern uint64_t tscTraceId;
extern HTimeTags tscTimeTagArray;
extern char tscHexLogBuffer[];

/**
 * Simple rdtsc based random generator to sample trace
 * transactions
 */
extern uint64_t tscTraceRandomSeed;

extern bool isClientBackend(void);

/**
 * Sampling decision true 1/256 of the time, roughly
 */
static inline bool sampleTracing() {
	uint64_t seed;

	tscTraceRandomSeed += instRdtsc();
	seed = (tscTraceRandomSeed >> 32) ^ tscTraceRandomSeed;
	seed = (seed >> 16) ^ seed;

  // default mask 0xff -> 1/4096 chance
  bool enableTrace = ((seed & 0xfff) == 0);
	return enableTrace;
}

/** Initialize 'thread' local time tags, so that later
instrumentation points can take effects. Chance of this
actually turns on tracing depends on sampleTracing()

@param[in]	traceId        unique ID of a trace unit, can
                           be one per transaction or
                           statement, depending on needs       
@return void
 */
static inline void initTimedTrace(uint64_t traceId) {
  if (!sampleTracing()) return;

  tscTraceId = traceId;
  initTimeTags(&tscTimeTagArray, true);
}

/** Instrumentation point
@param[in]	tagName name of the instrumentation point,
                    one per program point.
@param[in]	...     extra information that will be
                    persisted, for caller's discretion       
 */
static inline void timedTrace(enum TimeTagNameEnum tagName,
                  uint16_t pad1, uint32_t pad2, uint64_t pad3) {
  appendTimeTag(&tscTimeTagArray, tscTraceId, tagName, pad1, pad2, pad3);
}

/**
 * Output time tags to log stream and clean all tags.
 * 
 * Note that PG's ereport takes numbers of memory allocations, multiple
 * memory copies, a kernel call into pipe, a gobal lock and another
 * kernal call to file write.  Seems quite expensive.
 * 
 * Using it for similicity reason at the moment. 
 */
extern void logTimedTrace(void);

/***************** End PG Specific **********************/


/** 
 * Utilities for offline processing tool to convert the log
 * entry back into time tag, and print out the tag for human
 * reading
 */

/**
 * Convert string into a TimeTagEntry
 */
extern bool txt2TimeTag(const char* txt, TimeTagEntry* pTag, int len);

const char* getTagName(uint16_t tagName);

/**
 * Pretty print a time tag to CSV:
 * trace id, tag name, TSC, pad1, pad2, pad3
 */
void printTimeTag(const TimeTagEntry* pTag,
                  enum TimeTagNameEnum traceStartTag);


#endif
