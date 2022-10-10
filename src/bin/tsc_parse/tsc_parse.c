/*-------------------------------------------------------------------------
 *
 * tsc_parse.c
 *
 * Offline tools for translating hex log (read from stdin) into
 * CSV format (written to stdout) for eyeballing or excel/R processing.
 *
 * Comments include example code for using the TSC tracer
 * library for collecting timeing information, output them
 * via a log stream.
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 *-------------------------------------------------------------------------
 */


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "utils/tsc_tracer.h"

#define CPU_CYCLES_PER_MICROSECOND  2500

/**
 * Functions for offline processing of hex logs and output
 * to CSV for eyeballing and excel/R processing
 */

static unsigned char hexIndex(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  } else if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  } else if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  } else {
    fprintf(stderr, "Char input %c is NOT a hex code!\n", c);
    exit(EXIT_FAILURE);
  }
}

/**
 * Convert string into a TimeTagEntry
 */
bool txt2TimeTag(const char* txt, TimeTagEntry* pTag, int len) {
  unsigned char* pDest = (unsigned char*)pTag;
  int i,j;
  j = 0;
  while(j<len && txt[j++]==' ');

  if(j >= len)
    return false;

  for (i = 0; i < sizeof(TimeTagEntry); i++) {
    char first = txt[i*2+j];
    char second = txt[i*2+1+j];
    pDest[i] = hexIndex(first) << 4 | hexIndex(second);
  }
  return true;
}

/*
static char* printLongAsString(uint64_t val){
  char* ret = malloc(9);
  char* p = ret;
  int i;
  
  for (i=0; i<8; i++){
    *p = val;
    val = val >> 8;
    p++;
  }
  *p = 0;

  return ret;
}
*/
/**
 * Pretty print a time tag to CSV:
 * trace id, tag name, TSC, pad1, pad2, pad3
 */
void printTimeTag(const TimeTagEntry* pTag, uint32_t freqMhz) {

  uint64_t timeInUs;

  timeInUs = pTag->tsc / freqMhz;
  printf("0x%016lx, %016lu,", pTag->traceId, timeInUs);

  switch (pTag->tagName)
  {
//  case TimeTag???: handling special tags
  default:
    printf(" %-30s, %u, %u, %lu\n", getTagName(pTag->tagName),
         pTag->pad1, pTag->pad2, pTag->pad3);
    break;
  }
}


int main(int argc, char **argv) {

  char* line = NULL;
  size_t bufSize = 0;

  for (;;) {
    int len = getline(&line, &bufSize, stdin);
    if (-1 == len) { break; }
    if (len <= 2) { continue; } // empty line

    if (len < 2 * sizeof(TimeTagEntry)) {
      fprintf(stderr, "Invalid line: %s\n", line);
    } else {
      TimeTagEntry tag;
      if(txt2TimeTag(line, &tag,len))
      //TODO!!!! read MHz information from /proc/cpuinfo!!
      // instead of hard coding it as 2500!!
        printTimeTag(&tag, CPU_CYCLES_PER_MICROSECOND);
    }

  }

  return 0;

  /************** Example Code for using tsc_tracer ******************
  int i;

  printf("Processor Base Frequency:  %04d MHz\r\n", getFrequenceMhz());

  // collection of tags, should be in thread local memory
  HTimeTags tags;
  initTimeTags(&tags, true);

  // trace id should be updated per transaction or per statement,
  // depending on tracing purpose
  uint64_t traceId = 0x123456789abcdef0L;

  printf("Simulating an execution by sleeping random times,"
         " instrumented with appendTimeTag\n");
  
  for (i=0; i < 10; i++) {
    int r = rand();
    printf("Sleeping: %d\n", r);
    usleep(r/1000);
    uint16_t name = r % TimeTagMaxValue;
    appendTimeTag(&tags, traceId, name, (uint16_t)r, r, ((uint64_t)r) << 32 | r);
  }

  // Simulate execution finished, now serialize the tags to string
  // so as to pipe them out the log stream
  char* bigbuf = malloc(txtSize(&tags));
  outputTagsForLogging(&tags, bigbuf);
  printf("%s", bigbuf);

  printf("\nNow simulate offline tool process the log message,"
        " where performance does not matter that much.\n");
  printf("\nTrace Id          , Time Tag Name                                , TSC                 , pad, pad, pad\n");

  char* p = bigbuf;
  char* s = bigbuf;

  while(*p != 0) {
    while (*p != '\n' && *p != 0) { p++; }
    if (p == s) {
      // empty line
      if (*p == 0) { break; }
      p++; s++;
      continue;
    }

    if (p - s < 2 * sizeof(TimeTagEntry)) {
      char save = *p;
      *p = 0;
      printf("Invalid line: %s\n", s);
      *p = save;
    } else {
      TimeTagEntry tag;
      txt2TimeTag(s, &tag);
      printTimeTag(&tag);
    }

    if (*p == 0) { break; }
    // next line
    p++;
    s = p;
  }
  ************************************************************/
}
