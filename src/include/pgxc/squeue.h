/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *      Definitions for the shared queue handling
 *
 *
 * Copyright (c) 2012-2014, TransLattice, Inc.
 *
 * IDENTIFICATION
 *      $$
 *
 *-------------------------------------------------------------------------
 */

#ifndef SQUEUE_H
#define SQUEUE_H

#include "postgres.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"
#include "utils/tuplestore.h"
#include "tcop/dest.h"
#include "storage/dsm_impl.h"

#include "storage/s_lock.h"
#include "storage/spin.h"
#include <signal.h>


#define WORD_IN_LONGLONG        2
#define BITS_IN_BYTE            8
#define BITS_IN_WORD            32
#define BITS_IN_LONGLONG        64
#define MAX_UINT8               0XFF
#define MAX_UINT32              0XFFFFFFFF
#define MAX_UINT64              (~(uint64)(0))
#define ERR_MSGSIZE             (256)


#define ALIGN_UP(a, b)   (((a) + (b) - 1)/(b)) * (b)
#define ALIGN_DOWN(a, b) (((a))/(b)) * (b)
#define DIVIDE_UP(a, b)   (((a) + (b) - 1)/(b))
#define DIVIDE_DOWN(a, b)   (((a))/(b)) 


enum MT_thr_detach 
{ 
    MT_THR_JOINABLE, 
    MT_THR_DETACHED 
};

typedef struct
{
    int             m_cnt;
    pthread_mutex_t m_mutex;
    pthread_cond_t  m_cond;
}ThreadSema;

extern void ThreadSemaInit(ThreadSema *sema, int32 init);
extern void ThreadSemaDown(ThreadSema *sema);
extern void ThreadSemaUp(ThreadSema *sema);


typedef struct 
{
    void                 **m_List; /*循环队列数组*/
    uint32               m_Length; /*队列队列长度*/
    slock_t              m_lock;   /*保护下面的两个变量*/
    volatile uint32      m_Head;   /*队列头部，数据插入往头部插入，头部加一等于尾则队列满*/
    volatile uint32      m_Tail;   /*队列尾部，尾部等于头部，则队列为空*/
}PGPipe;
extern PGPipe* CreatePipe(uint32 size);
extern void    DestoryPipe(PGPipe *pPipe);
extern void    *PipeGet(PGPipe *pPipe);
extern int     PipePut(PGPipe *pPipe, void *p);
extern bool    PipeIsFull(PGPipe *pPipe);
extern bool    IsEmpty(PGPipe *pPipe);
extern int        PipeLength(PGPipe *pPipe);

extern int32 CreateThread(void *(*f) (void *), void *arg, int32 mode);

#endif