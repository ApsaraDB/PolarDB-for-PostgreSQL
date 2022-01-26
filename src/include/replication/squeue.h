/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *      Definitions for the shared queue handling
 *
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	int			m_cnt;
	pthread_mutex_t m_mutex;
	pthread_cond_t m_cond;
}			ThreadSemaRec;

extern void ThreadSemaRecInitRec(ThreadSemaRec * sema, int32 init);
extern void ThreadSemaRecDownRec(ThreadSemaRec * sema);
extern void ThreadSemaRecUpRec(ThreadSemaRec * sema);


typedef struct
{
	void	  **m_List;
	uint32		m_Length;
	slock_t		m_lock;
	volatile uint32 m_Head;
	volatile uint32 m_Tail;
}			PGPipeRec;
extern PGPipeRec * CreatePipeRec(uint32 size);
extern void DestoryPipeRec(PGPipeRec * pPipe);
extern void *PipeGetRec(PGPipeRec * pPipe);
extern int	PipePutRec(PGPipeRec * pPipe, void *p);
extern bool PipeIsFullRec(PGPipeRec * pPipe);
extern bool IsEmptyRec(PGPipeRec * pPipe);
extern int	PipeLengthRec(PGPipeRec * pPipe);

extern int32 CreateThreadRec(void *(*f) (void *), void *arg, int32 mode);
extern void ThreadMutexInitRec(pthread_mutex_t *mutex);
extern void ThreadMutexLockRec(pthread_mutex_t *mutex);
extern void ThreadMutexUnlockRec(pthread_mutex_t *mutex);

#endif
