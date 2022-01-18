/*-------------------------------------------------------------------------
 *
 * consensus_repl.h
 *		Consensus Service for XLOG replication
 *
 * Copyright (c) 2021, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 * src/include/polar_dma/consensus_repl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _CONSENSUS_REPL_H
#define _CONSENSUS_REPL_H

#include "pthread.h"

#include "postgres.h"
#include "access/xlog.h"

extern uint64 ConsensusGetCommitIndex(void );
extern void ConsensusSetCommitIndex(uint64 commit_index);

extern bool consensus_check_physical_flush_lsn(uint64 term, 
		XLogRecPtr lsn, TimeLineID tli, bool wait);

#endif /* _CONSENSUS_REPL_H */
