/*-------------------------------------------------------------------------
 *
 * cm.c
 *
 *    cluster manager module of Postgres-XC configuration and operation tool.
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
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
 * Copyright (c) 2013 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <c.h>

#include "postgres_ext.h"
#include "utils.h"
#include "variables.h"
/* This is an ugly hack to avoid conflict between gtm_c.h and pgxc_ctl.h */
#undef true
#undef false
#include "pgxc_ctl_log.h"
#include "varnames.h"
#include "config.h"
#include "monitor.h"
#include "do_shell.h"
#include "nodes/pg_list.h"
#include "datanode_cmd.h"



#define GetToken() (line = get_word(line, &token))
#define testToken(word) ((token != NULL) && (strcmp(token, word) == 0))
#define TestToken(word) ((token != NULL) && (strcasecmp(token, word) == 0))

static void cm_datanode(List *xc_nodes, char **nodeList);

static int do_datanode_command(bool master_run, bool slave_run, int idx)
{
    int rc = 0;
    int rc_local;
    
#define checkRc() do{if(WEXITSTATUS(rc_local) > rc) rc = WEXITSTATUS(rc_local);}while(0)

    if(isPaxosEnv())
        return rc;

    if(!master_run)
    {
        /* start datanode master */
        rc_local = doImmediate(aval(VAR_datanodeMasterServers)[idx], NULL,
                               "pg_ctl start -w -D %s %s; sleep 1",
                               aval(VAR_datanodeMasterDirs)[idx],
                               isVarYes(VAR_standAlone)? "" :"-o -i");
        checkRc();
    }

    if(!slave_run)
    {
        /* start datanode slave */
        rc_local = doImmediate(aval(VAR_datanodeSlaveServers)[idx], NULL,
                               "pg_ctl start -w -D %s %s; sleep 1",
                               aval(VAR_datanodeSlaveDirs)[idx],
                               isVarYes(VAR_standAlone)? "" :"-o -i");
        checkRc();
    }

    if((!master_run) && (slave_run))
    {
        // master dn still not startup, we will failover.
        if(pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx])!=0)
        {
            if(isVarYes(VAR_standAlone))
                rc_local = failover_oneDatanode_standalone(idx);
            else
                rc_local = failover_oneDatanode(idx);
            checkRc();
        }
    }
    return rc;

#undef checkRc
}

/*
 *	do_datanode_command_aa
 *
 *  do all cluster manager command for active active replication datanode
 *   
 */
static int do_datanode_command_aa(bool master_run, bool slave_run, int idx)
{
    int rc = 0;
    int rc_local;
    
#define checkRc() do{if(WEXITSTATUS(rc_local) > rc) rc = WEXITSTATUS(rc_local);}while(0)

    if(!master_run)
    {
        /* start datanode master, for aa replication, only take care master node */
        rc_local = doImmediate(aval(VAR_datanodeMasterServers)[idx], NULL,
                               "pg_ctl start -w -D %s -o -i; sleep 1",
                               aval(VAR_datanodeMasterDirs)[idx]);
        checkRc();
    }

    if((!master_run) && (slave_run))
    {
        rc_local = failover_oneDatanode_aa(idx);
        checkRc();
    }
    return rc;
#undef checkRc
}

static void cm_datanode(List *xc_nodes, char **nodeList)
{
    char **actualNodeList;
    int ii;
    int idx;
    bool masterstatus;
    bool slavestatus;

    actualNodeList = makeActualNodeList(nodeList);
    for (ii = 0; actualNodeList[ii]; ii++)
    {
        if ((idx = datanodeIdx(actualNodeList[ii])) < 0)
        {
            elog(ERROR, "ERROR: %s is not a datanode\n", actualNodeList[ii]);
            continue;
        }
        masterstatus = (pingNode(aval(VAR_datanodeMasterServers)[idx], aval(VAR_datanodePorts)[idx])==0)?true:false;
        if (doesExist(VAR_datanodeSlaveServers, idx) && !is_none(aval(VAR_datanodeSlaveServers)[idx]))
        {
            slavestatus = (pingNodeSlave(aval(VAR_datanodeSlaveServers)[idx], aval(VAR_datanodeSlaveDirs)[idx])==0)?true:false;
        }
        else
        {
            continue;  //no slave, should also help master startup later.
        }

        if(strcmp(aval(VAR_datanodeSlaveType)[idx], "1")== 0)
            do_datanode_command(masterstatus, slavestatus, idx);
        else
            do_datanode_command_aa(masterstatus, slavestatus, idx);

        Free(actualNodeList[ii]);
    }
    Free(actualNodeList);
}

#pragma GCC diagnostic ignored "-Wmissing-prototypes"
void do_cm_command(char *line)
{
    char *token;
    List *xc_nodes = NULL;

    if (!GetToken())
    {
        elog(ERROR, "ERROR: no monitor command options found.\n");
        return;
    }
 
    if (TestToken("all"))
    {
        cm_datanode(xc_nodes, aval(VAR_datanodeNames));
    }
	else
	{
		elog(NOTICE, "NOTICE: don't support seperate cluster manager for now.\n");
	}

    return;
}

