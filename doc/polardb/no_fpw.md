
## Remote Recovery

PostgreSQL introduces full page write mechanism to avoid inconsistency in case of
system crashes. However, full page writes can amplify writes and reduce performance.
Remote Recovery is desgined to fetch full page from the mirror node (standby or master)
during recovery to prevent torn pages in the absence of full page write.
As a result, full page write can be turned off for reduced IO and improved performance.

Specifically, PolarDB PG writes a special bit in WAL when a page is first modified since
the last checkpoint. During recovery, upon encountering a special bit that indicates a 
remote fetching is needed, a PolarDB PG database instance would fetch the page from its mirror
instance and restore it locally before replaying later modifications.

Remote recovery also can leverage our parallel WAL redo framework to fetch pages and to relay WAL 
records in parallel.

## How to use
1. You can put a remote_recovery.conf in the data directory to support remote recovery.
   The command format of remote_recovery.conf is the same with recovery.conf.
   You should specify the mirror node address for building connection. For example, you 
   can write "standby_conninfo = 'host=xx port=xx user=xx password=xx     application_name=standby'" into remote_recovery.conf.
2. The parameter of max_wal_senders in the mirror node should be configured to be larger
   than max_parallel_replay_workers in the recoverying node.
3. If the mirror node is a backup node, it should turn on hot_standby for accepting connections
   from the recoverying node. checkpoint_sync_standby should be turned on to guarantee that the mirror node has full WAL modifications once a checkpoint is done.

___

Copyright © Alibaba Group, Inc.

