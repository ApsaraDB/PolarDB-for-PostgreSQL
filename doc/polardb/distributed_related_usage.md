## How to deploy a cluster

### Cluster Deployment

You can configure a configuration file and use the pgxc_ctl utility to deploy a cluster.

    pgxc_ctl -c polarx.conf init all //Initialize and start a cluster

    pgxc_ctl -c polarx.conf clean all // shut down and clean up the cluster

    pgxc_ctl -c polarx.conf start all // start the cluster

    pgxc_ctl -c polarx.conf stop all // shut down the cluster

### manually deployment

In the following examples, a cluster has one coordinator and two datanodes.

#### Modify postgresql.conf

CN/DN:

    shared_preload_libraries = 'polarx'

    listen_addresses = '*'

    max_pool_size = 100

    max_connections = 300

    max_prepared_transactions = 10000

CN:

    port = 20015

    pooler_port = 21015

DN1:

    port = 20018

    pooler_port = 21018

DN2:

    port = 20019

    pooler_port = 21019


#### Modify pg_hba.conf

    host    all             all             127.0.0.1/32            trust

    host    all             all             ::1/128                 trust


#### Create cluster node information

CN node：

    CREATE EXTENSION polarx;

    CREATE SERVER coord1m TYPE 'C' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20015', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'true', node_id '1');

    CREATE SERVER datanode1m TYPE 'D' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20018', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'false', node_id '2');

    CREATE SERVER datanode2m TYPE 'D' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20019', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'false', node_id '3');

    CREATE SERVER cluster_server FOREIGN DATA WRAPPER polarx;


DN1 node:

    CREATE EXTENSION polarx;

    CREATE SERVER coord1m TYPE 'C' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20015', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'false', node_id '1');

    CREATE SERVER datanode1m TYPE 'D' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20018', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'true', node_id '2');

    CREATE SERVER datanode2m TYPE 'D' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20019', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'false', node_id '3');

    CREATE SERVER cluster_server FOREIGN DATA WRAPPER polarx;


DN2 node：

    CREATE EXTENSION polarx;

    CREATE SERVER coord1m TYPE 'C' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20015', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'false', node_id '1');

    CREATE SERVER datanode1m TYPE 'D' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20018', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'false', node_id '2');

    CREATE SERVER datanode2m TYPE 'D' FOREIGN DATA WRAPPER polarx OPTIONS (host 'localhost', port '20019', nodeis_primary 'false', nodeis_preferred 'false', node_cluster_name 'cluster_server', nodeis_local 'true', node_id '3');

    CREATE SERVER cluster_server FOREIGN DATA WRAPPER polarx;



### Useful Commands

    select * from pg_foreign_server; // view node information

    select * from pg_foreign_table; // view distributed tables

    create table polarx_test(id int , name text); //Create a distributed table. By default, a hash-distributed table is created based on the first column. If none of the columns can be used to create a hash-distributed table, a round-robin distributed table is created.

    create table polarx_test(id int , name text) with (dist_type = hash, dist_col = id); //Explicitly specify the distribution mode and distribution keys to create a hash-distributed table. Calculate hash values based on the id column, determine the number of nodes based on the remainder method, and then distribute the data.

    create table polarx_test(id int, name text) with (dist_type = modulo, dist_col = id); //Explicitly create a modulo distributed table. Calculate hash values based on data IDs, determine the number of nodes based on the remainder method, and then distribute the data.

    create table polarx_test(id int, name text) with (dist_type = roundrobin); //Explicitly create a round-robin distributed table. The data is distributed to each node in sequence.

    create table polarx_test(id int, name text) with (dist_type = replication); //Explicitly establish a replication table whose complete data is synchronized on each datanode.

    execute polarx_direct('datanode1m', 'select * from bmsql_warehouse;'); //Execute the statement on the corresponding datanode through a coordinator.

    drop table polarx_test; // delete a table

___

Copyright © Alibaba Group, Inc.
