# Create sharded tables by using native PostgreSQL syntax
PolarDB for PostgreSQL supports using the CREATE TABLE statement to create distributed tables

## Syntax for creating distributed tables

The syntax of the CREATE TABLE statement is the same as that of the CREATE TABLE statement in PostgreSQL. However, the CREATE TABLE statement in PolarDB for PostgreSQL also supports the usage of the WITH clause. In short, the distribution information of distributed tables can be added to the WITH clause. dist_type specifies the distribution types of tables. The following distribution types are supported: replication, hash, modulo, and round-robin. dist_col column specifies the information of distribution keys. Currently, only a single column can be used to record distribution keys. For more information, visit https://www.postgresql.org/docs/11/sql-createtable.html. The following section provides the syntax for creating simple tables:

Explicitly create a replicated table:


```
create table polarx_test(id int, name text) with (dist_type = replication);
```
Explicitly create a hash-distributed table:
```
create table polarx_test(id int, name text) with (dist_type = hash, dist_col = id);
```
Explicitly create a modulo distributed table:
```
create table polarx_test(id int, name text) with (dist_type = modulo, dist_col = id);
```
Explicitly create a round-robin distributed table:
```
create table polarx_test(id int, name text) with (dist_type = roundrobin);
```
Implicitly create a distributed table. By default, a hash-distributed table is created based on the first column. If none of the columns can be used to create a hash-distributed table, a round-robin distributed table is created.
```
create table polarx_test(id int, name text);
```
# The execute polarx_direct statement
You can execute the execute polarx_direct statement on the coordinator. 
## Syntax:：
```
execute polarx_direct($node_name, $sql);
```
node_name specifies the name of the node on which to execute the SQL statement. sql specifies the statement you want to execute.  In this example, the polarx_test table has two datanodes: datanode1 and datanode2. The following statement is used to query the content of the polarx_test table on datanode1:

```
execute polarx_direct('datanode1', 'select * from bmsql_warehouse;');
```

___

Copyright © Alibaba Group, Inc.
