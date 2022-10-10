# Limitations

* Sequences are not supported for cross-coordinator queries.

* Global vacuum operations are not supported.

* Analyze operations without specifying a table are not supported.

* Reindexing is not supported.

* A query with multiple SQL statements cannot be run.

* DEFERRABLE constraints are not supported in complex data manipulation language (DML) statements.

* Object identifiers (OIDs) are not globally unique.

* The CREATE INDEX CONCURRENTLY statement is not supported.

* OIDs cannot be used as shard keys.

* Constraints cannot be used to create tables of the non-replicated type.

* The EXPLAIN ANALYZE SELECT INTO statement is not supported.

* The xmin and xmax columns of non-local tables cannot be queried from coordinators.

* The volatile function cannot be used to update a replicated table.

* Prepared transactions cannot be displayed.

* The TABLESAMPLE clause is not supported.

* Serializable Snapshot Isolation (SSI) is not supported.

* The WHERE CURRENT OF statement is not supported.

* Permissions cannot be granted only on views. If you want to grant permissions on views, you must also grant permissions to expanded tables.

* Roles that cannot access data on foreign servers cannot execute the SELECT INTO statement. To allow rows the permissions to execute the SELECT INTO statement, the roles must be granted permissions to access data on foreign servers.

* Internal triggers cannot be executed on coordinators.

___

Copyright © Alibaba Group, Inc.


