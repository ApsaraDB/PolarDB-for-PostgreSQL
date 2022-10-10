# When HLC patch is not used

PolarDB for PostgreSQL provides the polarx plug-in to manage the coordination logic of distributed transactions. When a transaction is committed, if the write set involves two or more nodes, the two-phase commit (2PC) protocol is used to commit the transaction. However, the consistency of distributed transactions cannot be guaranteed when HLC patches are not used, because the polarx plug-in does not support the timestamp-based MVCC mechanism. In short, the write set of the transaction on node A may be visible to other transactions, while the write set of the same transaction on node B is not visible to other transactions.  The 2PC coordination logic code is written in contrib/polarx. To use this feature, you need to install the polarx plug-in.

## Cleanup of residual 2PC transactions

The pgxc_clean utility in the src/bin directory helps you clean up residual 2PC transactions. Due to the lack of the commit point record function provided by patches, all residual prepared transactions are committed or rolled back based on the SQL statement you entered.  Syntax:

``` 
pgxc_clean -h $host -d $dbname -U $dbuser -p $port -S -H 0
```

host, port, dbname, and dbuser can be replaced with the information of the coordinator.  

-S indicates that the simple_clean_2pc mode is used. In this mode, all prepared transactions are rolled back or committed based on the specified value of -H.

-H can be 0 or 1.  
    0 indicates that all prepared transactions are rolled back.  0 is the default value.  
    1 indicates that all prepared transactions are committed.

	
# When HLC patch is used
An HLC patch applicable to PostgreSQL 11.2 is provided. This patch resolves the issue where distributed transactions are inconsistent.

## How to apply an HLC patch on PostgreSQL 11.2

1. Download the PostgreSQL 11.2 kernel code at https://github.com/postgres/postgres/tree/REL_11_2.
2. Add a patch to the kernel.  The patch is stored in patchs/HLC_patch_based_on_pg11_2.patch.

```
$ git apply /path/to/patch/HLC_patch_based_on_pg11_2.patch
```
3. Copy the contrib or polarx plug-in of PolarDB for PostgreSQL to the contrib of PostgreSQL 11.2.
4. Compile the code by using patchs/build.sh.  Run the ./build.sh command.
5. The HLC patch is successfully added. The code for distributed transaction coordination is written in the polarx plug-in. You need to execute the create extension polarx statement to install polarx before you can use the feature.

## Cleanup of residual 2PC transactions

The pgxc_clean utility in the src/bin directory can help you clean up residual 2PC transactions. After the patch is added, the kernel will record complete 2PC information and can determine the cleanup measures for residual 2PC transactions.  Then, copy src/bin/pgxc_clean to the src/bin of PostgreSQL 11.2. After the pgxc_clean code is compiled, you can use the pgxc_clean utility.

Usage： 
```
pgxc_clean -h $host -d $dbname -U $dbuser -p $port
```
host, port, dbname, and dbuser can be replaced with the information of the coordinator.

# When the plug-in is not used
You can download a version branch of PolarDB for PostgreSQL, which includes full distributed database features.

___

Copyright © Alibaba Group, Inc.
