# polarx 分布式插件当前版本使用限制

* 不支持跨CN sequence.

* 不支持全局vacuum.

* 不支持不指定表的analyze.

* 不支持reindex.

* 不支持一条语句执行多个sql.

* 不支持复杂DML中使用DEFERRABLE constraints.

* 不支持全局OID.

* 不支持CREATE INDEX CONCURRENTLY.

* 不支持使用OID作为分片键.

* 不支持在创建一个非复制表的类型化的表中使用constraint.

* 不支持explain analyze select into.

* 不支持从coordinator查询非本地表的xmin,xmax.

* 不支持使用Volatile函数更新复制表.

* 不支持显示prepared transaction. 

* 不支持 TABLESAMPLE. 

* 不支持 SSI 隔离级别.

* 不支持  WHERE CURRENT OF.

* 权限管理：不支持只对view 赋予权限. 如果需要给view赋予权限，则同时也要赋予展开表权限.

* 权限管理：不支持没有foreign server 权限的角色，使用select into. 如需使用需要授权这个角色使用foreign server的权限.

* 触发器：不支持在coordinator上执行内部触发器.


___

Copyright © Alibaba Group, Inc.


