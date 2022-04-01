# 支持使用原生Postgresql语法建立分片表
Polardb for PostgreSQL分布式实现了插件化建表语句，通过原生语法可以建立分布式表。
## 分布式表建表语法介绍
通过原生SQL中CREATE TABLE 语法对WITH语句支持，将分布式表的分布信息加入到WITH语句中，通过dist_type记录表的分布类型，目前支持replication，hash, modulo, roundrobin 四种分布类型，通过dist_col记录分布列信息，目前只支持使用单列作为分布列，其他的语法同原生SQL CREATE TALBE 语法，可参考社区版Postgresql https://www.postgresql.org/docs/11/sql-createtable.html。下面以建立简单表举例：

显示创建复制表语法：
```
create table polarx_test(id int, name text) with (dist_type = replication);
```
显式创建hash分布式表语法：
```
create table polarx_test(id int, name text) with (dist_type = hash, dist_col = id);
```
显示创建modulo分布式表语法：
```
create table polarx_test(id int, name text) with (dist_type = modulo, dist_col = id);
```
显示创建roundrobin分布式表语法：
```
create table polarx_test(id int, name text) with (dist_type = roundrobin);
```
隐式创建分布式表语法, 默认从第一个列尝试建立hash 分布表，如果所有列都不可以建立，则最后建立roundrobin 分布表：
```
create table polarx_test(id int, name text);
```
# execute polarx_direct 语法
通过Coordinator节点 使用 execute polarx_direct 语法可以直接在对应节点上执行SQL语句。 
## 语法介绍：
```
execute polarx_direct($node_name, $sql);
```
node_name为设置的节点名称，sql 为要在这个节点上执行的语句。
这里以polarx_test表举例，假设有两个DN，分别为datanode1， datanode2。想查询datanode1节点上polarx_test表的内容语句如下：
```
execute polarx_direct('datanode1', 'select * from bmsql_warehouse;');
```
