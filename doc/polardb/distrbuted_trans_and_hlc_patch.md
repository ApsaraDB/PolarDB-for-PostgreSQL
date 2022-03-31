# 不使用HLC patch
Polardb for PostgreSQL分布式将分布式事务的协调逻辑进行了插件化。即在事务提交时，如果write set涉及了两个及以上节点，则会使用两阶段提交（2pc or two-phase commit) 进行提交。因为插件中缺少基于时间戳的MVCC机制，所以不使用HLC patch时，分布式事务不能保证一致性。可能发生A节点上该事务的write set被其他事务可见，而在B节点上同一事务的write set不能被其他事务可见。
两阶段提交协调逻辑代码在contrib/polarx 中，所以需要安装插件polarx才可以使用该功能。
## 两阶段提交残留事务的清理
src/bin/pgxc_clean 提供了简单的两阶段提交残留事务的清理。同样因为缺少patch提供的commit point记录功能，只能根据用户的输入，对所有的残留prepared事务进行全部提交或全部回滚。
用法：
``` 
pgxc_clean -h $host -d $dbname -U $dbuser -p $port -S -H 0
```
host port dbname dbuser等为协调节点的信息。
其中 
-S 表示使用simple_clean_2pc模式，这种模式下只会根据-H 提供的内容进行全部回滚或全部提交prepared 事务。
-H 可以有两个取值。 0 表示回滚所有prepared 事务。 这也是默认值。
                 1 表示提交所有prepared 事务。
	
# 使用 HLC patch
当前为社区PG 11.2 进行了适配，提供了适用该版本的HLC patch。该patch解决了分布式事务不一致的问题。
## 如何在社区PG 11.2上应用patch
1.	下载PG11.2 内核代码
https://github.com/postgres/postgres/tree/REL_11_2
2.	打上patch。 patch在 patchs/HLC_patch_based_on_pg11_2.patch 
```
$ git apply /path/to/patch/HLC_patch_based_on_pg11_2.patch
```
3.	将Polardb for PostgreSQL分布式代码中的contrib/polarx 插件拷贝至社区PG11.2的contrib下。
4.	编译代码
可以使用 patchs/build.sh 进行编译。 直接执行 ./build.sh 即可。
5.	完成。因为分布式事务协调逻辑在插件polarx中，所以需要create extension polarx后才能使用。

## 两阶段提交残留事务的清理
src/bin/pgxc_clean 提供了两阶段提交残留事务的清理。因为打完patch后，内核会记录完备的2pc信息，可以自行判断对残留2pc事务的清理措施。
将src/bin/pgxc_clean 拷贝至社区11.2版本的src/bin下，编译安装后即可使用pgxc_clean
用法： 
```
pgxc_clean -h $host -d $dbname -U $dbuser -p $port
```
host port dbname dbuser等为协调节点的信息。

# 不使用插件
直接下载polardb for PG分布式版本分支，包含完整分布式数据库功能。
