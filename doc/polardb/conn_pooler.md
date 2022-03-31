# polarx 分布式插件采用连接池处理coordinator和datanode之间的通信

## 连接池参数介绍：

1. pooler.persistent_datanode_connections 参数
连接池永久不释放获得的连接。默认值为false.

2. pooler.pool_conn_keepalive
设置一个空闲连接最大存活时间.默认60秒。

3. pooler.pool_maintenance_timeout
设置连接池维护间隔，如果连接池空闲超过这个时间，就进行维护操作，默认设置10秒

4. pooler.max_pool_size
设置最大连接数，如果连接池总连接数超过这个值，将拒绝新的连接申请，默认值为300.

5. pooler.min_pool_size 
设置最小连接数，如果连接池总连接数小于这个值，将建立新的空闲连接，来位置最小连接数，默认值为5.

6. pooler.port
设置连接池服务端口, 默认6667.

7. pooler.pool_print_stat_timeout
设置连接池状态信息输出间隔，默认值为60秒，如果设置为-1 可关掉此功能。

8. 

