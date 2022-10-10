# Polarx plug-in uses connection pools between coordinators and datanodes

## Connection pools:

The distributed plug-in polarx uses a connection pool to establish communication between coordinators and datanodes. Connection pools adopt a multi-threaded architecture. The main thread receives messages sent by all backend service processes through listening ports. The main thread then distributes messages based on their types to different threads for processing, and finally synchronizes the processing results to the backend service processes. Connection pools provide a way to reuse connections, reducing the overhead for repeatedly establishing new connections. You can also dynamically adjust the number of connections by configuring the parameters of the feature to release unused connections. By doing so, the idle connections can be used to prefetch resources, improving the efficiency of connection management. Connection pools are deployed as plug-ins and can dynamically start as workers. Each connection pool worker corresponds to one database.

## Connection pool parameters:

 pool works

The distributed plug-in polarx uses a connection pool to establish communication between coordinators and datanodes. Connection pools adopt a multi-threaded architecture. The main thread receives messages sent by all backend service processes through listening ports. The main thread then distributes messages based on their types to different threads for processing, and finally synchronizes the processing results to the backend service processes. Connection pools provide a way to reuse connections, reducing the overhead for repeatedly establishing new connections. You can also dynamically adjust the number of connections by configuring the parameters of the feature to release unused connections. By doing so, the idle connections can be used to prefetch resources, improving the efficiency of connection management. Connection pools are deployed as plug-ins and can dynamically start as workers. Each connection pool worker corresponds to one database.

Connection pool parameters:

1. pooler.persistent_datanode_connections: specifies that the connection pool does not release connections permanently. Default value: false.
2. pooler.pool_conn_keepalive: sets the maximum time to live for an idle connection. Default value: 60. Unit: seconds.
3. pooler.pool_maintenance_timeout: sets the maintenance interval of the connection pool. If the idle duration of the connection pool exceeds the specified time, the maintenance operation is performed. Default value: 10. Unit: seconds.
4. pooler.max_pool_size: sets the maximum number of connections. If the total number of connections in the connection pool exceeds this value, new connections are rejected. Default value: 300.
5. pooler.min_pool_size: sets the minimum number of connections. If the total number of connections in the connection pool is less than this value, new idle connections will be established. Default value: 5.
6. pooler.port: specifies the service port of the connection pool . Default value: 6667.
7. pooler.pool_print_stat_timeout: sets the time interval to return connection pool status information. Default value: 60. Unit: seconds. If you set this parameter to -1, the feature is disabled.
8. pooler.pooler_scale_factor: sets the number of parallel threads in the connection pool. Default value: 2.
9. pooler.pooler_dn_set_timeout: sets the timeout period for the connection pool to wait for datanode messages. Default value: 10. Unit: seconds.
10. pooler.pool_session_memory_limit: sets the memory usage limit of a session. Default value: 10. Unit: MB. If the memory usage of a session on a node exceeds this limit, the session is closed.
11. pooler.pool_session_max_lifetime: sets the maximum time to live for a session. Default value: 300. Unit: seconds.
12. pooler.pool_session_context_check_gap: sets the time interval for the connection pool to check the memory usage of a session. Default value: 120. Unit: seconds.
13. pooler.min_free_size: sets the minimum number of idle connections in the connection pool. Default value: 5.
14. pooler.pooler_connect_timeout: sets the timeout period for creating a connection. Default value: 10. Unit: seconds.

___

Copyright © Alibaba Group, Inc.
