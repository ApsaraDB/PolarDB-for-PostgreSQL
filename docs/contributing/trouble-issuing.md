# 问题报告

如果在运行 PolarDB for PostgreSQL 的过程中出现问题，请提供数据库的日志与机器的配置信息以方便定位问题。

通过 `polar_stat_env` 插件可以轻松获取数据库所在主机的硬件配置：

```sql:no-line-numbers
=> CREATE EXTENSION polar_stat_env;
=> SELECT polar_stat_env();
                           polar_stat_env
--------------------------------------------------------------------
 {                                                                 +
   "CPU": {                                                        +
     "Architecture": "x86_64",                                     +
     "Model Name": "Intel(R) Xeon(R) Platinum 8369B CPU @ 2.70GHz",+
     "CPU Cores": "8",                                             +
     "CPU Thread Per Cores": "2",                                  +
     "CPU Core Per Socket": "4",                                   +
     "NUMA Nodes": "1",                                            +
     "L1d cache": "192 KiB (4 instances)",                         +
     "L1i cache": "128 KiB (4 instances)",                         +
     "L2 cache": "5 MiB (4 instances)",                            +
     "L3 cache": "48 MiB (1 instance)"                             +
   },                                                              +
   "Memory": {                                                     +
     "Memory Total (GB)": "14",                                    +
     "HugePage Size (MB)": "2",                                    +
     "HugePage Total Size (GB)": "0"                               +
   },                                                              +
   "OS Params": {                                                  +
     "OS": "5.10.134-16.1.al8.x86_64",                             +
     "Swappiness(1-100)": "0",                                     +
     "Vfs Cache Pressure(0-1000)": "100",                          +
     "Min Free KBytes(KB)": "67584"                                +
   }                                                               +
 }
(1 row)
```
