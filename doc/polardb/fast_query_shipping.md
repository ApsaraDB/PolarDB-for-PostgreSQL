# Fast Query Shipping 引擎

## 架构介绍
Fast Query Shipping 引擎, 通过使用custom scan 的封装，实现了插件化，在一些简单的语句经过下推逻辑判断，如果可以下推，通过Fast Query Shipping优化器，快速生成remote query 执行计划，并将remote query plan 封装到custom scan中，通过custom scan 执行器，并行执行下推语句。通过Fast Query Shipping 引擎，减少了查询优化时间，提高了sql处理效率。

## 参数开关介绍：

1. polarx.enable_fast_query_shipping 参数
设置fast query shipping 功能开关，设置为true，表示打开，false 为关闭，默认值为true..

