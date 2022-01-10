---
home: true
title: PolarDB for PostgreSQL
heroImage: /images/polardb.png
actions:
  - text: 快速上手
    link: /zh/guide/
    type: primary
  - text: 架构解读
    link: /zh/architecture/
    type: secondary
features:
  - title: 极致弹性
    details: 存储与计算能力均可独立地横向扩展。
  - title: 毫秒级延迟
    details: WAL 日志存储在共享存储上；独创的 LogIndex 技术，实现了延迟回放和并行回放，理论上最大程度地缩小了 RW 和 RO 节点间的延迟。
  - title: HTAP 能力
    details: 基于 Shared-Storage 的分布式并行执行框架，加速在 OLTP 场景下的 OLAP 查询。
footer: Apache 2.0 Licensed | Copyright © Alibaba Group, Inc.
---
