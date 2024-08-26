import type { NavbarConfig } from "@vuepress/theme-default";

export const zh: NavbarConfig = [
  {
    text: "部署指南",
    children: [
      "/zh/deploying/introduction.html",
      "/zh/deploying/quick-start.html",
      "/zh/deploying/deploy.html",
      {
        text: "共享存储设备的准备",
        children: [
          "/zh/deploying/storage-aliyun-essd.html",
          "/zh/deploying/storage-curvebs.html",
          "/zh/deploying/storage-ceph.html",
          "/zh/deploying/storage-nbd.html",
        ],
      },
      {
        text: "文件系统的准备",
        children: [
          "/zh/deploying/fs-pfs.html",
          "/zh/deploying/fs-pfs-curve.html",
        ],
      },
      {
        text: "部署 PolarDB-PG 数据库",
        children: [
          "/zh/deploying/db-localfs.html",
          "/zh/deploying/db-pfs.html",
          "/zh/deploying/db-pfs-curve.html",
        ],
      },
      {
        text: "更多部署方式",
        children: [
          "/zh/deploying/deploy-stack.html",
          "/zh/deploying/deploy-official.html",
        ],
      },
    ],
  },
  {
    text: "使用与运维",
    link: "/zh/operation/",
    children: [
      {
        text: "日常运维",
        children: [
          "/zh/operation/backup-and-restore.html",
          "/zh/operation/grow-storage.html",
          "/zh/operation/scale-out.html",
          "/zh/operation/ro-online-promote.html",
        ],
      },
      {
        text: "问题诊断",
        children: ["/zh/operation/cpu-usage-high.html"],
      },
      {
        text: "性能测试",
        children: [
          "/zh/operation/tpcc-test.html",
          "/zh/operation/tpch-test.html",
        ],
      },
    ],
  },
  {
    text: "自研功能",
    children: [
      {
        text: "功能总览",
        link: "/zh/features/",
      },
      {
        text: "PolarDB for PostgreSQL 11",
        link: "/zh/features/v11/",
        children: [
          "/zh/features/v11/performance/",
          "/zh/features/v11/availability/",
          "/zh/features/v11/security/",
          "/zh/features/v11/epq/",
          "/zh/features/v11/extensions/",
        ],
      },
    ],
  },
  {
    text: "原理解读",
    link: "/zh/theory/",
    children: [
      {
        text: "PolarDB for PostgreSQL",
        children: [
          "/zh/theory/arch-overview.html",
          "/zh/theory/arch-htap.html",
          "/zh/theory/buffer-management.html",
          "/zh/theory/ddl-synchronization.html",
          "/zh/theory/logindex.html",
        ],
      },
      {
        text: "PostgreSQL",
        children: [
          "/zh/theory/analyze.html",
          "/zh/theory/polar-sequence-tech.html",
        ],
      },
    ],
  },
  {
    text: "上手开发",
    link: "/zh/development/",
    children: [
      "/zh/development/dev-on-docker.html",
      "/zh/development/customize-dev-env.html",
    ],
  },
  {
    text: "参与社区",
    link: "/zh/contributing/",
    children: [
      "/zh/contributing/contributing-polardb-docs.html",
      "/zh/contributing/contributing-polardb-kernel.html",
      "/zh/contributing/coding-style.html",
      "/zh/contributing/trouble-issuing.md",
    ],
  },
];
