import type { NavbarConfig } from "@vuepress/theme-default";

export const zh: NavbarConfig = [
  {
    text: "入门指南",
    children: [
      "/zh/guide/quick-start.html",
      "/zh/guide/introduction.html",
      "/zh/guide/deploy.html",
      {
        text: "准备块存储设备",
        children: [
          "/zh/guide/storage-ceph.html",
          "/zh/guide/storage-aliyun-essd.html",
          "/zh/guide/storage-nbd.html",
        ],
      },
      {
        text: "准备文件系统",
        children: ["/zh/guide/fs-pfs.html"],
      },
      {
        text: "编译部署 PolarDB 内核",
        children: ["/zh/guide/db-localfs.html", "/zh/guide/db-pfs.html"],
      },
      "/zh/guide/deploy-more.html",
      {
        text: "特性体验",
        children: ["/zh/guide/tpch-on-px.html"],
      },
    ],
  },
  {
    text: "架构解读",
    link: "/zh/architecture/",
    children: [
      {
        text: "架构详解",
        link: "/zh/architecture/",
      },
      {
        text: "缓冲区管理",
        link: "/zh/architecture/buffer-management.html",
      },
      {
        text: "DDL 同步",
        link: "/zh/architecture/ddl-synchronization.html",
      },
      {
        text: "LogIndex",
        link: "/zh/architecture/logindex.html",
      },
    ],
  },
  {
    text: "版本规划",
    link: "/zh/roadmap/",
  },
  {
    text: "参与社区",
    link: "/zh/contributing/",
    children: [
      {
        text: "贡献代码",
        link: "/zh/contributing/contributing-polardb-kernel.html",
      },
      {
        text: "贡献文档",
        link: "/zh/contributing/contributing-polardb-docs.html",
      },
      {
        text: "编码风格",
        link: "/zh/contributing/coding-style.html",
      },
      {
        text: "行为准则",
        link: "/zh/contributing/code-of-conduct.html",
      },
    ],
  },
];
