import type { NavbarConfig } from "@vuepress/theme-default";

export const en: NavbarConfig = [
  {
    text: "Guide",
    children: [
      "/guide/quick-start.html",
      "/guide/introduction.html",
      "/guide/deploy.html",
      {
        text: "准备块存储设备",
        children: [
          "/guide/storage-ceph.html",
          "/guide/storage-aliyun-essd.html",
          "/guide/storage-nbd.html",
        ],
      },
      {
        text: "准备文件系统",
        children: ["/guide/fs-pfs.html"],
      },
      {
        text: "编译部署 PolarDB 内核",
        children: ["/guide/db-localfs.html", "/guide/db-pfs.html"],
      },
      {
        text: "更多",
        children: [
          "/guide/backup-and-restore.html",
          "/guide/customize-dev-env.html",
          "/guide/deploy-more.html",
        ],
      },
      {
        text: "性能测试",
        children: ["/guide/tpch-on-px.html", "/zh/guide/tpcc-test.html"],
      },
    ],
  },
  {
    text: "Architecture",
    link: "/architecture/",
    children: [
      {
        text: "Overview",
        link: "/architecture/",
      },
      {
        text: "Buffer Management",
        link: "/architecture/buffer-management.html",
      },
      {
        text: "DDL Synchronization",
        link: "/architecture/ddl-synchronization.html",
      },
      {
        text: "LogIndex",
        link: "/architecture/logindex.html",
      },
    ],
  },
  {
    text: "Roadmap",
    link: "/roadmap/",
  },
  {
    text: "Community",
    link: "/contributing/",
    children: [
      {
        text: "Code Contributing",
        link: "/contributing/contributing-polardb-kernel.html",
      },
      {
        text: "Docs Contributing",
        link: "/contributing/contributing-polardb-docs.html",
      },
      {
        text: "Coding Style",
        link: "/contributing/coding-style.html",
      },
      {
        text: "Code of Conduct",
        link: "/contributing/code-of-conduct.html",
      },
    ],
  },
];
