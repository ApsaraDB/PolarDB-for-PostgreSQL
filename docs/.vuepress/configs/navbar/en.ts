import type { NavbarConfig } from "@vuepress/theme-default";

export const en: NavbarConfig = [
  {
    text: "Deployment",
    children: [
      "/deploying/introduction.html",
      "/deploying/quick-start.html",
      "/deploying/deploy.html",
      {
        text: "Preparation of Storage Device",
        children: [
          "/deploying/storage-aliyun-essd.html",
          "/deploying/storage-ceph.html",
          "/deploying/storage-nbd.html",
        ],
      },
      {
        text: "Preparation of File System",
        children: ["/deploying/fs-pfs.html"],
      },
      {
        text: "Building PolarDB Kernel",
        children: ["/deploying/db-localfs.html", "/deploying/db-pfs.html"],
      },
      {
        text: "More about Deployment",
        children: [
          "/deploying/deploy-stack.html",
          "/deploying/deploy-official.html",
        ],
      },
    ],
  },
  {
    text: "Ops",
    link: "/operation/",
    children: [
      {
        text: "Daily Ops",
        children: ["/operation/backup-and-restore.html"],
      },
      {
        text: "Benchmarks",
        children: ["/operation/tpcc-test.html"],
      },
    ],
  },
  {
    text: "Features",
    link: "/features/",
    children: [
      {
        text: "HTAP",
        children: ["/features/tpch-on-px.html"],
      },
    ],
  },
  {
    text: "Theory",
    link: "/theory/",
    children: [
      {
        text: "PolarDB for PostgreSQL",
        children: [
          "/theory/arch-overview.html",
          "/theory/buffer-management.html",
          "/theory/ddl-synchronization.html",
          "/theory/logindex.html",
        ],
      },
      {
        text: "PostgreSQL",
        children: ["/theory/analyze.html"],
      },
    ],
  },
  {
    text: "Dev",
    link: "/development/",
    children: [
      {
        text: "Development on Docker",
        link: "/development/dev-on-docker.html",
      },
      {
        text: "Customize Development Environment",
        link: "/development/customize-dev-env.html",
      },
    ],
  },
  {
    text: "Contributing",
    link: "/contributing/",
    children: [
      {
        text: "Code of Conduct",
        link: "/contributing/code-of-conduct.html",
      },
      {
        text: "Contributing Docs",
        link: "/contributing/contributing-polardb-docs.html",
      },
      {
        text: "Contributing Code",
        link: "/contributing/contributing-polardb-kernel.html",
      },
      {
        text: "Coding Style",
        link: "/contributing/coding-style.html",
      },
    ],
  },
];
