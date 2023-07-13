import type { NavbarConfig } from "@vuepress/theme-default";

export const en: NavbarConfig = [
  {
    text: "Deployment",
    children: [
      "/deploying/introduction.html",
      "/deploying/quick-start.html",
      "/deploying/deploy.html",
      {
        text: "Preparation of Shared-Storage Device",
        children: [
          "/deploying/storage-aliyun-essd.html",
          "/deploying/storage-curvebs.html",
          "/deploying/storage-ceph.html",
          "/deploying/storage-nbd.html",
        ],
      },
      {
        text: "Preparation of File System",
        children: ["/deploying/fs-pfs.html", "/deploying/fs-pfs-curve.html"],
      },
      {
        text: "Deploying PolarDB",
        children: [
          "/deploying/db-localfs.html",
          "/deploying/db-pfs.html",
          "/deploying/db-pfs-curve.html",
        ],
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
        children: [
          "/operation/backup-and-restore.html",
          "/operation/grow-storage.html",
          "/operation/scale-out.html",
          "/operation/ro-online-promote.html",
        ],
      },
      {
        text: "Benchmarks",
        children: ["/operation/tpcc-test.html", "/operation/tpch-test.html"],
      },
    ],
  },
  {
    text: "Kernel Features",
    link: "/features/",
  },
  {
    text: "Theory",
    link: "/theory/",
    children: [
      {
        text: "PolarDB for PostgreSQL",
        children: [
          "/theory/arch-overview.html",
          "/theory/arch-htap.html",
          "/theory/buffer-management.html",
          "/theory/ddl-synchronization.html",
          "/theory/logindex.html",
        ],
      },
      {
        text: "PostgreSQL",
        children: ["/theory/analyze.html", "/theory/polar-sequence-tech.html"],
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
