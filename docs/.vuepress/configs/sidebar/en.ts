import type { SidebarConfig } from "@vuepress/theme-default";

export const en: SidebarConfig = {
  "/deploying": [
    {
      text: "Deployment",
      children: [
        "/deploying/introduction.md",
        "/deploying/quick-start.md",
        {
          text: "Advanced Deployment",
          link: "/deploying/deploy.md",
          children: [
            {
              text: "Preparation of Storage Device",
              children: [
                "/deploying/storage-aliyun-essd.md",
                "/deploying/storage-curvebs.md",
                "/deploying/storage-ceph.md",
                "/deploying/storage-nbd.md",
              ],
            },
            {
              text: "Preparation of File System",
              children: ["/deploying/fs-pfs.md", "/deploying/fs-pfs-curve.md"],
            },
            {
              text: "Building PolarDB Kernel",
              children: ["/deploying/db-localfs.md", "/deploying/db-pfs.md"],
            },
          ],
        },
        {
          text: "More about Deployment",
          children: [
            "/deploying/deploy-stack.md",
            "/deploying/deploy-official.md",
          ],
        },
      ],
    },
  ],
  "/operation/": [
    {
      text: "Ops",
      children: [
        {
          text: "Daily Ops",
          children: ["/operation/backup-and-restore.md"],
        },
        {
          text: "Benchmarks",
          children: ["/operation/tpcc-test.md"],
        },
      ],
    },
  ],
  "/features": [
    {
      text: "Features Practice",
      children: [
        {
          text: "HTAP",
          children: ["/features/tpch-on-px.md"],
        },
      ],
    },
  ],
  "/theory/": [
    {
      text: "Theory",
      children: [
        {
          text: "PolarDB for PostgreSQL",
          children: [
            "/theory/arch-overview.md",
            "/theory/buffer-management.md",
            "/theory/ddl-synchronization.md",
            "/theory/logindex.md",
          ],
        },
        {
          text: "PostgreSQL",
          children: ["/theory/analyze.md"],
        },
      ],
    },
  ],
  "/development/": [
    {
      text: "Development",
      children: [
        "/development/dev-on-docker.md",
        "/development/customize-dev-env.md",
      ],
    },
  ],
  "/contributing": [
    {
      text: "Contributing",
      children: [
        "/contributing/contributing-polardb-kernel.md",
        "/contributing/contributing-polardb-docs.md",
        "/contributing/coding-style.md",
        "/contributing/code-of-conduct.md",
      ],
    },
  ],
};
