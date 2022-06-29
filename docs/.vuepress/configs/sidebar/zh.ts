import type { SidebarConfig } from "@vuepress/theme-default";

export const zh: SidebarConfig = {
  "/zh/guide/": [
    {
      text: "入门指南",
      children: [
        "/zh/guide/quick-start.md",
        "/zh/guide/introduction.md",
        {
          text: "进阶部署",
          link: "/zh/guide/deploy.md",
          children: [
            {
              text: "一、准备块存储设备",
              children: [
                "/zh/guide/storage-aliyun-essd.md",
                "/zh/guide/storage-ceph.md",
                "/zh/guide/storage-nbd.md",
              ],
            },
            {
              text: "二、准备文件系统",
              children: ["/zh/guide/fs-pfs.md"],
            },
            {
              text: "三、编译部署 PolarDB 内核",
              children: ["/zh/guide/db-localfs.md", "/zh/guide/db-pfs.md"],
            },
            {
              text: "四、 PolarDB 备份恢复",
              children: ["/zh/guide/backup-and-restore.md"],
            },
          ],
        },
        "/zh/guide/customize-dev-env.md",
        "/zh/guide/deploy-more.md",
        {
          text: "特性体验",
          children: ["/zh/guide/tpch-on-px.md"],
        },
      ],
    },
  ],
  "/zh/architecture/": [
    {
      text: "架构解读",
      children: [
        "/zh/architecture/README.md",
        "/zh/architecture/buffer-management.md",
        "/zh/architecture/ddl-synchronization.md",
        "/zh/architecture/logindex.md",
      ],
    },
  ],
  "/zh/roadmap/": [
    {
      text: "版本规划",
      children: ["/zh/roadmap/README.md"],
    },
  ],
  "/zh/contributing": [
    {
      text: "参与社区",
      children: [
        "/zh/contributing/contributing-polardb-kernel.md",
        "/zh/contributing/contributing-polardb-docs.md",
        "/zh/contributing/coding-style.md",
        "/zh/contributing/code-of-conduct.md",
      ],
    },
  ],
};
