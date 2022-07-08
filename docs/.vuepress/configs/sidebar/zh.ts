import type { SidebarConfig } from "@vuepress/theme-default";

export const zh: SidebarConfig = {
  "/zh/deploying": [
    {
      text: "部署指南",
      children: [
        "/zh/deploying/introduction.md",
        "/zh/deploying/quick-start.md",
        {
          text: "进阶部署",
          link: "/zh/deploying/deploy.md",
          children: [
            {
              text: "存储设备的准备",
              children: [
                "/zh/deploying/storage-aliyun-essd.md",
                "/zh/deploying/storage-ceph.md",
                "/zh/deploying/storage-nbd.md",
              ],
            },
            {
              text: "文件系统的准备",
              children: ["/zh/deploying/fs-pfs.md"],
            },
            {
              text: "编译部署 PolarDB 内核",
              children: [
                "/zh/deploying/db-localfs.md",
                "/zh/deploying/db-pfs.md",
              ],
            },
          ],
        },
        {
          text: "更多部署方式",
          children: [
            "/zh/deploying/deploy-stack.md",
            "/zh/deploying/deploy-official.md",
          ],
        },
      ],
    },
  ],
  "/zh/operation/": [
    {
      text: "使用与运维",
      children: [
        {
          text: "日常运维",
          children: ["/zh/operation/backup-and-restore.md"],
        },
        {
          text: "性能测试",
          children: ["/zh/operation/tpcc-test.md"],
        },
      ],
    },
  ],
  "/zh/features": [
    {
      text: "特性实践",
      children: [
        {
          text: "HTAP",
          children: ["/zh/features/tpch-on-px.md"],
        },
      ],
    },
  ],
  "/zh/theory/": [
    {
      text: "原理解读",
      children: [
        "/zh/theory/arch-overview.md",
        "/zh/theory/buffer-management.md",
        "/zh/theory/ddl-synchronization.md",
        "/zh/theory/logindex.md",
      ],
    },
  ],
  "/zh/development/": [
    {
      text: "上手开发",
      children: [
        "/zh/development/dev-on-docker.md",
        "/zh/development/customize-dev-env.md",
      ],
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
