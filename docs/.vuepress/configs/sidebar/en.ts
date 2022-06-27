import type { SidebarConfig } from "@vuepress/theme-default";

export const en: SidebarConfig = {
  "/guide/": [
    {
      text: "Guide",
      children: [
        "/guide/quick-start.md",
        "/guide/introduction.md",
        {
          text: "进阶部署",
          link: "/guide/deploy.md",
          children: [
            {
              text: "一、准备块存储设备",
              children: [
                "/guide/storage-aliyun-essd.md",
                "/guide/storage-ceph.md",
                "/guide/storage-nbd.md",
              ],
            },
            {
              text: "二、准备文件系统",
              children: ["/guide/fs-pfs.md"],
            },
            {
              text: "三、编译部署 PolarDB 内核",
              children: ["/guide/db-localfs.md", "/guide/db-pfs.md"],
            },
            {
              text: "四、 PolarDB 备份恢复",
              children: ["/guide/backup-and-restore.md"],
            },
          ],
        },
        "/guide/customize-dev-env.md",
        "/guide/deploy-more.md",
        {
          text: "特性体验",
          children: ["/guide/tpch-on-px.md"],
        },
      ],
    },
  ],
  "/architecture/": [
    {
      text: "Architecture Introduction",
      children: [
        "/architecture/README.md",
        "/architecture/buffer-management.md",
        "/architecture/ddl-synchronization.md",
        "/architecture/logindex.md",
      ],
    },
  ],
  "/roadmap/": [
    {
      text: "Roadmap",
      children: ["/roadmap/README.md"],
    },
  ],
  "/contributing": [
    {
      text: "Community",
      children: [
        "/contributing/contributing-polardb-kernel.md",
        "/contributing/contributing-polardb-docs.md",
        "/contributing/coding-style.md",
        "/contributing/code-of-conduct.md",
      ],
    },
  ],
};
