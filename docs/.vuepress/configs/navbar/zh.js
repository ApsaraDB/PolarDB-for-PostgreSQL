module.exports = [
  {
    text: "快速上手",
    link: "/zh/guide/",
  },
  {
    text: "架构解读",
    link: "/zh/architecture/",
    children: [
      {
        text: "架构详解",
        link: "/zh/architecture/README.md",
      },
      {
        text: "缓冲区管理",
        link: "/zh/architecture/buffer-management.md",
      },
      {
        text: "DDL 同步",
        link: "/zh/architecture/ddl-synchronization.md",
      },
      {
        text: "LogIndex",
        link: "/zh/architecture/logindex.md",
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
        link: "/zh/contributing/contributing-polardb-kernel.md",
      },
      {
        text: "贡献文档",
        link: "/zh/contributing/contributing-polardb-docs.md",
      },
      {
        text: "编码风格",
        link: "/zh/contributing/coding-style.md",
      },
      {
        text: "行为准则",
        link: "/zh/contributing/code-of-conduct.md",
      },
    ],
  },
];
