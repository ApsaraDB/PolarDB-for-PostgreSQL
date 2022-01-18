module.exports = [
  {
    text: "Getting Started",
    link: "/guide/",
  },
  {
    text: "Architecture",
    link: "/architecture/",
    children: [
      {
        text: "Overview",
        link: "/architecture/README.md",
      },
      {
        text: "Buffer Management",
        link: "/architecture/buffer-management.md",
      },
      {
        text: "DDL Synchronization",
        link: "/architecture/ddl-synchronization.md",
      },
      {
        text: "LogIndex",
        link: "/architecture/logindex.md",
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
        link: "/contributing/contributing-polardb-kernel.md",
      },
      {
        text: "Docs Contributing",
        link: "/contributing/contributing-polardb-docs.md",
      },
      {
        text: "Coding Style",
        link: "/contributing/coding-style.md",
      },
      {
        text: "Code of Conduct",
        link: "/contributing/code-of-conduct.md",
      },
    ],
  },
];
