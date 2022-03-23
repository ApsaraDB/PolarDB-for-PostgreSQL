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
