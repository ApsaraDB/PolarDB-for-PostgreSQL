module.exports = {
  "/guide/": [
    {
      text: "Getting Started",
      children: [
        "/guide/README.md",
        "/guide/deploy-on-cloud.md",
        '/guide/deploy-on-local-storage.md',
        '/guide/deploy-on-nbd-shared-storage.md',
        '/guide/deploy-on-ceph-shared-storage.md',
        "/guide/deploy-on-polardb-stack.md",
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
