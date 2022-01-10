module.exports = {
  "/zh/guide/": [
    {
      text: "快速上手",
      children: [
        "/zh/guide/README.md",
        "/zh/guide/deploy-on-cloud.md",
        "/zh/guide/deploy-on-local-storage.md",
        "/zh/guide/deploy-on-nbd-shared-storage.md",
        "/zh/guide/deploy-on-ceph-shared-storage.md",
        "/zh/guide/deploy-on-polardb-stack.md",
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
