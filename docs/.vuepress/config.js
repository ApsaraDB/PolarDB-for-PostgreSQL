const { navbar, sidebar } = require("./configs");

module.exports = {
  // lang: "zh-CN",
  // sidebar: false,
  // navbar: false,

  base: "/PolarDB-for-PostgreSQL/",

  head: [["link", { rel: "icon", href: "./favicon.ico" }]],

  locales: {
    "/": {
      lang: "en-US",
      title: "PolarDB for PostgreSQL",
      description:
        "A cloud-native database service independently developed by Alibaba Cloud",
    },
    "/zh/": {
      lang: "zh-CN",
      title: "PolarDB for PostgreSQL",
      description: "阿里云自主研发的云原生数据库产品",
    },
  },

  themeConfig: {
    logo: "/images/polardb.png",
    repo: "ApsaraDB/PolarDB-for-PostgreSQL",

    // whether to enable light/dark mode
    darkMode: false,

    locales: {
      "/": {
        selectLanguageName: "English",

        // page meta
        editLinkText: "Edit this page on GitHub",

        // navbar
        navbar: navbar.en,

        // sidebar
        sidebarDepth: 1,
        sidebar: sidebar.en,
      },
      "/zh/": {
        selectLanguageName: "简体中文",

        selectLanguageName: "简体中文",
        selectLanguageText: "选择语言",
        selectLanguageAriaLabel: "选择语言",

        // page meta
        editLinkText: "在 GitHub 上编辑此页",
        lastUpdatedText: "上次更新",
        contributorsText: "贡献者",

        // custom containers
        tip: "提示",
        warning: "注意",
        danger: "警告",

        // navbar
        navbar: navbar.zh,

        // sidebar
        sidebarDepth: 3,
        sidebar: sidebar.zh,
      },
    },
  },

  plugins: [
    [
      "@vuepress/plugin-search",
      {
        locales: {
          "/": {
            placeholder: "Search",
          },
          "/zh/": {
            placeholder: "搜索",
          },
        },
      },
    ],
  ],
};
