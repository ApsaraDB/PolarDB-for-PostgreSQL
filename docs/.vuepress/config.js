const { navbar, sidebar } = require("./configs");

module.exports = {
  // lang: "zh-CN",
  // sidebar: false,
  // navbar: false,

  base: "/PolarDB-for-PostgreSQL/",

  head: [
    ["link", { rel: "icon", href: "/PolarDB-for-PostgreSQL/favicon.ico" }],
  ],

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
      "@vuepress/plugin-docsearch",
      {
        appId: "OYQ6LCESQG",
        apiKey: "748b096a5ca5958b2da16301f213d7b1",
        indexName: "polardb-for-postgresql",
        locales: {
          "/zh/": {
            placeholder: "搜索文档",
            translations: {
              button: {
                buttonText: "搜索文档",
                buttonAriaLabel: "搜索文档",
              },
              modal: {
                searchBox: {
                  resetButtonTitle: "清除查询条件",
                  resetButtonAriaLabel: "清除查询条件",
                  cancelButtonText: "取消",
                  cancelButtonAriaLabel: "取消",
                },
                startScreen: {
                  recentSearchesTitle: "搜索历史",
                  noRecentSearchesText: "没有搜索历史",
                  saveRecentSearchButtonTitle: "保存至搜索历史",
                  removeRecentSearchButtonTitle: "从搜索历史中移除",
                  favoriteSearchesTitle: "收藏",
                  removeFavoriteSearchButtonTitle: "从收藏中移除",
                },
                errorScreen: {
                  titleText: "无法获取结果",
                  helpText: "你可能需要检查你的网络连接",
                },
                footer: {
                  selectText: "选择",
                  navigateText: "切换",
                  closeText: "关闭",
                  searchByText: "搜索提供者",
                },
                noResultsScreen: {
                  noResultsText: "无法找到相关结果",
                  suggestedQueryText: "你可以尝试查询",
                  openIssueText: "你认为该查询应该有结果？",
                  openIssueLinkText: "点击反馈",
                },
              },
            },
          },
        },
      },
    ],
  ],
};
