# 贡献文档

PolarDB for PostgreSQL 的文档使用 [VuePress 2](https://v2.vuepress.vuejs.org/zh/) 进行管理，以 Markdown 为中心进行写作。

## 浏览文档

本文档在线托管于 [GitHub Pages](https://ApsaraDB.github.io/PolarDB-for-PostgreSQL/) 服务上。

## 本地文档开发

若您发现文档中存在内容或格式错误，或者您希望能够贡献新文档，那么您需要在本地准备文档开发环境。

### Node 环境准备

本项目的文档是一个 Node.js 工程，您需要在本地准备 Node.js 环境。[Node.js®](https://nodejs.org/zh-cn/) 是一个基于 Chrome V8 引擎的 JavaScript 运行时环境，可在官网 [下载](https://nodejs.org/zh-cn/download/) 页面直接下载安装包。

在 macOS 上可以直接使用 [Homebrew](https://brew.sh/) 安装：

```bash
brew install node
```

安装完毕后，使用如下命令检查安装是否成功：

```bash
node -v
npm -v
```

### 依赖安装

在 PolarDB for PostgreSQL 工程的根目录下运行以下命令，`npm` 将会根据 `package.json` 安装所有依赖：

```bash
npm install --registry https://registry.npm.taobao.org
```

### 运行文档开发服务器

在 PolarDB for PostgreSQL 工程的根目录下运行以下命令：

```bash
npm run docs:dev
```

文档开发服务器将运行于 `http://localhost:8080/PolarDB-for-PostgreSQL/`。打开浏览器即可访问。

## 文档目录组织

PolarDB for PostgreSQL 的文档资源位于工程根目录的 `docs/` 目录下。其目录被组织为：

```
└── docs
    ├── .vuepress
    │   ├── configs
    │   ├── public
    │   └── styles
    ├── README.md
    ├── architecture
    ├── contributing
    ├── guide
    ├── imgs
    ├── roadmap
    └── zh
        ├── README.md
        ├── architecture
        ├── contributing
        ├── guide
        ├── imgs
        └── roadmap
```

可以看到，`docs/zh/` 目录下是其父级目录除 `.vuepress/` 以外的翻版。`docs/` 目录中全部为英语文档，`docs/zh/` 目录下全部是相对应的简体中文文档。

`.vuepress/` 目录下包含文档工程的全局配置信息：

- `config.js`：文档配置
- `configs/`：文档配置模块（导航栏 / 侧边栏、英文 / 中文等配置）
- `public/`：公共静态资源
- `styles/`：文档主题默认样式覆盖

文档的配置方式请参考 VuePress 2 官方文档的 [配置指南](https://v2.vuepress.vuejs.org/zh/guide/configuration.html)。

## 文档开发规范

1. 新的文档写好后，需要在文档配置中配置路由（可参考其他已有文档）
2. 修正一种语言的文档时，也需要顺带修正其他语言的相同文档
3. 修改文档后，使用 [Prettier](https://prettier.io/) 工具对 Markdown 文档进行格式化。Prettier 支持的编辑器集成：
   - [Prettier-VSCode](https://github.com/prettier/prettier-vscode)
   - [Vim-Prettier](https://github.com/prettier/vim-prettier)

## 文档在线部署

本文档借助 [GitHub Actions](https://github.com/features/actions) 提供 CI 服务。向主分支推送代码时，将触发对 `docs/` 目录下文档资源的构建，并将构建结果推送到 [gh-pages](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL/tree/gh-pages) 分支上。[GitHub Pages](https://pages.github.com/) 服务会自动将该分支上的文档静态资源部署到 Web 服务器上形成文档网站。
