# 贡献代码

::: danger
需要翻译
:::

PolarDB for PostgreSQL is an open source product from PostgreSQL and other open source projects. Our main target is to create a larger community for PostgreSQL. Contributors are welcomed to submit their code and ideas. In a long run, we hope this project can be managed by developers from both inside and outside Alibaba.

## Before Contributing

- Read and follow our [Code of Conduct](./code-of-conduct.md).
- Sign CLA of PolarDB for PostgreSQL:
  Please download [PolarDB CLA](https://gist.github.com/alibaba-oss/151a13b0a72e44ba471119c7eb737d74). Follow the instructions to sign it.

Here is a checklist to prepare and submit your PR (pull request):

- Create your own Github branch by forking `ApsaraDB/PolarDB-for-PostgreSQL`.
- Checkout [documentations](../guide/) for how to start PolarDB from source code.
- Push changes to your personal fork and make sure they follow our [coding style](./coding-style.md).
- Create a PR with a detailed description, if commit messages do not express themselves.
- Submit PR for review and address all feedbacks.
- Wait for merging (done by committers).

## An Example of Submitting Code Change to PolarDB

Let's use an example to walk through the list.

### Fork Your Own Branch

On GitHub repository of [PolarDB for PostgreSQL](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL), Click **fork** button to create your own PolarDB repository.

### Create Local Repository

```bash
git clone https://github.com/your_github/PolarDB-for-PostgreSQL.git
```

### Create a dev Branch (named as `test-github`)

```bash
git checkout -b test-github
```

### Make Changes and Commit Locally

```bash
git status
git add files-to-change
git commit -m "modification for test-github"
```

### Rebase and Commit to Remote Repository

```bash
git checkout main
git pull
git checkout test-github
git rebase main
-- resolve conflict, compile and test --
git push origin test-github
```

### Create a PR

Click **New pull request** or **Compare & pull request** button, choose to compare branches `ApsaraDB/PolarDB-for-PostgreSQL:main` and `your_github/PolarDB-for-PostgreSQL:test-github`, and write PR description.

### Address Reviewers' Comments

Resolve all problems raised by reviewers and update PR.

### Merge

It is done by PolarDB committers.
