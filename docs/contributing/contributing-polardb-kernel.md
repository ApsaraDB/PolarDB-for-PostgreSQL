# Code Contributing

PolarDB for PostgreSQL is an open source product from PostgreSQL and other open source projects. Our main target is to create a larger community for PostgreSQL. Contributors are welcomed to submit their code and ideas. In a long run, we hope this project can be managed by developers from both inside and outside Alibaba.

## Branch Description and Management

- `POLARDB_11_STABLE` is the stable branch of PolarDB, it can accept the merge from `POLARDB_11_DEV` only
- `POLARDB_11_DEV` is the stable development branch of PolarDB, it can accept the merge from both pull requests and direct pushes from maintainers

New features will be merged to `POLARDB_11_DEV`, and will be merged to `POLARDB_11_STABLE` periodically by maintainers

## Before Contributing

- Sign the [CLA](https://gist.github.com/alibaba-oss/151a13b0a72e44ba471119c7eb737d74) of PolarDB for PostgreSQL

## Contributing

Here is a checklist to prepare and submit your PR (pull request):

- Create your own Github repository copy by forking `ApsaraDB/PolarDB-for-PostgreSQL`.
- Checkout documentations for [Advanced Deployment](../deploying/deploy.md) from PolarDB source code.
- Push changes to your personal fork and make sure they follow our [coding style](./coding-style.md).
- Create a PR with a detailed description, if commit messages do not express themselves.
- Submit PR for review and address all feedbacks.
- Wait for merging

## An Example of Submitting Code Change to PolarDB

Let's use an example to walk through the list.

### Fork Your Own Repository

On GitHub repository of [PolarDB for PostgreSQL](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL), Click **fork** button to create your own PolarDB repository.

### Create Local Repository

```bash
git clone https://github.com/<your-github>/PolarDB-for-PostgreSQL.git
```

### Create a Local Development Branch

Check out a new development branch from the stable development branch `POLARDB_11_DEV`. Suppose your branch is named as `dev`:

```bash
git checkout POLARDB_11_DEV
git checkout -b dev
```

### Make Changes and Commit Locally

```bash
git status
git add <files-to-change>
git commit -m "modification for dev"
```

### Rebase and Commit to Remote Repository

Click `Fetch upstream` on your own repository page to make sure your stable development branch is up do date with PolarDB official. Then pull the latest commits on stable development branch to your local repository.

```bash
git checkout POLARDB_11_DEV
git pull
```

Then, rebase your development branch to the stable development branch, and resolve the conflict:

```bash
git checkout dev
git rebase POLARDB_11_DEV
-- resolve conflict --
git push -f dev
```

### Create a Pull Request

Click **New pull request** or **Compare & pull request** button, choose to compare branches `ApsaraDB/PolarDB-for-PostgreSQL:POLARDB_11_DEV` and `<your-github>/PolarDB-for-PostgreSQL:dev`, and write PR description.

GitHub will automatically run regression test on your code. Your PR should pass all these checks.

### Address Reviewers' Comments

Resolve all problems raised by reviewers and update the PR.

### Merge

It is done by PolarDB maintainers.
