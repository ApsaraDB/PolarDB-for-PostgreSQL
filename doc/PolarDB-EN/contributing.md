## How to contribute

PolarDB PostgreSQL is an open source product from PostgreSQL and other open source projects. Our main target is to create a larger community for PostgreSQL. Contributors are welcome to submit their code and ideas. In a long run, we hope this project can be managed by developers from both inside and outside Alibaba. 

### Before contributing

* Read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).
* Sign CLA of PolarDB for PostgreSQL:
Please download [PolarDB CLA](https://gist.github.com/alibaba-oss/151a13b0a72e44ba471119c7eb737d74). Follow the instructions to sign it. 

Here is a checklist to prepare and submit your PR (pull request). 

* Create your own Github branch by forking alibaba/PolarDB-for-PostgreSQL.
* Checkout [README](https://github.com/alibaba/PolarDB-for-PostgreSQL/blob/main/README.md) for how to start PolarDB from source code.
* Push changes to your personal fork and make sure they follow our [coding style](style.md).
* Create a PR with a detail description, if commit messages do not express themselves.
* Submit PR for review and address all feedbacks.
* Wait for merging (done by committers).

Let's use an example to walk through the list. 

## An Example of Submitting Code Change to PolarDB

### Fork Your Own Branch

On [PolarDB Github page](https://github.com/alibaba/PolarDB-for-PostgreSQL), Click **fork** button to create your own PolarDB repository.

### Create Local Repository 
```bash
git clone https://github.com/your_github/PolarDB-for-PostgreSQL.git
```
### Create a dev Branch (named as test-github)
```bash
git branch test-github
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
Click **New pull request** or **Compare & pull  request** button, choose to compare branches alibaba/PolarDB-for-PostgreSQL and your_github/test-github, and write PR description.

### Address Reviewers' Comments
Resolve all problems raised by reviewers and update PR.

### Merge
It is done by PolarDB committers. 
___

Copyright © Alibaba Group, Inc.
