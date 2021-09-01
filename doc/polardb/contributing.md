## How to contribute

PolarDB for PostgreSQL is an open source product from PostgreSQL and other open source projects. Our main target is to create a larger community for PostgreSQL. Contributors are welcome to submit their code and ideas. In a long run, we hope this project can be managed by developers from both inside and outside Alibaba. 

### Before contributing

* read and follow our [Code of Conduct](CODE_OF_CONDUCT.md)
* sign CLA of PolarDB for PostgreSQL:
Please download [PolarDB CLA](https://gist.github.com/alibaba-oss/151a13b0a72e44ba471119c7eb737d74). Follow the instructions to sign it. 

Here is a checklist to prepare and submit your PR (pull request). 

* create your own Github branch by forking alibaba/PolarDB-for-PostgreSQL
* checkout [readme](https://github.com/alibaba/PolarDB-for-PostgreSQL#deployment-from-source-code) for how to start PolarDB from source code; more deployment information can be found in [deployment](deployment.md)
* push changes to your personal fork and make sure they follow our [coding style](style.md)
* run basic testing, such as **make check** or **make check-world-dma**, see [regress](regress.md)
* create a PR with a detail description, if commit messages do not express themselves
* submit PR for review and address all feedbacks
* wait for merging (done by committers)

Let's use an example to walk through the list. 

## An Example of Submitting Code Change to PolarDB

### fork your own branch

On [PolarDB Github page](https://github.com/alibaba/PolarDB-for-PostgreSQL), Click **fork** button to create your own PolarDB repo

### create local repository 
```bash
git clone https://github.com/your_github/PolarDB-for-PostgreSQL.git
```
### create a dev branch (named as test-github)
```bash
git branch test-github
```
### make changes and commit locally
```bash
git status
git add files-to-change
git commit -m "modification for test-github"
```

### rebase on master and commit to remote repo
```bash
git checkout master
git pull
git checkout test-github
git rebase master
-- resolve conflict, compile and test --
git push origin test-github
```

### create a PR 
click **New pull request** or **Compare & pull  request** button, choose to compare branches alibaba/PolarDB-for-PostgreSQL and your_github/test-github, and write PR description.

### Address reviewers' comments
resolve all problems raised by reviewers and update PR

### Merge
It is done by PolarDB committers. 
___

Copyright © Alibaba Group, Inc.
