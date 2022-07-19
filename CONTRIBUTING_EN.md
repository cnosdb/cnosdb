[English](./CONTRIBUTING_EN.md) | [中文](./CONTRIBUTING.md)

# Contribution Guide

CnosDB is a community-driven open source project, and contributions to CnosDB should comply with our [Code of Conduct](./CODE_OF_CONDUCT.md),We thank anyone who has contributed to CnosDB.

Please read and sign the [Contributor License Agreement（CLA）](https://cla-assistant.io/cnosdb/cnosdb) carefully before contributing. You can participate in this project in the following ways
-   [Error Reporting](Error-Reporting)
-   [Feature Request](Feature-Request)
-   [Submit Pull Requests](Submit-Pull-Requests)

## Error Reporting

Before you report an error, please search for existing issues to prevent them from being submitted or fixed. If you have a question, please include the following information:

-   Complete information about the operating system.
-   The version of the running CnosDB.
-   Clearly reproduce the steps of problems, if possible.

## Feature Request

Please indicate your requirements and objectives and help us understand the reasons for adding to cnosdb through examples. If it is found that the feature request already exists as an issue, please click: + 1: to indicate your support for the feature.

## Submit Pull Requests

1. Search [GitHub](https://github.com/cnosdb/cnosdb/pulls) for an open or closed PR that relates to your submission.
   You don't want to duplicate existing efforts.

2. Be sure that an issue describes the problem you're fixing, or documents the design for the feature you'd like to add.
   Discussing the design upfront helps to ensure that we're ready to accept your work.

3. Please sign our [Contributor License Agreement (CLA)](https://cla-assistant.io/cnosdb/cnosdb) before sending PRs.
   We cannot accept code without a signed CLA.
   Make sure you author all contributed Git commits with email address associated with your CLA signature.

4. [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) the cnosdb/cnosdb repo.

5. In your forked repository, make your changes in a new git branch:

     ```shell
     git checkout -b my-fix-branch main
     ```

6. Add code and test case.

7. Push your branch to GitHub:

    ```shell
    git push origin my-fix-branch
    ```

8. In GitHub, send pull request to `cnosdb:main`.

## Commit Specification
> For more, See [Commit convention](https://www.conventionalcommits.org/en/v1.0.0/)

### Format

> Commit message includes three parts：header，body and footer, which separated by blank lines.

```
<type>[optional scope]: <description>
// blank lines
[optional body]
// blank lines
[optional footer(s)]

```

#### Header

Header has only one line, including three fields：`type`（required），`scope`（），description（required）

The types of `type` includes：

| type     | instructions                                                         |
| -------- | ------------------------------------------------------------ |
| feat     | new feature                                                   |
| fix      | fixes                                                    |
| perf     | changes improving code performance                                         |
| style    | Changes to the format class of the code, like using `gofmt` to format codes, delete the blank lines, etc.        |
| refactor | Changes to other classes of the code, which do not belong to feat、fix、perf and style, like simplifying code, renaming variables, removing redundant code, etc. |
| test     | Add test cases or update existing test cases                            |
| ci       | Changes to continuous integration and continuous deployment, like modifying Ci configuration files or updating systemd unit files. |
| docs     | Updates to document classes, including modifying user documents, developing documents, etc.                 |
| chore    | Other types, like building processes, dependency management, changes to auxiliary tools, etc.         |

`scope` is used to illustrate the scope of the impact of commit, scope is as follows:

- tskv
- meta
- query
- docs
- config
- tests
- utils
- \*

`description` is the short description of commit, which is specified not to exceed 72 characters.
#### Body

> Body is a detailed description of this commit, which can be divided into multiple lines.
>
> Notes:
>
> -   Use the first person and present tense, like using change instead of changed or changes.
> -   Describe the motivations for code changes in detail and the comparison of before and after behavior


#### Footer

> If the current code is not compatible with the previous version, the Footer section begins with BREAKING CHANGE, which is followed by a description of the changes, as well as the reasons for the changes and the method of migration.
> 
> Close Issue, if the current commit is for an issue, you can close the issue in the Footer section

```
Closes #1234,#2345

```

#### Revert

>In addition to the Header, Body, and Footer, Commit Message has a special case: If the current commit restores a previous commit, it should start with revert:, which is followed by a header of a restored commit. Besides, it must be written as This reverts commit  in the Body. Among them, hash is the SHA identity of the commit to be restored. For example：

```
revert: feat(core): add 'Host' option

This reverts commit 079360c7cfc830ea8a6e13f4c8b8114febc9b48a.
```
