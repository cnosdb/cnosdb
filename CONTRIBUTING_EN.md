[English](./CONTRIBUTING_EN.md) | [中文](./CONTRIBUTING.md)

# Contribution Guide

CnosDB is a community-driven open source project, and contributions to CnosDB should comply with our [Code of Conduct](./CnosDBWeChatUserGroupGuidelines.md),We thank anyone who has contributed to CnosDB.

Please read and sign the [Contributor License Agreement（CLA）](https://cla-assistant.io/cnosdatabase/cnosdb) carefully before contributing. You can participate in this project in the following ways
-   [Error Reporting](Error-Reporting)
-   [Feature Request](Feature-Request)
-   [Submit Pull Requests](Submit-Pull-Requests)
-   [Commit Specification](Commit-Specification)

## Error Reporting

Before you report an error, please search for existing issues to prevent them from being submitted or fixed. If you have a question, please include the following information:

-   Complete information about the operating system.
-   The version of the running CnosDB.
-   Clearly reproduce the steps of problems, if possible.

## Feature Request

Please be clear about your requirements and goals, and use examples to help us understand why it should be added to CnosDB. If you find that a feature request already exists as an issue, click 👍 to indicate your support for the feature. If the feature passes, the audit member will create a new branch for the feature to develop the new feature.

## Submit Pull Requests

When you submit pull requests, they should be branched locally and commit the merge on the develop branch of CnosDB. When the task starts, you should indicate that you have started developing the task in issue to avoid duplicate code development. You should include the details, reasons, methods and tests of changes in your pull requests.  Besides, please make sure that you run the test suite if the changes are in place. The changes that cause the test to fail could not be committed.
## Commit Specification

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
| ci       | Changes to continuous integration and continuous deployment, like modifying Ci configuration files such as jenkins and GitLab CI or updating systemd unit files. |
| docs     | Updates to document classes, including modifying user documents, developing documents, etc.                 |
| chore    | Other types, like building processes, dependency management, changes to auxiliary tools, etc.         |

`scope` is used to illustrate the scope of the impact of commit, scope is as follows:

- cmd
- cnosql
- common
- db
- meta
- parser
- pkg
- query
- server
- *

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
