[English](./CONTRIBUTING_EN.md) | [中文](./CONTRIBUTING.md)

# 贡献指南

感谢您的贡献，在贡献之前请认真阅读并签署[贡献者许可协议（CLA）](./Contributor%20License%20Agreement.md)，您可以通过以下方式参与本项目。

- 报告错误
- 功能请求
- 提交拉取请求
- Commit规范

## 报告错误

在提交错误之前，请搜索现有问题以防止已经提交或已经修复。如果您提出问题，请包含以下内容：

- 关于操作系统的完整信息
- 正在运行的CnosDB版本
- 如果可能的话，明确重现描述问题步骤

## 功能请求

请明确您的要求和目标，通过示例帮助我们了解添加到CnosDB的原因。如果您发现功能请求已作为问题存在，请点击:+1:来表明您对功能的支持。

## 提交拉取请求

提交拉取请求，应该在CnosDB仓库中建立分支，并在功能分支上进行更改，然后从您的CnosDB仓库的master分支生成拉取请求。在您的拉取请求中应包含更改的详细信息、原因和方式以及您执行的测试。此外，请确保更改到位的情况下运行测试套件。导致测试失败的更改无法提交。

为了帮助审查PR，请将以下内容添加到您的拉取评论中：

```
- [ ] CHNAGELOG.md update
- [ ] Rebased/mergable
- [ ] Tests pass
```

## Commit规范

### 格式

> Commit message 包含三个部分：header，body和footer，中间用空行隔开。

```
<type>[optional scope]: <description>
// 空行
[optional body]
// 空行
[optional footer(s)]
```

#### Header

Header只有一行，包含三个字段：`type`（必需），`scope`（可选），description（必需）

`type`的种类包括：

| 类型     | 说明                                                         |
| -------- | ------------------------------------------------------------ |
| feat     | 新增功能                                                     |
| fix      | Bug修复                                                      |
| perf     | 提高代码性能的变更                                           |
| style    | 代码格式类的变更，比如用`gofmt`格式化代码、删除空行等        |
| refactor | 其他代码类的变更，这些变更不属于feat、fix、perf和style，例如简化代码、重命名变量、删除冗余代码等 |
| test     | 新增测试用例或是更新现有测试用例                             |
| ci       | 持续集成和部署相关的改动，比如修改jenkins、GitLab CI等CI配置文件或者更新systemd unit文件 |
| docs     | 文档类的更新，包括修改用户文档或者开发文档等                 |
| chore    | 其他类型，比如构建流程、依赖管理或者辅助工具的变动等         |

如何确定一个 commit 所属的 `type`：

![img](https://github.com/cnosdatabase/cnosdb/blob/main/doc/assets/commit_scope.png)

`scope`用于说明commit影响的范围，scope 如下：

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

`description`是commit的简短描述，规定不超过72个字符

#### Body

> Body是对本次commit的详细描述，可以分成多行
>
> 注意点：
>
> - 使用第一人称现在时，比如使用change而不是changed或changes。
> - 详细描述代码变动的动机，以及前后行为的对比

#### Footer

> 如果当前代码与上一个版本不兼容，则 Footer 部分以BREAKING CHANGE开头，后面是对变动的描述、以及变动理由和迁移方法。

> 关闭Issue，如果当前 commit 针对某个issue，那么可以在 Footer 部分关闭这个 issue

```
Closes #1234,#2345
```

#### Revert

> 除了 Header、Body 和 Footer 这 3 个部分，Commit Message 还有一种特殊情况：如果当前 commit 还原了先前的 commit，则应以 revert: 开头，后跟还原的 commit 的 Header。而且，在 Body 中必须写成 This reverts commit ，其中 hash 是要还原的 commit 的 SHA 标识。例如：

```
revert: feat(core): add 'Host' option

This reverts commit 079360c7cfc830ea8a6e13f4c8b8114febc9b48a.
```
