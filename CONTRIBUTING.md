## Commits、Issues And Pull Requests

* 写出简洁、有意义的 git commit 信息(确人你阅读了 http://chris.beams.io/posts/git-commit/)
* Commit message 应该关联对应 issue
* 可以在 issue 中添加相关上下文、你的分析等等你认为必要的信息，还可以与他人讨论
* PR 的描述包含关闭对应 issues 的语句（可参考 https://github.com/blog/1506-closing-issues-via-pull-requests）
* 对于很小很小的改动，可以合并 commits
* 如果你认为一个 issue 不能在一天内完成，那么它应该被分成多个 issues
* 测试、文档应该随着代码一起提交、持续集成，而不是写完代码几个月后再补充
* 确保 CI 和测试通过

## 代码风格

* 必须使用 [editorconfig](http://editorconfig.org/#download) 来维持代码风格

## 提交 Bug

* 当你提交一个 bug 报道的时候，你要确保提供了复现步骤