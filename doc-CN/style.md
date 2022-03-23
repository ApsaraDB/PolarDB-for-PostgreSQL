## 编程语言
* PostgreSQL的内核、扩展以及内核相关的工具都使用C语言，这样可以兼容社区版本，易于升级。
* 管理工具可以使用Shell、Go或Python实现高效开发。

## 编程风格
* C语言中的编码遵循PostgreSQL的编程风格，如命名、错误消息格式、控制语句、代码行长度、注释格式、函数长度和全局变量。详情请查阅[PostgreSQL Coding Conventions](https://www.postgresql.org/docs/12/source.html)。请注意：
   * PostgreSQL的代码应该仅依赖于C99标准中可用的语言功能。
   * 不要使用//进行注释。
   * 可以使用带有参数的宏和静态内联函数。仅当前者简化编码时，后者才是优选。
   * 遵循BSD C编程约定。

* Shell、Go或Python编写的程序可以遵循Google代码约定。
   * <https://google.github.io/styleguide/pyguide.html>
   * <https://github.com/golang/go/wiki/CodeReviewComments>
   * <https://google.github.io/styleguide/shellguide.html>

## 代码设计和审查

关于代码审查，阿里云同样遵循[Google开源代码审查](https://github.com/google/eng-practices/blob/master/review/index.md)中的思想和规则。

在提交代码审查之前，请进行单元测试，并通过src/test下的所有测试，如回归测试和隔离测试。单元测试或功能测试应与代码修改一并提交。

除了代码审查之外，本文档还提供了整个高质量开发周期的说明，包括设计、实施、测试、文档开发和准备代码审查。在开发过程中，关键步骤存在很多问题，比如关于设计、功能、复杂性、测试、命名、文档开发以及代码审查的问题。本文档总结了如下代码审查规则。

*进行代码审查时，应该确保：*

* *代码设计完善。*
* *代码的功能性对使用者友好。*
* *任何UI改变都合理美观。*
* *任何并行编程都安全完成。*
* *代码不应“过分”复杂。*
* *开发人员不该实现一个现在不用而未来可能需要的功能。*
* *代码具有适当的单元测试。*
* *测试经过完善的设计。*
* *命名清晰。*
* *代码注释清晰且有用，主要解释为什么而非是什么。*
* *有适当的文档对代码进行说明。*
* *代码符合阿里云代码的风格指南。*

___

© 阿里巴巴集团控股有限公司 版权所有