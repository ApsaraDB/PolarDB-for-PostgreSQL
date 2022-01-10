# Coding Style

## Languages

- PostgreSQL kernel, extension, and kernel related tools use C, in order to remain compatible with community versions and to easily upgrade.
- Management related tools can use shell, GO, or Python, for efficient development.

## Style

- Coding in C follows PostgreSQL's programing style， such as naming, error message format, control statements, length of lines, comment format, length of functions, and global variable. For detail, please reference [Postgresql style](https://www.postgresql.org/docs/12/source.html). Here is some highlines:

  - Code in PostgreSQL should only rely on language features available in the C99 standard
  - Do not use // for comments
  - Both, macros with arguments and static inline functions, may be used. The latter is preferred only if the former simplifies coding.
  - Follow BSD C programming conventions

- Programs in Shell, Go, or Python can follow Google code conventions
  - https://google.github.io/styleguide/pyguide.html
  - https://github.com/golang/go/wiki/CodeReviewComments
  - https://google.github.io/styleguide/shellguide.html

## Code design and review

We share the same thoughts and rules as [Google Open Source Code Review](https://github.com/google/eng-practices/blob/master/review/index.md)

Before submitting for code review, please do unit test and pass all tests under src/test, such as regress and isolation. Unit tests or function tests should be submitted with code modification.

In addition to code review, this doc offers instructions for the whole cycle of high-quality development, from design, implementation, testing, documentation, to preparing for code review. Many good questions are asked for critical steps during development, such as about design, about function, about complexity, about test, about naming, about documentation, and about code review. The doc summarized rules for code review as follows.

_In doing a code review, you should make sure that:_

- _The code is well-designed._
- _The functionality is good for the users of the code._
- _Any UI changes are sensible and look good._
- _Any parallel programming is done safely._
- _The code isn't more complex than it needs to be._
- _The developer isn't implementing things they might need in the future but don't know they need now._
- _Code has appropriate unit tests._
- _Tests are well-designed._
- _The developer used clear names for everything._
- _Comments are clear and useful, and mostly explain why instead of what._
- _Code is appropriately documented._
- _The code conforms to our style guides._
