# Contributing

We welcome all contributions! Please open a [pull request](https://github.com/cmu-db/benchbase/pulls). Common contributions may include:

- Adding support for a new DBMS.
- Adding more tests of existing benchmarks.
- Fixing any bugs or known issues.

## Contents

<!-- TOC -->

- [Contributing](#contributing)
    - [Contents](#contents)
    - [IDE](#ide)
    - [Adding a new DBMS](#adding-a-new-dbms)
    - [Java Development Notes](#java-development-notes)
        - [Code Style](#code-style)
        - [Compiler Warnings](#compiler-warnings)
        - [Avoid var keyword](#avoid-var-keyword)
            - [Alternatives to arrays of generics](#alternatives-to-arrays-of-generics)
            - [this-escape warnings](#this-escape-warnings)

<!-- /TOC -->

## IDE

Although you can use any IDE you prefer, there are some configurations for [VSCode](https://code.visualstudio.com/) that you may find useful included in the repository, including [Github Codespaces](https://github.com/features/codespaces) and [VSCode devcontainer](https://code.visualstudio.com/docs/remote/containers) support to automatically handle dependencies, environment setup, code formatting, and more.

## Adding a new DBMS

Please see the existing MySQL and PostgreSQL code for an example.

## Java Development Notes

### Code Style

To allow reviewers to focus more on code content and not style nits, [PR #416](https://github.com/cmu-db/benchbase/pulls/416) added support for auto formatting code at compile time according to [Google Java Style](https://google.github.io/styleguide/javaguide.html) using [google-java-format](https://github.com/google/google-java-format) and [fmt-maven-plugin](https://github.com/spotify/fmt-maven-plugin) Maven plugins.

Be sure to commit and include these changes in your PRs when submitting them so that the CI pipeline passes.

Additionally, this formatting style is included in the VSCode settings files for this repo.

### Compiler Warnings

In an effort to enforce clean, safe, maintainable code, [PR #413](https://github.com/cmu-db/benchbase/pull/413) enabled the `-Werror` and `-Xlint:all` options for the `javac` compiler.

This means that any compiler warnings will cause the build to fail.

If you are seeing a build failure due to a compiler warning, please fix the warning or (on rare occassions) add an exception to the line causing the issue.

### Avoid `var` keyword

In general, we prefer to avoid the `var` keyword in favor of explicit types.

#### Alternatives to arrays of generics

Per the [Java Language Specification](https://docs.oracle.com/javase/tutorial/java/generics/restrictions.html#createArrays), arrays of generic types are not allowed and will cause compiler warnings (e.g., `unchecked cast`)

In some cases, you can extend a generic type to create a non-generic type that can be used in an array.

For instance,

```java
// Simple generic class overload to avoid some cast warnings.
class SomeTypeList extends LinkedList<SomeType> {}

SomeTypeList[] someTypeLists = new SomeTypeList[] {
    new SomeTypeList(),
    new SomeTypeList(),
    new SomeTypeList(),
};
```

#### `this-escape` warnings

`possible 'this' escape before subclass is fully initialized`

The `this-escape` warning above is caused by passing using `this.someOverridableMethod()` in a constructor.
This could in theory cause problems with a subclass not being fully initialized when the method is called.

Since many of our classes are not designed to be subclassed, we can safely ignore this warning by marking the class as `final` rather than completely rewrite the class initialization.
