# pipeline

A multithreaded pipeline implementation with support of Go 1.18 generics.

### Installation

The webapp module can be installed by running the the following command in your
main Go module:

    go get github.com/qqiao/pipeline

### Documentation and Usage

API documentation and example code can be found at
[pkg.go.dev](https://pkg.go.dev/github.com/qqiao/pipeline).

### Known issues

- Currently, there is an bug with pkg.go.dev with regard to generics. 
  Examples with generics are not correctly associated to their function 
  signatures. This causes all the examples to be missing from pkg.go.dev. A 
  simple workaround is to use `godoc` to read the documentation locally. 
  Up-to-date local versions of `godoc` renders examples correctly.
