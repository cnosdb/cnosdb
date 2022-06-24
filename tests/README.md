# CnosDB CI tests

In this directory are some integration tests that will be run in CI.

## How to run these test cases

If you want to run these tests on your own machine, you can execute this line of command below.

```bash
cd cnosdb
go test -v ./tests
```

This will start a local cnosdb and then run all tests on it.

If you want to test against a cnosdb that has been started early, you just need to set an environment variable URL.

```bash
export URL=http://ip:port
go test -parallel 1 ./tests
```

## What is tested here

TODO
## What is the architecture of the test suite

* server_helpers.go: The Server interface is defined and encapsulates the database related operations.

* server_suite.go: A large number of statement-based test cases are stored and will be tested in server_test.go.

* server_test.go: Defines the TestMain function, which is the entry point for the execution of the entire test suite.


## How to add a new test case

TODO

















