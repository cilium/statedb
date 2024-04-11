# :memo: StateDB [![GoDoc](https://pkg.go.dev/badge/github.com/cilium/statedb)](https://pkg.go.dev/github.com/cilium/statedb) 

StateDB is an in-memory database for Go. It supports unique and non-unique indexes and objects
can have multiple keys.

The database is built on top of Persistent Adative Radix Trees (implemented in part/). It supports
multi-version concurrency control and transactional cross-table write transaction. Changes to the database
can be watched at fine-granularity which allows reacting to changes in the database.

