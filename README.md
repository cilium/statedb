# :memo: StateDB [![GoDoc](https://pkg.go.dev/badge/github.com/cilium/statedb)](https://pkg.go.dev/github.com/cilium/statedb) 

StateDB is an in-memory database for Go. It stores immutable objects in
[immutable radix trees](https://github.com/hashicorp/go-immutable-radix), and thus
supports multi-version concurrency control (MVCC) and transactional write transactions.
Changes to the database can be watched at fine-granularity which allows reacting to changes in the database. 

The library is inspired by [go-memdb](https://github.com/hashicorp/go-memdb). The main differences
to it are type-safety via generics, builtin object revision numbers and per-table locking rather
than single database lock.

