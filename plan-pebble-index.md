# Plan

## PebbleIndex With Mixed Backends, IndexedBatch, and One-Time Restore

## Summary

Add a `PebbleIndex[Obj, Key]` that can be used as either a primary or secondary index alongside existing in-memory indexes on the same table. `PebbleIndex` is always unique, persists the full object plus its owning primary key and revision, and uses Pebble's own `IndexedBatch` for write-transaction read-your-own-writes semantics.

Add `func (db *DB) Restore() error` as a startup-only, once-only operation that restores every table containing at least one `PebbleIndex`. `Restore` rebuilds all in-memory indexes from one durable Pebble source per table; revision and graveyard indexes remain in-memory only.

## Public API and Interface Changes

- Add `PebbleIndex[Obj interface { encoding.BinaryMarshaler; encoding.BinaryUnmarshaler }, Key any]` as an `Indexer[Obj]`.
- `PebbleIndex` fields:
  - `Name string`
  - `FromObject func(Obj) index.KeySet`
  - `FromKey func(Key) index.Key`
  - `FromString func(string) (index.Key, error)`
  - `NewObj func() Obj`
- `PebbleIndex` is implicitly unique and total:
  - exactly one key per object
  - zero or multiple keys is an error
- Add `WithPebble(path string, opts *pebble.Options)` on `DB`.
- Add `func (db *DB) Restore() error`.
- Add explicit errors:
  - `ErrRestoreAlreadyCalled`
  - `ErrRestoreAfterUse`
  - `ErrPebbleIndexKeyCount`
  - `ErrPebblePrimaryMismatch`
  - `ErrPebbleIndexConflict`

## Implementation Changes

- Keep one shared Pebble DB per `statedb.DB`, namespaced by table name and index name.
- Allow mixed index backends within a table:
  - primary may be in-memory or Pebble-backed
  - secondary indexes may be in-memory or Pebble-backed
  - revision, graveyard, and graveyard-revision indexes stay in-memory only
- Persist each Pebble row as:
  - key: namespaced unique index key
  - value: encoded primaryKey, revision, marshaled object bytes
- Use Pebble's `IndexedBatch` for the outer `WriteTxn` whenever any locked table has a `PebbleIndex`.
- All Pebble-backed index txns for that write txn share the same batch.
- Reads and iterators against a Pebble-backed index inside the write txn use the batch directly.
- This replaces the need for a separate `partIndex` overlay.

### Benefit of IndexedBatch Over a `partIndex` Overlay

- one source of truth for staged Pebble mutations
- built-in merged reads against batch + committed DB
- built-in iterator support for `Prefix`, `LowerBound`, and `All`
- less duplicate logic for deletes/replacements and less commit translation code

### Cost of IndexedBatch

- writes are slower than a plain batch because Pebble maintains an internal skiplist index
- acceptable here because write txns need in-transaction reads and scans

### Important Iterator Rule From Pebble

- batch iterators see existing batch mutations, not later ones
- therefore every table API call creates a fresh iterator; do not cache iterators across writes within a txn

## Write Transaction Design

- Refactor `writeTxnState` to own an optional shared Pebble `IndexedBatch`.
- `pebbleIndex.txn()` returns an index txn bound to:
  - the shared batch during writes
  - the committed snapshot reader during reads
- On commit:
  - under the existing `db.mu` sequencing, commit/apply the shared Pebble batch first
  - create fresh committed Pebble readers for modified tables/indexes
  - swap the StateDB root
  - close index-level watch channels for changed Pebble indexes

## Operation Mapping and Semantics

### Get

- direct lookup in the shared `IndexedBatch` during write txns
- direct lookup in the committed Pebble reader during read txns
- decode stored object from row value

### List

- identical to `Get`, because `PebbleIndex` is always unique
- returns empty or singleton iterator

### Prefix, LowerBound, All

- use Pebble iterators over the namespaced key range
- no non-unique duplicate suppression is needed

### Insert / Modify for a Pebble-backed Primary Index

- direct key lookup by primary key
- overwrite same-key row with new revision/object payload

### Insert / Modify for a Pebble-backed Secondary Index

- compute the single secondary key
- if an existing row is present under that key, decode its stored `primaryKey`
- if stored `primaryKey` differs from the current object's primary key, fail with `ErrPebbleIndexConflict`
- if it matches, update in place with new revision/object payload

### Reindex for Pebble-backed Secondaries

- if old secondary key == new secondary key, update the row in place after validating `primaryKey`
- if the key changed, delete the old key and set the new key

### Delete

- delete Pebble rows for every Pebble-backed index using the object's computed key
- revision and graveyard handling remains current in-memory behavior

### DeleteAll

- keep current object-by-object semantics
- do not use range deletes

### Changes()

- unchanged at runtime because revision/graveyard are still in-memory
- after restart and restore, historical delete/change streams are not recovered

## Restore Design

- `Restore` signature is `func (db *DB) Restore() error`.
- `Restore` is allowed exactly once for the lifetime of the DB.
- `Restore` is startup-only:
  - if any table already contains live objects or has a non-zero revision, return `ErrRestoreAfterUse`
  - second call returns `ErrRestoreAlreadyCalled`
- `Restore` scans all registered tables and selects those with at least one `PebbleIndex`.
- For each such table, choose one authoritative durable source:
  - if the primary index is a `PebbleIndex`, use it
  - otherwise use the first declared `PebbleIndex`

### Restore Process Per Table

- acquire the table in a write txn
- clear and rebuild all in-memory indexes for that table
- reopen Pebble-backed indexes as committed readers over existing Pebble data
- scan the authoritative Pebble source and decode each row
- reinsert objects into:
  - in-memory primary index, if primary is in-memory
  - all in-memory secondary indexes
  - in-memory revision index
- set table revision to the max restored revision
- leave graveyard and graveyard-revision empty

Because every Pebble row stores full object bytes, a secondary `PebbleIndex` can restore the table by itself.

## Watch Semantics

- Each `PebbleIndex` has one coarse index-level watch channel.
- Any committed mutation touching that Pebble index closes that channel.
- `GetWatch`, `ListWatch`, `PrefixWatch`, and `LowerBoundWatch` against a `PebbleIndex` all return that same channel.
- Existing in-memory indexes keep current fine-grained watches.
- `InsertWatch` continues to return the watch from the primary index implementation:
  - fine-grained if primary is in-memory
  - coarse if primary is Pebble-backed

## Test Plan

- Mixed-backend table with in-memory primary and Pebble secondary:
  - CRUD before and after restore
  - in-memory primary/secondary rebuild from Pebble
- Mixed-backend table with Pebble primary and in-memory secondary:
  - CRUD, restart, restore, revision preservation
- Table with multiple Pebble secondary indexes:
  - restore uses primary Pebble source if present, otherwise first declared Pebble index
- `IndexedBatch` write-txn visibility:
  - `Get`, `List`, `Prefix`, `LowerBound`, and `All` observe uncommitted Pebble mutations in the same txn
- Unique enforcement:
  - same Pebble key with different primary key fails
  - same Pebble key with same primary key updates in place
- Key-change reindexing:
  - changing a Pebble secondary key deletes old row and inserts new row
- Restore guards:
  - second `Restore()` call fails
  - `Restore()` after any live in-memory state exists fails
- Watch behavior:
  - Pebble index watch closes on any commit touching that index
  - in-memory watches remain unchanged
- Change iterator:
  - works for post-restore mutations
  - does not replay pre-restart history

## Assumptions and Defaults

- `PebbleIndex` must return exactly one key for every stored object.
- Full object payload is stored in every Pebble row so any Pebble index can be a restore source.
- Revision is persisted in Pebble rows and restored into the in-memory revision index.
- Graveyard, graveyard-revision, delete trackers, and old `Changes()` history are not durable across restart.
- Using Pebble's `IndexedBatch` is the default design because it better matches StateDB's write-txn read semantics than maintaining a separate overlay.
