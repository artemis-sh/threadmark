---
title: "Local Storage Backend"
status: implemented
kind: rfc
created: 2026-08-16T06:26:02+00:00
---

# RFC: Local Storage Backend

## Summary

Threadmark will support a second deployment shape: a single binary plus a
writable directory, backed by SQLite and local filesystem storage. The existing
PostgreSQL and S3-compatible deployment remains the flagship and is not changed
by this work in any user-visible way.

Two seams are introduced — a `Store` trait for the database and a `BlobStore`
trait for object storage — each with two implementations in the existing crate.
No workspace split, no new runtime, no new dependency on anything outside the
current stack.

## Motivation

Today the smallest possible Threadmark is a three-container `docker-compose.yml`
with PostgreSQL and MinIO. That is the right shape for a production self-host
and the wrong shape for everything else: evaluation, single-user deployments,
development, and small installations where running and backing up Postgres is
more operational weight than the workload justifies.

The coupling preventing this is structural rather than configuration. `src/store.rs`
is 1357 lines of `sqlx` calls against `Postgres` types, with transaction control,
`pg_advisory_xact_lock`, `SELECT ... FOR UPDATE`, `jsonb`, and
`unnest($2::text[])` interleaved directly with domain rules. There is no seam at
which another engine could be introduced.

The rules interleaved with that SQL are the part that must not diverge. Turn-start
idempotency, the single-active-turn invariant, sequence allocation, and file
reference lifetime are correctness-critical and specified in adjacent RFCs. Two
divergent copies of those rules would be a worse outcome than one backend, so the
seam has to be drawn where the rules stay in one place.

## Goals

- Add a deployment requiring only a binary and a writable directory.
- Keep PostgreSQL first-class and unchanged, with no constraint imposed on it by
  the new backend's existence.
- Express every storage-invariant-bearing rule once, so the two backends cannot
  silently diverge.
- Make each backend's capabilities explicit, so the local backend can decline an
  operation rather than forcing the contract to shrink.
- Keep the wire API identical across both.

## Non-goals

- Any Cloudflare target: Workers, Durable Objects, D1, R2, or Containers. See
  `2026-08-16-cloudflare-backend-research.md` for prior investigation, which is
  deferred and not committed to.
- Turso, libSQL, or any remote SQLite. The local backend is a file on disk.
- Multi-process or multi-replica operation of the local backend.
- Cross-backend live migration or replication.
- Changing the authorization model, capability token format, or conversation
  data model semantics.
- Resumable or chunked uploads, out of scope from the direct-upload RFC.

## Deployment Targets

| | T1 Production self-host | T2 Single-node self-host |
| --- | --- | --- |
| Database | PostgreSQL | SQLite file |
| Blobs | S3 / MinIO | Local filesystem |
| Concurrency | database | single process |
| Direct browser upload | yes, presigned POST | no |
| Tier rank | 1, governing | 2 |
| Status | flagship, exists today | new |

### Tier conformance rule

> **Constraints flow downward only.** T1 defines the contract. T2 conforms to
> T1. T2 never narrows, weakens, or reshapes T1.

1. **T1 never loses a capability because T2 lacks it.** Presigned POST and
   bucket versioning stay exactly as they are on S3 and MinIO.
2. **T1 never accepts a limit originating in T2.** PostgreSQL deployments are
   bounded by PostgreSQL.
3. **T1's implementation is not reshaped for T2's benefit.** The Postgres
   implementation keeps `pg_advisory_xact_lock`, `SELECT ... FOR UPDATE`,
   interactive transactions, and `unnest($1::text[])`.
4. **Where T2 cannot meet the contract it refuses the operation and says so**,
   via the capability matrix, rather than shrinking the contract.
5. **Domain rules are tier-invariant.** Only capabilities vary.

## Verified Platform Constraints

### sqlx SQLite defaults are unsafe for write transactions

`Pool::begin()` issues a bare `BEGIN`, which SQLite treats as `DEFERRED`. A
deferred transaction that starts as a reader and later upgrades to a writer can
fail with `SQLITE_BUSY_SNAPSHOT`, and SQLite deliberately does **not** invoke the
busy handler for that contention shape, so a configured `busy_timeout` never
runs. Every Threadmark write transaction reads before it writes, so every one is
exposed.

T2 must begin all write transactions with `BEGIN IMMEDIATE`, taking the write
lock up front. `sqlx` 0.8 provides `Pool::begin_with` and
`Connection::begin_with` for this. `SqliteConnectOptions` must additionally set
`journal_mode = WAL`, `foreign_keys = true`, a `busy_timeout`, and a
`synchronous` level exposed as an operator durability choice.

### SQLite is a single-writer database

One writer at a time. T2 is a single-process deployment and must not be run as
multiple replicas against one file. This is a documentation and configuration
requirement, not something the code can enforce.

## Proposal

### Where the seams go

Both backends are native, both use `sqlx`, both run under tokio and axum, so no
workspace split is needed.

> **Implementation note.** This section originally proposed a `Store` trait with
> a separate implementation per engine, accepting that "a storage-touching
> feature is written twice". That proved unnecessary. SQLite treats `$N` as a
> numbered parameter with the same positional binding semantics PostgreSQL uses,
> so almost every one of the 74 queries is byte-identical on both engines and the
> store is written **once**, generic over the engine. `tests/sqlite_dialect.rs`
> pins that assumption, along with timestamp ordering, `RETURNING`, `ON CONFLICT`,
> and unique-violation reporting.

```
src/db.rs               Backend trait: the dialect differences, and only those
src/store.rs            SqlStore<DB>, generic; Stores enum dispatching to it
src/files.rs            file methods on SqlStore<DB>
src/uploads.rs          upload-session methods on SqlStore<DB>
src/blob/mod.rs         ObjectStore enum
src/blob/s3.rs          S3Store, moved not rewritten
src/blob/fs.rs          FsStore, new
```

`src/api.rs`, `src/auth.rs`, `src/capability.rs`, `src/model.rs`, and
`src/error.rs` are backend-agnostic and are untouched apart from the state type.

### What actually differs between the engines

`Backend` carries the whole of it:

| | PostgreSQL | SQLite |
| --- | --- | --- |
| Write transaction | `BEGIN` | `BEGIN IMMEDIATE` |
| Read-for-update | ` FOR UPDATE` | none needed |
| Shared row lock | ` FOR KEY SHARE` | none needed |
| Same-key serialization | `pg_advisory_xact_lock` | none needed |
| Unique violation | index name via `constraint()` | column list in the message |
| `rows_affected` | per-engine method, no shared trait exists | same |

Three constructs had no portable form and moved out of SQL into Rust, which also
made them dialect-free:

- `now()` and interval arithmetic became bound timestamps. This removes any
  dependence on the database clock and keeps the stored format identical to what
  sqlx encodes.
- `unnest($n::text[])` became rendered placeholders, since SQLite has no arrays.
- The metadata merge replaced `jsonb || jsonb`. SQLite's nearest equivalent,
  `json_patch`, deletes keys whose value is null instead of storing a JSON null,
  so merging in Rust keeps the observable behaviour identical.

| Backend | Atomicity and mutual exclusion |
| --- | --- |
| Postgres | `BEGIN` + `pg_advisory_xact_lock` + `SELECT ... FOR UPDATE`, as today |
| SQLite | `begin_with("BEGIN IMMEDIATE")`; the write lock replaces the advisory lock |

Dispatch to the configured engine is a static `match` in a `Stores` enum
generated by a macro, so no store method is virtual and each backend keeps its
own monomorphized queries. Adding a store method means adding one line to that
macro, and forgetting to is a compile error at the call site.

### The `BlobStore` trait

```
trait BlobStore {
    async fn put(&self, key: &str, bytes: Bytes, content_type: &str) -> ApiResult<()>;
    async fn get(&self, key: &str) -> ApiResult<Bytes>;
    async fn get_stream(&self, key: &str) -> ApiResult<ByteStream>;
    async fn head(&self, key: &str) -> ApiResult<Option<ObjectHead>>;
    async fn delete(&self, key: &str) -> ApiResult<()>;

    fn capabilities(&self) -> BlobCapabilities;
    async fn presigned_get(&self, ...) -> ApiResult<Option<String>>;
    fn presigned_post(&self, ...) -> ApiResult<Option<PresignedPost>>;
}
```

`BlobCapabilities` reports `direct_upload` and `presigned_download`. The
filesystem implementation returns false for both.

### What does not change

Worth stating explicitly, because an earlier and much larger draft of this RFC
proposed changing all of it in service of a Cloudflare target that is no longer
in scope:

- **Object versioning stays.** S3 and MinIO support it, the direct-upload RFC
  depends on it, and T2 does not need it because T2 declines direct upload. The
  version-pinned copy in `src/uploads.rs` is untouched.
- **Presigned POST stays.** Including its `content-length-range` policy
  condition, which is the strongest upload-size control available and is not
  given up.
- **`unnest($1::text[])` stays** at `src/store.rs:270`. SQLite's default
  `SQLITE_MAX_VARIABLE_NUMBER` is 32,766, far above anything Threadmark
  generates, so the SQLite implementation can use a plain `IN` list without
  chunking.
- **No payload size limit is introduced.** Neither engine needs one.

The single change to T1 behaviour is that the versioning boot check moves.

### Moving the versioning check

`src/main.rs:44` currently fails startup unless the bucket reports versioning
`Enabled`. That is a global check on a property only the S3 implementation has,
and it would block the filesystem backend from ever booting.

It moves into the S3 implementation and becomes conditional on
`direct_upload_enabled`, which is the only feature that requires it. A T1
deployment with direct upload enabled sees identical behaviour, including the
same startup failure with the same message. A T1 deployment with direct upload
disabled no longer needs a versioned bucket, which is a small liberalization
rather than a loss. The filesystem implementation has no such check.

### Data model portability

One logical schema, two migration sets, both run by `sqlx::migrate!` from
separate directories.

| Postgres | SQLite |
| --- | --- |
| `jsonb` | `text` with a `json_valid` check, `json1` for access |
| `timestamptz`, `now()` | `integer` epoch millis, `unixepoch()` |
| `bytea` + `octet_length(x) = 32` | `blob` + `length(x) = 32` |
| `smallint` | `integer` |
| `$1` placeholders | `?` placeholders |
| partial unique indexes | supported natively, unchanged |
| composite FK `(turn_id, conversation_id)` | supported natively, unchanged |

Three migrations need hand-written equivalents rather than mechanical
translation:

- `0005` backfills `conversation_item_files` using `jsonb_path_query` and
  `CROSS JOIN LATERAL`. New SQLite deployments start empty, so the backfill is
  omitted from the SQLite set. It is retained for Postgres.
- `0006` uses a `DO $$` block as a pre-flight assertion, omitted for the same
  reason.
- `0006` uses `ON DELETE SET NULL (turn_id)`, the PG15 column-list form. SQLite
  has no equivalent and needs an `AFTER DELETE` trigger.

### Filesystem blob layout

```
$BLOB_DIR/<tenant_id>/<file_id>
```

Both components are server-generated. No path component is ever derived from a
client-supplied string. Writes go to a temporary file in the same directory
followed by an atomic `rename`, so a partially written file is never visible
under its final name.

Downloads are served by the existing streaming proxy. There is no presigning, so
`presigned_get` returns `None` and the API returns a proxy URL, which the
download path already supports.

### Capability matrix

| Capability | T1 Postgres + S3/MinIO | T2 SQLite + FS |
| --- | --- | --- |
| Direct browser upload | yes, presigned POST | no |
| Upload-time size enforcement | yes, `content-length-range` | n/a |
| Finalize-time size enforcement | yes | yes |
| Server-mediated multipart upload | yes | yes |
| Presigned download | yes | no, proxied |
| Streaming download proxy | yes | yes |
| Object versioning | yes, if bucket enabled | not applicable |
| Concurrent writers | yes | single process |

T2 declines direct upload rather than reimplementing presigning. The
server-mediated endpoint at `POST /v1/files` is a supported path under the
direct-upload RFC and covers T2 completely. If T2 later needs direct upload,
Threadmark would serve HMAC-signed upload URLs from its own origin; that is a
separate proposal.

### Configuration

`STORAGE_BACKEND` selects `postgres` | `sqlite`. `BLOB_BACKEND` selects
`s3` | `filesystem`. Invalid combinations are rejected at startup with a
specific message, as `Config::from_env` already does elsewhere.

New for T2: `SQLITE_PATH`, `BLOB_DIR`, `SQLITE_SYNCHRONOUS`,
`SQLITE_BUSY_TIMEOUT_MS`. Existing Postgres and S3 settings are unchanged, and a
T1 deployment's environment continues to work with no edits.

## Security Properties

Preserved unchanged:

- A browser never receives write authority over a final object key.
- No row in `files` exists until bytes are complete, immutable, and validated
  for exact size, size limit, content type, and session marker.
- Pending uploads cannot be read, downloaded, or referenced by items.
- Ownership is enforced on every file reference.
- Upload-time size enforcement via `content-length-range` on S3 and MinIO.

New, specific to the filesystem backend:

- Path construction must reject traversal and must never derive a path component
  from a client-controlled string. Keys come from server-generated `tenant_id`
  and `file_id` only.
- Directory permissions and the process umask become part of the security
  posture and must be documented. The blob directory should not be world
  readable.
- Because there is no presigned download, every read is authorized by Threadmark
  itself. This is stronger than T1's presigned path, not weaker.

## Rollout

1. **Move the versioning check** into the S3 implementation, conditional on
   `direct_upload_enabled`. Ship on the current stack. Tested with direct upload
   both enabled and disabled.
2. **Introduce the two traits**, with the existing Postgres and S3 code moved
   behind them unchanged. No behaviour change. This is the largest and riskiest
   step and is gated on a full pass of the test suite and both smoke scripts.
3. **SQLite implementation.** `BEGIN IMMEDIATE`, WAL, the SQLite migration set.
4. **Filesystem implementation.** Atomic rename, capability flags off for direct
   upload and presigned download.
5. **Packaging.** A single-binary quickstart in the README and a `docker-compose`
   profile with no Postgres or MinIO.

Step 1 is the only step touching T1, and it is a liberalization. Step 2 is a pure
refactor. Steps 3 through 5 add T2 without reshaping T1.

## Testing

- The existing conformance suite runs unmodified against both backends. A
  backend that cannot pass it is not shipped. This is the primary defence
  against divergence and matters more than any per-backend test.
- Property tests for invariants both mechanisms must uphold: sequence
  contiguity, single active turn, turn-start idempotency under concurrent
  identical requests, and no file deleted while referenced.
- Concurrency tests per backend against the actual mechanism: advisory lock
  contention on T1, `BEGIN IMMEDIATE` contention and `SQLITE_BUSY` handling on
  T2. The SQLite test must specifically cover the read-then-write upgrade that
  `BEGIN IMMEDIATE` exists to prevent.
- A crash-injection test for upload finalize between the object copy and the
  database commit, per backend.
- `scripts/media-smoke.sh` and `scripts/atomic-turn-smoke.sh` gain a backend
  parameter and run in CI against both.
- Filesystem path traversal tests using hostile filenames and MIME types.
- **Tier regression test:** assert T1 still advertises direct upload and
  presigned download on S3, so a future change cannot quietly level T1 down to
  T2's capability set.

## Drawbacks

- ~~**Two implementations is a permanent tax.**~~ Did not materialize. The store
  is written once and generic over the engine, so a storage-touching feature is
  written once. The residual cost is the bound list, which is declared per impl
  block and repeated in three modules, and the dispatch macro entry per method.
- **Step 2 was high-risk and delivered nothing visible**, as predicted: 1357
  lines of interleaved domain and SQL logic rewritten with no behaviour change.
  Both smoke suites passing unchanged on PostgreSQL was the gate.
- **T2 is single-process.** No horizontal scaling, and misconfiguration as
  multiple replicas against one file will corrupt data. Documentation and a
  startup lock file are the only defences.
- **T2 has no direct upload**, so file bytes flow through the Threadmark process.
  This is precisely the memory pressure the direct-upload RFC set out to remove,
  reintroduced for the smallest deployment. Acceptable because that deployment is
  the one least likely to face concurrent large uploads, but it is a real
  regression relative to T1.
- **The abstraction may leak.** A coarse trait contains dialect differences but
  duplicates logic. If the two drift, the conformance suite is the only thing
  that catches it.

## Alternatives

### Keep one backend and require Postgres everywhere

Rejected. This is the status quo, and it excludes the deployments this RFC
exists to serve. Postgres is the right dependency for a production install and
disproportionate for a single-user one.

### Use sqlx's `Any` driver instead of a trait

Rejected. `Any` erases the dialect differences that actually matter here —
placeholder syntax, JSON types, lock statements, `RETURNING` behaviour — and
provides no way to express `pg_advisory_xact_lock` on one backend and
`BEGIN IMMEDIATE` on the other. It would push those differences into runtime
string manipulation rather than typed code.

### Expose a fine-grained trait with a transaction handle

Rejected. It would leak dialect differences into shared code, which is the thing
the seam exists to prevent, and it would make the two mutual-exclusion
mechanisms the caller's problem.

### Split into a workspace with separate crates

Rejected as premature. The workspace split was only forced by a wasm32 target,
which is now out of scope. Two native implementations behind traits in one crate
is simpler and can be split later if a reason appears.

### Add Turso or libSQL

Rejected. `sqlx` supports neither: `launchbadge/sqlx#2674` is open,
`tursodatabase/turso#635` is backlogged, and `sqlx-turso` is `0.1.0-alpha`
against the Rust `turso` rewrite rather than libSQL. T2's goal is a binary plus a
directory, which a local file satisfies. Remote replicated SQLite is a different
deployment shape served by T1.

### Cloudflare backends

Deferred. See `2026-08-16-cloudflare-backend-research.md`. The investigation
found no blocking defect, but the work is large — a workspace split, a wasm
runtime, a third storage implementation, and an unresolved question about
whether `workers-rs` can express a multi-statement SQL transaction at all. Out of
scope until the local backend is delivered.

## Unresolved Questions

1. ~~Should `SQLITE_SYNCHRONOUS` default to `FULL` or `NORMAL`?~~ Defaults to
   `FULL`, the safe choice for a durable ledger, and is configurable.
2. Should T2 take an advisory lock file at startup to detect a second process
   against the same database, or is documentation sufficient?
3. The filesystem backend fsyncs the file before renaming it, so a crash cannot
   publish a visible but empty object. It does **not** fsync the directory
   afterwards, so the rename itself could in principle be lost on power failure.
   Still open: whether that matters enough to pay the cost per upload.
4. Should the deletion outbox sweeper interval differ on T2, given that a
   single-node deployment may be idle for long periods and the 60-second timer
   at `src/main.rs:53` will keep the process awake?
5. Is there an export/import path between T1 and T2, and does it belong in this
   RFC or its own?
