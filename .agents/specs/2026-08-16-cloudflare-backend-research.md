---
title: "Cloudflare Backend Research (deferred)"
status: deferred
kind: notes
created: 2026-08-16T06:26:02+00:00
---

# Cloudflare Backend Research (deferred)

Findings from investigating a Cloudflare deployment target for Threadmark.
**Deferred** — scope was reduced to PostgreSQL and local single-node. Kept
because the facts were verified against vendor docs and upstream issues, and
re-deriving them is expensive. Nothing here is committed to.

Verified 2026-08-16. Several of these are actively moving and must be
re-verified before use.

## R2

- **No object versioning.** `GetBucketVersioning` and `PutBucketVersioning` are
  listed unimplemented in R2's S3 compatibility table; `ListObjectVersions` does
  not appear at all. Threadmark's boot check at `src/main.rs:44` and the
  version-pinned copy in `src/uploads.rs:263` cannot work on R2.
- **No presigned POST.** R2 supports presigned `GET`, `HEAD`, `PUT`, `DELETE`
  only. Docs state plainly that `POST` multipart form uploads via HTML forms are
  not supported. `src/object_store.rs:264` is unusable there, including its
  `content-length-range` size enforcement.
- **Conditional headers are supported.** `PutObject` honours `If-Match` /
  `If-None-Match`; `CopyObject` honours `x-amz-copy-source-if-match` and
  `x-amz-metadata-directive`. Enough to build write-once immutable keys and an
  ETag-pinned copy as a versioning substitute.
- Presigned URLs work only on the S3 API domain, not custom domains.
- Whether a signed `Content-Length` is enforced on presigned PUT is
  **unresolved**. Evidence is contradictory; `aws/aws-sdk-go-v2#1954` documents
  it not working as expected. Would need a live trace before being relied on.

## D1

- **No interactive transactions.** Operates in auto-commit. `batch()` is atomic
  but blind: statements are submitted together with no ability to read a result
  and branch mid-sequence.
- The REST `/query` endpoint "supports multiple statements, joined by
  semicolons, which will be executed as a batch", so a non-Worker client gets
  the same atomic-batch semantics as the binding.
- **Read-modify-write is expressible, but only as optimistic concurrency.** Read
  first, compute, then submit one batch whose race guard is a unique-constraint
  violation. Threadmark's schema already has the needed constraints:
  `UNIQUE (conversation_id, seq)`, `turns_one_active_per_conversation_idx`,
  `UNIQUE (tenant_id, owner_ref, client_id, idempotency_key)`.
  - Trap: `UPDATE ... WHERE next_seq = ?` matching zero rows is a *successful
    no-op*, not an error, so it would commit inserts without the seq bump. The
    CAS must ride on the unique INSERTs, never on the UPDATE's row count.
- **10 GB per database, cannot be raised.** For a single-database deployment
  that caps total conversation history permanently.
- Cloudflare explicitly endorses per-tenant sharding: D1 "is designed for
  horizontal scale out across multiple, smaller (10 GB) databases, such as
  per-user, per-tenant or per-entity databases." 50,000 databases per account on
  Workers Paid, 10 on Free.
- Static bindings cap around 5,000 per Worker script, below the 50,000-database
  limit, so per-tenant at scale requires the REST API. A container using the
  REST API needs only a database ID, so the binding cap is not a constraint
  there.
- Limits: 100 bound parameters per query, 100 columns per table, 2 MB per
  string/BLOB/row, 100 KB statement length.
- **No sqlx driver.** Would require a hand-written HTTP client and all SQL,
  losing pooling, compile-time checking, and `sqlx::migrate!`.

## Durable Objects

- A DO is one instance of a class, addressed by ID, owning a private SQLite
  database and processing its requests one at a time. `idFromName()` routes
  every request for a name to the same single global instance.
- Storage is colocated with the code, so `sql.exec()` is a local call.
- **Explicit SQL transactions are forbidden.** `sql.exec()` cannot run
  `BEGIN TRANSACTION` or `SAVEPOINT`; the runtime blocks them. Cloudflare
  directs callers to `ctx.storage.transaction()` / `transactionSync()`.
  Corroborated by Effect-TS `sql-sqlite-do` and MikroORM's DO guidance.
- **`workers-rs` has no typed path to a SQL transaction.** `worker::SqlStorage`
  exposes only `exec`, `exec_raw`, `database_size`.
  `worker::durable::Storage::transaction()` exists but yields a `Transaction`
  with only legacy KV methods and no `.sql()`. Whether it ambiently covers
  `sql.exec()` like the JS API was never resolved.
- **Implicit atomicity is the workaround.** Writes issued without an intervening
  `await` are automatically coalesced and committed atomically, all-or-nothing
  on failure. Input gates prevent interleaving; output gates hold outbound
  messages until writes commit. Cloudflare notes SQLite storage ops are
  synchronous and do not yield the event loop. A straight-line, `await`-free
  `sql.exec()` sequence therefore has the properties `BEGIN` +
  `pg_advisory_xact_lock` provides — but the constraint is compiler-invisible.
- Limits: 10 GB per object, 2 MB per row, 100 bound params, 100 columns, 30 s
  CPU per request (raisable to 5 min via `limits.cpu_ms`), 30-day PITR.
- Pricing: rows read/written match D1 rates; storage $0.20/GB-month against D1's
  $0.75; duration $12.50 per million GB-s, avoided while hibernating. For an
  append-heavy ledger, row writes dominate, so DO and D1 cost similarly.
- Cloudflare's storage-selection guidance recommends D1 for "read-heavy"
  applications and Durable Objects for "per-user or per-customer SQL state" —
  the latter matches Threadmark.
- Operationally there is no `psql`, no external console, no `pg_dump`. Data is
  reachable only through deployed code; backup is Cloudflare's PITR.

### Sharding, if ever revisited

- Must be **per owner**, `(tenant_id, owner_ref)`, not per conversation. Every
  table is owner-reachable, and cross-owner file references are impossible
  because `lock_turn_files` (`src/store.rs:272`) filters on
  `file.tenant_id AND file.owner_ref`.
- Per-conversation sharding splits atomic turn start, because `turn_starts` is
  owner-scoped and the same transaction may create the conversation it records.
- Owner sharding serializes *more* than the domain requires: Threadmark's
  invariants are conversation-scoped, so a principal's separate conversations
  would queue behind one object. Practical impact is likely small, since each
  write is a short local SQLite transaction and model inference happens outside
  Threadmark, but it was never measured.
- Unblocking per-conversation sharding would need (a) deterministic conversation
  IDs derived from the idempotency tuple for the create case, (b) narrowing
  turn-start idempotency to conversation scope, and (c) a solution for
  owner-scoped file references crossing shards — (c) being the hard part, since
  `ON DELETE RESTRICT` on `conversation_item_files` makes "no file deleted while
  referenced" a cross-shard invariant.

## Cloudflare Containers

- **Generally available since 2026-04-13**, previously public beta from June
  2025. GA brought higher limits, active-CPU pricing, Docker Hub images, SSH
  access, and binding access via hostnames.
- **All disk is ephemeral.** A sleeping instance restarts with a fresh
  filesystem from the image. Snapshots are "coming soon". FUSE-to-R2 is
  documented but warned about for performance. A container cannot hold a SQLite
  database durably.
- Bindings (D1, R2, KV, DO) are reachable only through **outbound handlers** —
  the container makes an HTTP request to a virtual hostname and a JS handler in
  the Worker resolves it against the binding. Not native access.
- **Arbitrary TCP egress works** when `enableInternet` is true, which is the
  default; only `enableInternet = false` restricts traffic to ports 80/443 and
  DNS. So a container can hold a pooled TCP connection to an external Postgres.
- Cold starts are 1–3 seconds, against roughly 5 ms for a Worker isolate.
- Autoscaling for stateless apps is not yet available; `getRandom` is the
  documented workaround.

### Implication if revisited

A container running the existing native binary against external Postgres and R2
needs no new database adapter at all — it is the current T1 topology on
Cloudflare compute. Its weakness is that Cloudflare starts containers near the
*request*, not near the database, so a multi-round-trip transaction can cross a
WAN unless both are pinned to one region. Pinning them makes it a conventional
regional app deployment whose advantage over a VPS is mostly vendor
consolidation.

## Other

- **sqlx does not support Turso or libSQL.** `launchbadge/sqlx#2674` open;
  `tursodatabase/turso#635` backlogged; `sqlx-turso` on crates.io is
  `0.1.0-alpha` against the Rust `turso` rewrite rather than libSQL.
- axum runs on `workers-rs` directly via standard `http` types, so the routing
  layer would have been shared rather than rewritten.
- A wasm32 target would have forced a workspace split, because Cargo resolves
  the dependency graph before features gate compilation, so `sqlx`,
  `aws-sdk-s3`, and tokio networking would still have had to resolve.
