---
title: "Atomic Turn Start"
status: final
kind: rfc
created: 2026-08-14T06:26:25+00:00
---

# RFC: Atomic Turn Start

## Summary

Threadmark will expose one idempotent operation that starts a turn by creating a
conversation when requested, creating its pending turn, and appending the user
items that triggered the turn in one PostgreSQL transaction. The operation
enforces the existing single-active-turn invariant and returns stable
conversation, turn, item, and sequence identifiers.

Parley invokes the agent only after this operation commits. Agent execution is
not part of the transaction and remains outside Threadmark's ledger role.

## Motivation

Parley currently creates a conversation when needed, creates a turn, appends its
user input, and launches the agent through separate calls. A failure after turn
creation but before item append leaves a pending turn with no triggering input.
Because a conversation permits only one pending or streaming turn, that partial
write can block later work. Creating a conversation through a separate call has
the same partial-failure and uncertain-retry problem.

Threadmark already has transactional item append and a partial unique index that
enforces one active turn, but existing idempotency keys are scoped to an existing
conversation. They cannot make optional conversation creation and turn start one
idempotent operation. Current turn and append retries also do not reject changed
request content. A first-class operation is needed rather than client-side
composition of the existing endpoints.

## Goals

- Create an optional conversation, one pending turn, and one or more triggering
  user items atomically.
- Enforce at most one `pending` or `streaming` turn per conversation under
  concurrency.
- Let an exact retry return the originally committed IDs without duplicating any
  resource.
- Reject reuse of an idempotency key for behaviorally different input.
- Preserve gap-free item sequence allocation and existing ownership checks.
- Give Parley all identifiers needed to launch the agent after commit.

## Non-goals

- Launching, scheduling, or proxying agent execution.
- Atomically coordinating a PostgreSQL commit with an external agent runtime.
- Replacing the existing conversation, turn, or append endpoints.
- Repairing or expiring active turns abandoned after a successful commit.
- General workflow orchestration or event delivery.

## Proposal

### Endpoint

Add an owner-session endpoint:

```text
POST /v1/turn-starts
```

The request is:

```json
{
  "idempotency_key": "parley-request-123",
  "conversation_id": "conv_01...",
  "agent_ref": "research-agent/prod",
  "items": [
    {
      "type": "message",
      "role": "user",
      "content": [{"type": "input_text", "text": "Plan a weekend in Lisbon"}]
    }
  ]
}
```

To create a conversation, replace `conversation_id` with `conversation`:

```json
{
  "idempotency_key": "parley-request-124",
  "conversation": {
    "title": "Trip planning",
    "metadata": {"client": "parley"}
  },
  "agent_ref": "research-agent/prod",
  "items": [
    {
      "type": "message",
      "role": "user",
      "content": [{"type": "input_text", "text": "Plan a weekend in Lisbon"}]
    }
  ]
}
```

Exactly one of `conversation_id` and `conversation` is required. An explicit
`conversation` object distinguishes creation from an accidentally omitted ID.
Its defaults and metadata-object validation match `POST /v1/conversations`.

`idempotency_key`, `agent_ref`, and conversation title are trimmed before
validation and storage. Each must contain 1 to 200 Unicode scalar values after
trimming. This intentionally tightens inconsistent byte-length and trimming
behavior in the current endpoints; shared normalized request helpers will be
used by `POST /v1/conversations` as a prerequisite to launching this endpoint so
title behavior is consistent. `items` must contain 1 to 100 JSON objects and
follows the existing 64 MiB HTTP body limit.

The endpoint uses a raw-body, duplicate-detecting JSON deserializer before
constructing typed request values; Axum's ordinary `Json<T>` extractor is not
used. Duplicate keys at any depth, malformed JSON, and numbers outside the
interoperable canonicalization domain return `400` with `error.code` set to
`invalid_request` in the standard envelope. Because PostgreSQL `text` and `jsonb`
cannot store U+0000, validation recursively rejects it in all JSON keys, string
values, and normalized text fields with the same error. The extractor retains
the existing 64 MiB bound.

All items are stored with ledger provenance `source=user` and the new turn's ID.
Payloads remain opaque in this owner-session endpoint: `source=user` does not
assert that an embedded protocol role or item type is user-only. Parley is
responsible for submitting valid Open Responses input. `threadmark://files/...`
ownership validation follows the coordinated lock protocol below.

On first execution, return `201 Created`:

```json
{
  "conversation_id": "conv_01...",
  "turn_id": "turn_01...",
  "item_ids": ["item_01..."],
  "first_seq": 1,
  "last_seq": 1,
  "replayed": false
}
```

An exact retry returns `200 OK`, the same identifiers and sequence range, and
`replayed: true`. The status difference is advisory; clients use the response
body as the source of truth. Item IDs preserve request order.

### Operation idempotency

Add a durable operation table independent of the conversation lifecycle:

```sql
CREATE TABLE turn_starts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_version smallint NOT NULL,
    request_digest bytea NOT NULL,
    conversation_id text NOT NULL,
    turn_id text NOT NULL,
    first_seq bigint NOT NULL,
    last_seq bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key),
    UNIQUE (turn_id),
    CHECK (request_version > 0),
    CHECK (first_seq > 0 AND last_seq >= first_seq),
    CHECK (octet_length(request_digest) = 32)
);

CREATE TABLE turn_start_items (
    turn_start_id text NOT NULL REFERENCES turn_starts(id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    item_id text NOT NULL,
    seq bigint NOT NULL CHECK (seq > 0),
    PRIMARY KEY (turn_start_id, ordinal),
    UNIQUE (turn_start_id, item_id),
    UNIQUE (turn_start_id, seq)
);
```

The insertion transaction creates exactly `last_seq - first_seq + 1` child rows
whose zero-based ordinals and sequences are contiguous. Replay treats a missing,
extra, duplicate, or noncontiguous child set as a deleted/corrupt result and
fails closed. Keeping item IDs in typed child rows gives the result schema
database-enforced string, ordering, and uniqueness properties.

`client_id` is the verified authorization claim identifying the owner-session
client. Trusted-header development mode supplies a fixed reserved client ID.
This prevents unrelated integrations acting for the same owner from sharing an
idempotency namespace. The same scope is used for the advisory lock.

The result IDs intentionally have no foreign keys. Conversation deletion must
not erase the idempotency record and permit the same request to create a second
conversation. An exact retry whose result resources have since been deleted
returns `409 Conflict` with code `idempotency_result_deleted`; it does not
re-execute. Turn-start records are not purged in this feature. A future retention
RFC must publish a minimum retry period and retain a compact actor/client/key
tombstone after result removal so an expired key can never recreate work.

`request_version=1` uses SHA-256 over RFC 8785 JSON Canonicalization Scheme bytes
for an explicitly tagged envelope containing:

- `operation: "turn_start"` and `version: 1`;
- existing `conversation_id`, or the effective trimmed/defaulted title and
  metadata for a new conversation;
- trimmed `agent_ref`;
- the complete ordered item array.

The exact version-1 preimage before RFC 8785 canonicalization is one of:

```json
{"operation":"turn_start","version":1,"mode":"existing","conversation_id":"conv_01...","agent_ref":"research-agent/prod","items":[]}
```

```json
{"operation":"turn_start","version":1,"mode":"create","conversation":{"title":"New conversation","metadata":{}},"agent_ref":"research-agent/prod","items":[]}
```

Every displayed member is required in the preimage. Effective defaults are
materialized, and no additional or null members are permitted. The examples use
empty `items` only to show the envelope; request validation still requires at
least one item.

The JSON parser rejects duplicate object keys, non-finite numbers, and numbers
that RFC 8785 cannot represent losslessly. For each JSON number token, parse to
IEEE 754 binary64 using round-to-nearest, ties-to-even, serialize it with RFC
8785's shortest round-tripping form, and compare the exact mathematical decimal
value of that form with the original token. Reject overflow, underflow to a
different value, rounded integers or fractions, and lexical negative zero;
positive zero is accepted. Thus `1`, `1.0`, and `1e0` are equivalent, while an
integer above the binary64 safe range is accepted only when conversion preserves
its exact value. Canonicalization and persistence use the validated effective
JSON value. The implementation hashes the
validated effective request, so omitted defaults and their explicit values are
equivalent. The idempotency key and authenticated actor/client are excluded
because they select the operation row. A matching key with a different digest
returns `409 Conflict` with code `idempotency_key_reused` and discloses no prior
result.

The initial version stores the result in the operation and child rows rather than
reconstructing it from mutable transcript tables. `request_version` selects the immutable
normalization and digest implementation used to compare a retry. Threadmark must
retain each implementation for the lifetime of rows using it. Incompatible
request changes use a new request version or API version; they never reinterpret
an old key.

### Transaction and concurrency

All database mutations occur in one transaction. Before the transaction,
Threadmark performs version-independent raw JSON parsing, body-size checks,
top-level discrimination of existing versus new conversation mode, and
idempotency-key trimming and scalar-count validation. It checks `turn:create`
and `transcript:append`, plus `conversation:create` for new-conversation mode,
before operation-row access. The normalized key is used consistently for the
advisory lock, row lookup, uniqueness constraint, and storage. Other
version-specific normalization follows row lookup:

1. Begin a transaction and acquire a transaction-scoped PostgreSQL advisory lock
   derived from `(tenant_id, owner_ref, client_id, idempotency_key)`. This
   serializes an absent-row race without requiring a partially populated
   operation row. Derivation hashes the domain separator
   `threadmark:turn-start-lock:v1\0` followed by each UTF-8 field in tuple order,
   framed as an unsigned 32-bit big-endian byte length and then field bytes. The
   first eight SHA-256 digest bytes are interpreted as a signed big-endian
   two's-complement `i64` for `pg_advisory_xact_lock`.
2. Read the operation row. If it exists, normalize and hash the retained raw
   request with that row's `request_version`, compare the digest, and return its
   stored result through the replay procedure or the relevant conflict. If no row
   exists, normalize, validate, extract referenced file IDs, and hash using the
   current request version. Mutable file validation is not performed on replay.
   Before returning `idempotency_key_reused` for a digest mismatch in existing-
   conversation mode, verify ownership of the submitted conversation and return
   `404` if it is missing or unowned. This prevents key-existence disclosure.
3. For a new conversation, insert it inside the transaction. For an existing
   conversation, select the actor-owned row `FOR UPDATE`; a missing or unowned ID
   returns `404`.
4. Check for a `pending` or `streaming` turn. If one exists, return `409 Conflict`
   with code `active_turn_exists` and make no changes.
5. Insert the new turn with status `pending`. Atomic-start turns use `NULL` for
   `turns.idempotency_key`; retries are represented exclusively by `turn_starts`.
   Migrate the column to nullable and replace its unique constraint with a partial
   unique index on `(conversation_id, idempotency_key) WHERE idempotency_key IS
   NOT NULL`. Ordinary turn creation no longer relies on `INSERT ... ON CONFLICT`:
   it begins a transaction, locks the actor-owned conversation, looks up and
   returns an existing keyed turn first, then checks for another active turn and
   inserts only when the key is absent. This makes replay of a terminal keyed turn
   succeed even while a newer turn is active and avoids key-domain collisions.
   Before returning an existing keyed turn, require its `agent_ref` to equal the
   normalized request; a difference is an altered-idempotency `409` that discloses
   no prior result. It must also equal any token-bound `agent_ref`, with mismatch
   returning `404`.
   The existing partial unique index
   on active turns remains the final race-safety constraint. A violation of that
   named index maps to `active_turn_exists`, not an internal error.
6. Resolve the union of pre-turn snapshot file IDs and newly referenced file IDs,
   then lock every actor-owned file row in ascending file-ID order using
   `FOR KEY SHARE`. A missing or unowned file returns `404`. This endpoint
   depends on file deletion
   using the same row lock before checking references and removing the database
   row, as required by the service-authorization RFC. File deletion commits a
   deletion outbox/tombstone before retryable S3 cleanup; it never removes the S3
   object before its database decision commits.
7. Create the new turn's authoritative immutable pre-turn allowed-input-file
   snapshot required
   for delegated execution. It contains only files referenced by conversation
   items committed before this triggering batch. Triggering-batch files are not
   pre-turn history and require issuer-signed `allowed_file_ids` in the delegated
   token. This matches the service-authorization RFC; unrelated owner files are
   never included. Snapshot creation and ordinary turn creation use the same
   helper and schema. The schema has one parent `turn_file_snapshots` row with
   `turn_id REFERENCES turns(id) ON DELETE CASCADE UNIQUE`, plus child rows with
   `snapshot_id REFERENCES turn_file_snapshots(id) ON DELETE CASCADE`,
   `file_id REFERENCES files(id) ON DELETE RESTRICT` with stable constraint name
   `turn_file_snapshot_files_file_id_fkey`, and primary key
   `(snapshot_id, file_id)`. The parent exists even for an empty set and carries
   `authoritative=true`; existing turns receive no parent during migration and
   cannot receive delegated access. This distinguishes an authoritative empty
   set from unavailable legacy state.
8. Allocate a contiguous sequence range by computing `new_next_seq = first_seq +
   item_count` with checked arithmetic. If `new_next_seq` is not representable as
   PostgreSQL `bigint`, return `409` with code
   `sequence_space_exhausted`. Add `CHECK (next_seq >= 1)` to `conversations`.
   Item sequences end at `new_next_seq - 1`, so the greatest allocatable item
   sequence is `bigint` maximum minus one. Insert each user item with the new turn
   ID and update `next_seq` and `updated_at`.
9. Insert the completed `turn_starts` and `turn_start_items` rows and commit.

The conversation row lock serializes this operation with ordinary item append,
truncate, and other turn starts. The advisory lock serializes only retries of the
same actor-scoped operation key. No operation row is committed unless every
resource was inserted successfully.

Every turn-creation path, including the existing endpoint, uses one transaction,
locks the actor-owned conversation first, resolves idempotency, checks active
state, inserts the pending turn, and creates its pre-turn snapshot before commit.
Transaction commit is the turn/snapshot linearization point. This prevents a
concurrent append from committing before the turn while being omitted from its
snapshot.

No S3 operation occurs in the transaction because starting a turn only
references already-uploaded files. The shared file-row lock serializes reference
creation with deletion; an unlocked existence recheck is not sufficient.

To avoid repeatedly scanning opaque JSON, add a normalized
`conversation_item_files(item_id, file_id)` relation populated by every item
insertion path in the same transaction. It uses
`item_id REFERENCES conversation_items(id) ON DELETE CASCADE`,
`file_id REFERENCES files(id) ON DELETE RESTRICT` with stable constraint name
`conversation_item_files_file_id_fkey`, and a unique composite key.
Migration backfills references using application parsing and verifies the result
before file deletion or delegated access is enabled. Existing reference rows
prevent file deletion through the foreign key. Turn start explicitly locks the
union of historical snapshot and triggering file IDs before inserting either
child relation, avoiding implicit foreign-key locks in a different order. The
pre-turn snapshot is built
with a set-based `INSERT ... SELECT` from this relation and does not lock or scan
every historical file. No arbitrary snapshot cardinality limit is introduced;
its work is proportional to the owner's files already referenced by that
conversation and is covered by query-duration metrics.

Every item insertion path deduplicates referenced file IDs, locks actor-owned
file rows in ascending ID order with `FOR KEY SHARE` after taking the conversation
lock, and inserts item-file rows before commit. File deletion locks its file row
with `FOR UPDATE` before checking both normalized item references and snapshot
child references, then deletes the database row. A named foreign-key violation
from either relation maps to the established referenced-file conflict. This common
conversation-then-file ordering prevents reference/deletion races and avoids
inconsistent lock ordering between append paths.

Add `UNIQUE (id, conversation_id)` to `turns` and replace the item turn foreign
key with `(turn_id, conversation_id) REFERENCES turns(id, conversation_id)`. The
foreign key uses `ON DELETE SET NULL (turn_id)`, preserving `conversation_id` and
the current behavior when a turn is removed while preventing cross-conversation
item associations.

### Replay resolution

An idempotent replay is resolved before mutable file and active-turn checks. It
therefore returns the original result after the turn has moved to a terminal
state and when a newer turn is active.

Replay resolution occurs in the same transaction as the operation-row read. It
locks the referenced actor-owned conversation row, then verifies in one query
that the turn belongs to it and that every stored child item ID exists at the stored
ordered contiguous sequence range with that turn ID. Truncation and conversation
deletion must acquire a conflicting conversation lock. A successful replay is a
linearizable observation at the locked verification point. It provides no
post-response resource-liveness guarantee: resources may be deleted after commit
and before network delivery, and clients must tolerate a later read returning
`404`.

If the conversation cannot be locked because it was deleted, or any relationship
check fails, the actor/client-scoped operation row is authoritative evidence that
the caller owns the deleted result and Threadmark returns
`idempotency_result_deleted`. It does not fall back to `404` or re-execute.

### Authorization

The endpoint is owner-session only. Delegated-agent tokens cannot start turns.
It depends on the service-authorization RFC extending `AuthContext` with verified
`client_id` and token kind; trusted-header mode supplies a reserved development
client ID `threadmark:trusted-headers`, and JWT validation rejects that reserved
value. The extension also retains the optional resource-bound `agent_ref` claim.
The handler rejects non-owner token kinds and rejects a normalized request
`agent_ref` that differs from a supplied claim with `404` before operation-row
lookup.
It requires:

- `turn:create` and `transcript:append` for every request;
- `conversation:create` when `conversation` is supplied;
- actor ownership of an existing `conversation_id`.

This RFC amends the service-authorization permission mapping to add
`POST /v1/turn-starts` as owner-session-only with `turn:create` and
`transcript:append`, plus conditional `conversation:create`. Route-permission
table tests cover both request modes.

Permission checks occur before accessing an existing idempotency result. A token
that no longer has all permissions required by its submitted request cannot use
an old key to retrieve IDs. Existing resource ownership is also verified before
returning a live replay; the actor/client-scoped operation row authorizes the
deleted-result conflict. Authorization failures do not disclose whether a key
exists.

### Error contract

The named conflict values in this RFC are the public `error.code` in Threadmark's
existing JSON error envelope, not human-readable messages. The shared error model
gains typed, code-bearing conflicts for `active_turn_exists`,
`idempotency_key_reused`, `idempotency_result_deleted`, and
`sequence_space_exhausted`. PostgreSQL errors are classified by exact constraint
name, including `turns_one_active_per_conversation_idx`; implementations must not
parse database messages.

### Agent launch and recovery

Parley launches the agent only after receiving a successful response. If Parley
loses the response, it retries the exact request and receives the same IDs.

A process crash after commit but before launch can still leave a valid pending
turn. Atomic ledger creation cannot eliminate this distributed-systems window.
Parley must persist its idempotency key and retry/reconcile pending launches.
Guaranteed eventual dispatch would require a separate durable outbox or event
delivery design and is not implied by this endpoint.

### Existing endpoints

The existing endpoints remain available for workflows that do not start an agent
turn. They are not called internally as independent transactions; validation and
insertion logic should be reused through transaction-aware store functions.

This feature does not depend on strengthening their current idempotency behavior,
but follow-up work should make altered retries of turn creation and append return
`409` consistently with this operation.

### Deployment

Threadmark is pre-release, so the schema migrations and application behavior ship
together without compatibility gates for older binaries. Migrations backfill
normalized file references, reject cross-conversation item/turn corruption, and
install the constraints required by atomic turn start before the service accepts
traffic. Transactional file deletion and atomic turn start are then active by
default.

Potentially blocking schema changes use PostgreSQL's online patterns: add
`conversations_next_seq_check` as `NOT VALID` and validate it separately; create
the turn composite and partial unique indexes with `CREATE UNIQUE INDEX
CONCURRENTLY` before attaching or replacing constraints. Every brief catalog-lock
step uses a bounded `lock_timeout` and is safely retryable.

### Observability

Emit a best-effort structured event per request attempt after the outcome is
known, with operation name, actor/client, conversation ID when authorized, turn
ID on success, replay status, and a bounded outcome code. A crash may lose the
event; exactly-once audit delivery is not claimed. Do not log item payloads or
raw idempotency keys; log a keyed or one-way digest of the key for correlation.
Metrics distinguish created, replayed, active-turn conflict, altered-key
conflict, deleted-result conflict, validation failure, authorization failure,
and storage failure.

### Test plan

Integration tests against PostgreSQL cover:

- existing-conversation success and correct item-to-turn association;
- new-conversation success with defaults and supplied metadata;
- validation and file-reference failures create no resources;
- injected failures at turn, item, sequence update, and operation-record writes
  roll back the full transaction, including a newly created conversation;
- exact retries return identical IDs in request order without duplicate rows;
- semantically equivalent requests using explicit defaults replay successfully;
- a request committed under digest version 1 replays after version 2 becomes the
  current version;
- changed conversation target, creation fields, agent, item order, or item content
  with the same key returns `idempotency_key_reused`;
- two concurrent exact requests converge on one result;
- concurrent whitespace-equivalent keys converge on one result;
- two concurrent requests with the same actor/client/key but different digests
  produce one commit and one `idempotency_key_reused`, with no loser resources;
- two concurrent different keys targeting one idle conversation produce one turn
  and one `active_turn_exists` response with no losing items;
- an ordinary concurrent append receives a disjoint, gap-free sequence range;
- an existing active turn causes no append or sequence allocation;
- a retry succeeds after its turn becomes terminal and while a newer turn is
  active;
- replay after truncation or conversation deletion returns
  `idempotency_result_deleted` without recreating resources;
- missing permissions and cross-owner IDs expose no operation or resource data;
- the active-turn unique-index race maps to HTTP 409 rather than HTTP 500;
- referenced-file deletion cannot race a successful item insertion;
- truncation and conversation deletion cascade item-file rows while referenced
  files remain deletion-protected;
- existing ordinary turn creation replays and races correctly after its partial
  unique-index and transaction migration;
- a pre-upgrade keyed turn with surrounding whitespace in `agent_ref` replays
  correctly after normalization backfill;
- atomic starts do not collide with ordinary turn idempotency keys or identical
  request keys from another `client_id`;
- the allowed-input-file snapshot contains prior references, excludes triggering
  and unrelated owner files, represents an authoritative empty set, rejects
  legacy unmarked turns, and requires signed delegation for triggering files;
- sequence upper-bound exhaustion creates no resources;
- item count, JSON shape, key, agent, title, metadata, multibyte scalar-count,
  whitespace normalization, and body-size boundaries.
- malformed JSON and duplicate keys at the top level, in `conversation`, and
  recursively in item payloads use the standard `invalid_request` envelope.

Unit tests use published RFC 8785 canonical-byte and SHA-256 fixtures, including
numbers, escapes, Unicode, duplicate-key rejection, tagged alternatives,
normalization/default equivalence, digest version selection, and database-error
classification by constraint name.

The repository gains a real PostgreSQL integration-test harness with an isolated
schema per test suite and unique actor/client/key namespaces. A test-only
failpoint parameter on the transaction-aware store function, unavailable in
production builds, injects errors after each mutation stage so rollback tests do
not depend on fragile database triggers.

## Drawbacks

- The operation overlaps three existing APIs and adds transaction-aware internal
  paths that must remain behaviorally aligned with them.
- Durable idempotency rows consume storage after conversations are deleted.
- PostgreSQL advisory locking introduces a database-specific mechanism and needs
  a stable collision-resistant 64-bit lock-key derivation. A hash collision only
  serializes unrelated requests; it does not merge their operation rows.
- External launch remains only retryable, not atomic with ledger persistence.

## Alternatives

### Have Parley compensate for partial failure

Parley could cancel a turn or delete a new conversation after append failure.
Compensation can itself fail, cannot make uncertain responses safe, and exposes
ledger consistency to every client implementation.

### Extend only the turn endpoint

`POST /v1/conversations/{id}/turns` could accept initial items. This handles an
existing conversation cleanly but cannot atomically create a conversation and
requires a second top-level endpoint or client branch. A single top-level
operation gives both modes one idempotency scope and response contract.

### Client-supplied resource IDs

Parley could pre-generate conversation and turn IDs and retry individual calls.
This leaks ID-generation contracts, still exposes partially initialized objects,
and does not atomically enforce the active-turn invariant with item append.

### Insert an in-progress operation row without advisory locking

The first request could claim the unique key by inserting an operation row, then
fill its result. Because all writes are in one transaction, a concurrent insert
would block and then resolve correctly. This avoids advisory locks but requires
nullable result columns or a separate request/result state model. The proposed
advisory lock keeps every committed row complete.

### Transactional outbox

Threadmark could write a launch event in the same transaction for a worker to
deliver. That provides eventual dispatch only if Threadmark owns the delivery
contract and destination. It is valuable future work but broader than Parley's
request and changes Threadmark from ledger to workflow participant.

## Unresolved questions

None. No automated idempotency purge is part of this feature.
