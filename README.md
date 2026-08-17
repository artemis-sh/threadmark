# Threadmark

Threadmark is an experimental durable conversation ledger for systems that use
the [Open Responses](https://www.openresponses.org/) protocol. It stores
ordered protocol items without translating them into a proprietary message
model, tracks turns, prepares replay input, and records agent-scoped
continuation checkpoints.

The name reflects the two core objects: a conversation thread and durable marks
within it from which clients and agents can continue.

## Why this exists

Conversation state may live in a client such as Parley, or behind an agent's
`previous_response_id`. Threadmark gives both deployments the same storage
primitive:

- A client can append user and agent items and request full replay input.
- A stateful agent can resolve a response ID to its transcript position and an
  optional private JSON checkpoint.
- Multiple clients can share the ledger without sharing their application
  database schema.

Threadmark is not an agent runtime. It does not call agents, proxy SSE streams,
or interpret extension items. JSON items are intentionally opaque to the core
ledger. It does own optional S3-backed files so multimodal references remain
portable across clients and agents.

### Multimodal items

Images, files, audio, video, and future content-part extensions are preserved
as opaque JSON. Current Open Responses `input_image` parts may carry a remote
URL or data URL, and `input_file` parts may carry `file_url` or inline
`file_data`; Threadmark stores and replays each form without rewriting nested
content. Unknown media part types receive the same treatment.

The HTTP JSON body limit is 64 MiB so a request can contain the protocol's
roughly 32 MiB maximum inline file plus its JSON envelope. Large multi-file
batches should use separate append requests.

Threadmark-owned objects will use canonical URIs rather than application-local
sentinels:

```text
threadmark://files/file_01k2example
```

The URI authority identifies the resource class (`files`) and the path carries
the opaque resource ID. Stored transcript items keep this durable URI. Replay
can resolve it according to the receiving agent's delivery policy:

| `file_delivery` | Projection |
| --- | --- |
| `preserve` | Keep the canonical `threadmark://` URI (default) |
| `capability_url` | Mint an expiring Threadmark URL that streams from private S3 |
| `presigned_url` | Mint a direct S3 URL using `S3_PUBLIC_URL` |
| `inline` | Read S3 bytes and emit `file_data` or an image data URL |

Consumers must not send the `threadmark://` URI directly to a model provider
that only accepts HTTP(S) or data URLs. Append rejects Threadmark file URIs that
do not resolve to a file owned by the conversation principal. A file referenced
by any conversation cannot be deleted.

For direct file delivery, an authenticated caller creates a signed grant with
`POST /v1/files/{id}/downloads`. A `redirect` grant keeps Threadmark as the
stable authorization origin but responds with a temporary redirect to S3; a
`proxy` grant streams the private S3 object through Threadmark with bounded
memory for clients that cannot follow redirects. The selected mode is covered
by the signature and cannot be changed by the recipient.

## Status

This is an experiment and its API is not stable. The current slice establishes:

- Postgres-backed, tenant-isolated conversations.
- Transactional, gap-free item ordering within each conversation.
- Idempotent item batches and turn creation.
- Cursor-based item reads.
- Open Responses replay projection with optional top-level `id` removal.
- Snapshot-consistent, size-bounded text replay for delegated agent turns.
- Agent-scoped continuation records and optional private checkpoint state.

Editing, branching, retention, event delivery, production authentication, and
fine-grained capabilities are intentionally deferred until the core contract is
validated by a second client.

Parley now serves as that second client. Threadmark additionally exposes
owner-scoped conversation listing and updates, turn reads, destructive linear
edit/regenerate operations, and authenticated file content reads for the Parley
server adapter. The trusted-header identity boundary remains experimental and
requires network isolation until service authentication is implemented.

## Run

Start the complete local stack:

```bash
docker compose up --build
```

Or start only Postgres and run the Rust service locally:

```bash
docker compose up -d postgres minio bucket-init
set -a; source .env; set +a
cargo run
```

Migrations run automatically at startup. The API listens on port `8090` by
default, and `GET /health` checks database connectivity.

The S3-compatible bucket must exist before Threadmark starts and must have
versioning enabled. This is a global alpha storage prerequisite, including for
the retained server-mediated `POST /v1/files` endpoint, so Threadmark can
delete every object version safely. Compose creates and version-enables a local
MinIO bucket automatically. In production, configure `S3_ENDPOINT`,
`S3_BUCKET`, credentials with `s3:GetBucketVersioning`, and optionally a
separately reachable `S3_PUBLIC_URL` for direct presigned delivery.

With the stack running, `scripts/media-smoke.sh` verifies upload, all replay
delivery policies, byte integrity, ownership isolation, and referenced-file
deletion protection. It requires `curl`, `jq`, `sha256sum`, and `base64`.

## Identity boundary

Every `/v1` request currently requires these trusted headers:

```text
X-Threadmark-Tenant: acme
X-Threadmark-Principal: user_123
```

They are an integration seam, not production authentication. Do not expose this
experiment directly to untrusted traffic. A production deployment should
validate signed service credentials or short-lived capabilities and derive
these values from claims rather than accepting arbitrary headers.

Set `AUTH_MODE=trusted_headers` only in an isolated development build compiled
with `--features trusted-headers`; the default binary does not contain that
mode. Production uses
`AUTH_MODE=jwt` with `AUTH_ISSUER`, `AUTH_AUDIENCE`, and an HTTPS
`AUTH_JWKS_URL`. JWT mode accepts Ed25519 `at+jwt` owner-session tokens and
derives tenant, principal, and endpoint permissions exclusively from verified
claims. It also accepts delegated-agent tokens only for the agent replay
operation described below. Delegated writes remain disabled.

An agent called by Parley can receive a short-lived token scoped to the same
tenant, principal, conversation, turn, and agent deployment. That authorization
layer is deliberately separate from the ledger model.

### Bounded agent replay

`POST /v1/conversations/{conversation_id}/turns/{turn_id}/agent-replay` is the
initial Bonsai replay integration. It accepts no request body and requires a
`delegated_agent` JWT with `transcript:read` and exact `tenant`, `principal`,
`conversation_id`, `turn_id`, and `agent_ref` bounds. Wrong actor or resource
bounds return a non-enumerating error. The turn must have been created by
`POST /v1/turn-starts`; its recorded `last_seq` is the immutable replay cursor.

The operation opens a PostgreSQL repeatable-read, read-only transaction before
resolving ownership, the atomic-start boundary, and ordered items. It verifies
the complete triggering batch still exists in that snapshot. A concurrent
truncate is therefore observed wholly before or wholly after its commit; a
snapshot missing the boundary returns `replay_snapshot_unavailable` and never
returns a cursor for absent turn-start input.

The first integration supports only these historical message shapes:

- `type=message`, `role=user`, with a nonempty array of `input_text` parts
  containing only string `text` plus the `type` discriminator;
- `type=message`, `role=assistant`, with a nonempty array of `output_text` parts
  containing string `text`, the `type` discriminator, and optional
  `annotations`.

Other item types, roles, non-text parts, mixed content, and media return
`unsupported_agent_replay_item`. In particular, file and image parts are never
forwarded, and any canonical file URI anywhere in an item is rejected, so
unresolved `threadmark://` resources cannot reach a model provider through this
endpoint. Accepted top-level item fields are otherwise preserved. Only
top-level fields named by `AGENT_REPLAY_STRIP_TOP_LEVEL_FIELDS` (comma-separated,
default `id`) are removed; nested fields are untouched.

`AGENT_REPLAY_MAX_ITEMS` (default `200`) and `AGENT_REPLAY_MAX_BYTES` (default
`1048576`) are hard inclusive limits. The byte limit is the exact compact JSON
serialization of the returned `input` array after configured field removal.
Exceeding either limit returns HTTP `413` with
`error.code=context_limit_exceeded` before a projection is returned.

The existing owner endpoint, `POST /v1/conversations/{id}/replay`, is unchanged:
it remains an opaque, multimodal projection with caller-selected file delivery.

## Example flow

Create a conversation:

```bash
curl -sS http://localhost:8090/v1/conversations \
  -H 'content-type: application/json' \
  -H 'x-threadmark-tenant: acme' \
  -H 'x-threadmark-principal: user_123' \
  -d '{"title":"Trip planning","metadata":{"client":"parley"}}'
```

Create a turn, replacing `conv_...` with the returned ID:

```bash
curl -sS http://localhost:8090/v1/conversations/conv_.../turns \
  -H 'content-type: application/json' \
  -H 'x-threadmark-tenant: acme' \
  -H 'x-threadmark-principal: user_123' \
  -d '{"idempotency_key":"request-1","agent_ref":"research-agent/prod"}'
```

Append an Open Responses user item:

```bash
curl -sS http://localhost:8090/v1/conversations/conv_.../items \
  -H 'content-type: application/json' \
  -H 'x-threadmark-tenant: acme' \
  -H 'x-threadmark-principal: user_123' \
  -d '{
    "idempotency_key":"request-1-user",
    "turn_id":"turn_...",
    "source":"user",
    "items":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Plan a weekend in Lisbon"}]}]
  }'
```

Build protocol-ready replay input:

```bash
curl -sS http://localhost:8090/v1/conversations/conv_.../replay \
  -H 'content-type: application/json' \
  -H 'x-threadmark-tenant: acme' \
  -H 'x-threadmark-principal: user_123' \
  -d '{"after_seq":0,"strip_top_level_ids":true,"file_delivery":"capability_url"}'
```

Upload a file before referencing it in an item:

```bash
curl -sS http://localhost:8090/v1/files \
  -H 'x-threadmark-tenant: acme' \
  -H 'x-threadmark-principal: user_123' \
  -F 'file=@./report.pdf'
```

The response contains a durable `threadmark://files/file_...` URI. Store that
URI in an `input_file.file_url` or `input_image.image_url` part.

Record the checkpoint represented by an agent's completed response:

```bash
curl -sS http://localhost:8090/v1/conversations/conv_.../continuations \
  -H 'content-type: application/json' \
  -H 'x-threadmark-tenant: acme' \
  -H 'x-threadmark-principal: user_123' \
  -d '{
    "agent_ref":"research-agent/prod",
    "response_id":"resp_abc",
    "state":{"provider_thread":"thread_xyz"}
  }'
```

The agent can resolve that state later with:

```text
GET /v1/continuations/resp_abc?agent_ref=research-agent%2Fprod
```

## API summary

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/conversations` | Create a conversation |
| `POST` | `/v1/turn-starts` | Atomically create a conversation if needed, turn, and user items |
| `GET` | `/v1/conversations/{id}` | Read conversation metadata |
| `GET` | `/v1/conversations/{id}/items` | Read ordered items after a sequence cursor |
| `POST` | `/v1/conversations/{id}/items` | Atomically append an idempotent item batch |
| `POST` | `/v1/conversations/{id}/replay` | Build an Open Responses input array |
| `POST` | `/v1/conversations/{conversation_id}/turns/{turn_id}/agent-replay` | Build bounded text input for a delegated agent turn |
| `POST` | `/v1/conversations/{id}/turns` | Create an idempotent turn |
| `PATCH` | `/v1/turns/{id}` | Update turn state and outcome |
| `POST` | `/v1/conversations/{id}/continuations` | Record an agent checkpoint |
| `GET` | `/v1/continuations/{response_id}` | Resolve an agent checkpoint |
| `POST` | `/v1/files` | Upload a tenant-owned S3-backed file |
| `GET` | `/v1/files/{id}` | Read owned file metadata |
| `DELETE` | `/v1/files/{id}` | Delete an unreferenced owned file |
| `POST` | `/v1/files/{id}/downloads` | Mint a redirect or proxy download grant |
| `GET` | `/v1/downloads/files/{id}` | Redeem a signed stable Threadmark URL |

## Design notes

- `payload` is JSONB and remains protocol-owned. Threadmark only requires each
  item to be a JSON object.
- Sequence numbers are allocated while locking the conversation row. Concurrent
  append requests therefore have deterministic, non-overlapping order.
- Continuations are namespaced by tenant and `agent_ref`; the same response ID
  may safely exist for unrelated agents or tenants.
- Private continuation `state` is returned only through the continuation API.
  A future capability system must prevent ordinary UI clients from reading it.
- The replay endpoint is a convenience projection, not summarization. The
  canonical item ledger remains lossless.
- Capability signatures bind tenant, owner, file ID, and expiry. Capability
  failures return `404` to avoid exposing file existence.
