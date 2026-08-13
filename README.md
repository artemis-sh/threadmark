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
| `capability_url` | Mint an expiring, signed Threadmark HTTPS download URL |
| `presigned_url` | Mint a direct S3 URL using `S3_PUBLIC_URL` |
| `inline` | Read S3 bytes and emit `file_data` or an image data URL |

Consumers must not send the `threadmark://` URI directly to a model provider
that only accepts HTTP(S) or data URLs. Append rejects Threadmark file URIs that
do not resolve to a file owned by the conversation principal. A file referenced
by any conversation cannot be deleted.

## Status

This is an experiment and its API is not stable. The current slice establishes:

- Postgres-backed, tenant-isolated conversations.
- Transactional, gap-free item ordering within each conversation.
- Idempotent item batches and turn creation.
- Cursor-based item reads.
- Open Responses replay projection with optional top-level `id` removal.
- Agent-scoped continuation records and optional private checkpoint state.

Editing, branching, retention, event delivery, production authentication, and
fine-grained capabilities are intentionally deferred until the core contract is
validated by a second client.

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

The S3-compatible bucket must exist before Threadmark starts. Compose creates a
local MinIO bucket automatically. In production, configure `S3_ENDPOINT`,
`S3_BUCKET`, credentials, and optionally a separately reachable
`S3_PUBLIC_URL` for direct presigned delivery.

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

An agent called by Parley can receive a short-lived token scoped to the same
tenant, principal, conversation, turn, and agent deployment. That authorization
layer is deliberately separate from the ledger model.

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
| `GET` | `/v1/conversations/{id}` | Read conversation metadata |
| `GET` | `/v1/conversations/{id}/items` | Read ordered items after a sequence cursor |
| `POST` | `/v1/conversations/{id}/items` | Atomically append an idempotent item batch |
| `POST` | `/v1/conversations/{id}/replay` | Build an Open Responses input array |
| `POST` | `/v1/conversations/{id}/turns` | Create an idempotent turn |
| `PATCH` | `/v1/turns/{id}` | Update turn state and outcome |
| `POST` | `/v1/conversations/{id}/continuations` | Record an agent checkpoint |
| `GET` | `/v1/continuations/{response_id}` | Resolve an agent checkpoint |
| `POST` | `/v1/files` | Upload a tenant-owned S3-backed file |
| `GET` | `/v1/files/{id}` | Read owned file metadata |
| `DELETE` | `/v1/files/{id}` | Delete an unreferenced owned file |
| `GET` | `/v1/capabilities/files/{id}` | Download through a signed capability |

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
