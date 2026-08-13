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
store attachments, or interpret extension items. JSON items are intentionally
opaque to the core ledger.

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
docker compose up -d postgres
DATABASE_URL=postgres://threadmark:threadmark@localhost:5434/threadmark cargo run
```

Migrations run automatically at startup. The API listens on port `8090` by
default, and `GET /health` checks database connectivity.

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
  -d '{"after_seq":0,"strip_top_level_ids":true}'
```

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
