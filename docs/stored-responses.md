# Stored response guarantees

Threadmark stores terminal public Open Responses objects with these guarantees:

- Identity is scoped by tenant, owner, and agent.
- The original validated JSON text is returned unchanged.
- A canonical SHA-256 digest and JSONB copy are checked before retrieval.
- The terminal turn, continuation, and transcript boundary are linked atomically.
- Stored responses cannot be updated in place.

Use `POST /v1/conversations/{id}/responses` to persist a response and
`GET /v1/responses/{response_id}?agent_ref=...` to recover its public JSON.
Private continuation state is available only from the continuation API.
