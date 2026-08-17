ALTER TABLE turns ADD COLUMN reserved_response_id text;

CREATE TABLE turn_finalizations (
    turn_id text PRIMARY KEY REFERENCES turns(id) ON DELETE CASCADE,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    agent_ref text NOT NULL,
    idempotency_key text NOT NULL,
    response_id text NOT NULL,
    request_version smallint NOT NULL CHECK (request_version > 0),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    response jsonb NOT NULL,
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    first_seq bigint NOT NULL CHECK (first_seq > 0),
    last_seq bigint NOT NULL CHECK (last_seq >= first_seq - 1),
    continuation_id text NOT NULL REFERENCES continuations(id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_ref, agent_ref, idempotency_key),
    UNIQUE (tenant_id, agent_ref, response_id)
);
