CREATE TABLE agent_turn_starts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    agent_ref text NOT NULL,
    response_id text NOT NULL,
    previous_response_id text,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    turn_id text NOT NULL UNIQUE REFERENCES turns(id) ON DELETE CASCADE,
    predecessor_seq bigint,
    input_seq bigint NOT NULL CHECK (input_seq > 0),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key),
    UNIQUE (tenant_id, agent_ref, response_id)
);
CREATE UNIQUE INDEX agent_turn_starts_one_child_idx ON agent_turn_starts (tenant_id, agent_ref, previous_response_id) WHERE previous_response_id IS NOT NULL;
