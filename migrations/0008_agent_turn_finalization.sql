UPDATE turns SET response_id = 'resp_legacy_' || md5(id) WHERE response_id IS NULL;
ALTER TABLE turns ALTER COLUMN response_id SET NOT NULL;

ALTER TABLE continuations ADD COLUMN owner_ref text;
ALTER TABLE continuations ADD COLUMN turn_id text REFERENCES turns(id) ON DELETE CASCADE;

UPDATE continuations continuation
SET owner_ref = conversation.owner_ref
FROM conversations conversation
WHERE conversation.id = continuation.conversation_id;

ALTER TABLE continuations ALTER COLUMN owner_ref SET NOT NULL;

ALTER TABLE continuations
    DROP CONSTRAINT IF EXISTS continuations_tenant_id_agent_ref_response_id_key;
ALTER TABLE continuations
    ADD CONSTRAINT continuations_actor_agent_response_key
    UNIQUE (tenant_id, owner_ref, agent_ref, response_id);

CREATE TABLE agent_turn_finalizations (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_version smallint NOT NULL CHECK (request_version > 0),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    conversation_id text NOT NULL,
    turn_id text NOT NULL UNIQUE,
    agent_ref text NOT NULL,
    response_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('completed', 'incomplete', 'failed', 'cancelled')),
    first_seq bigint,
    last_seq bigint,
    through_seq bigint NOT NULL CHECK (through_seq >= 0),
    continuation_id text NOT NULL UNIQUE,
    terminal_response jsonb NOT NULL,
    error jsonb,
    usage jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT agent_turn_finalizations_sequence_range_check CHECK (
        (first_seq IS NULL AND last_seq IS NULL)
        OR (first_seq > 0 AND last_seq >= first_seq AND last_seq = through_seq)
    ),
    CONSTRAINT agent_turn_finalizations_idempotency_key
        UNIQUE (tenant_id, owner_ref, client_id, idempotency_key),
    CONSTRAINT agent_turn_finalizations_response_key
        UNIQUE (tenant_id, owner_ref, agent_ref, response_id)
);

CREATE TABLE agent_turn_finalization_items (
    finalization_id text NOT NULL
        REFERENCES agent_turn_finalizations(id) ON DELETE RESTRICT,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    item_id text NOT NULL,
    seq bigint NOT NULL CHECK (seq > 0),
    PRIMARY KEY (finalization_id, ordinal),
    UNIQUE (finalization_id, item_id),
    UNIQUE (finalization_id, seq)
);
