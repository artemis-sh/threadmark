ALTER TABLE turns ALTER COLUMN idempotency_key DROP NOT NULL;

CREATE UNIQUE INDEX turns_conversation_id_idempotency_key_idx
    ON turns (conversation_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

ALTER TABLE conversations
    ADD CONSTRAINT conversations_next_seq_check CHECK (next_seq >= 1) NOT VALID;
ALTER TABLE conversations VALIDATE CONSTRAINT conversations_next_seq_check;

CREATE TABLE turn_starts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_version smallint NOT NULL CHECK (request_version > 0),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    conversation_id text NOT NULL,
    turn_id text NOT NULL UNIQUE,
    first_seq bigint NOT NULL CHECK (first_seq > 0),
    last_seq bigint NOT NULL CHECK (last_seq >= first_seq),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key)
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
