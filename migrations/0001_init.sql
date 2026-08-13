CREATE TABLE conversations (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    title text NOT NULL DEFAULT 'New conversation',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    next_seq bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX conversations_tenant_owner_updated_idx
    ON conversations (tenant_id, owner_ref, updated_at DESC);

CREATE TABLE turns (
    id text PRIMARY KEY,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    agent_ref text NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'streaming', 'completed', 'incomplete', 'failed', 'cancelled')),
    response_id text,
    error jsonb,
    usage jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    UNIQUE (conversation_id, idempotency_key)
);

CREATE INDEX turns_conversation_created_idx ON turns (conversation_id, created_at);

CREATE TABLE append_batches (
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    idempotency_key text NOT NULL,
    first_seq bigint NOT NULL,
    last_seq bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (conversation_id, idempotency_key)
);

CREATE TABLE conversation_items (
    id text PRIMARY KEY,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    turn_id text REFERENCES turns(id) ON DELETE SET NULL,
    seq bigint NOT NULL,
    source text NOT NULL CHECK (source IN ('user', 'agent', 'system')),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, seq)
);

CREATE INDEX conversation_items_conversation_seq_idx
    ON conversation_items (conversation_id, seq);

CREATE TABLE continuations (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    agent_ref text NOT NULL,
    response_id text NOT NULL,
    parent_response_id text,
    through_seq bigint NOT NULL,
    state jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, agent_ref, response_id)
);

CREATE INDEX continuations_conversation_agent_created_idx
    ON continuations (conversation_id, agent_ref, created_at DESC);
