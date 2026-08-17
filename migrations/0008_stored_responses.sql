ALTER TABLE continuations ADD COLUMN owner_ref text;
ALTER TABLE continuations ADD COLUMN turn_id text REFERENCES turns(id) ON DELETE CASCADE;

UPDATE continuations continuation
SET owner_ref = conversation.owner_ref
FROM conversations conversation
WHERE conversation.id = continuation.conversation_id;

ALTER TABLE continuations ALTER COLUMN owner_ref SET NOT NULL;
ALTER TABLE continuations
    DROP CONSTRAINT continuations_tenant_id_agent_ref_response_id_key;
ALTER TABLE continuations
    ADD CONSTRAINT continuations_tenant_owner_agent_response_key
    UNIQUE (tenant_id, owner_ref, agent_ref, response_id);

ALTER TABLE conversations
    ADD CONSTRAINT conversations_id_tenant_owner_key
    UNIQUE (id, tenant_id, owner_ref);
ALTER TABLE turns
    ADD CONSTRAINT turns_id_conversation_agent_key
    UNIQUE (id, conversation_id, agent_ref);
ALTER TABLE continuations
    ADD CONSTRAINT continuations_identity_link_key
    UNIQUE (id, tenant_id, owner_ref, conversation_id, turn_id, agent_ref,
            response_id, through_seq);
ALTER TABLE continuations
    ADD CONSTRAINT continuations_owned_conversation_fkey
    FOREIGN KEY (conversation_id, tenant_id, owner_ref)
    REFERENCES conversations (id, tenant_id, owner_ref) ON DELETE CASCADE;
ALTER TABLE continuations
    ADD CONSTRAINT continuations_turn_link_fkey
    FOREIGN KEY (turn_id, conversation_id, agent_ref)
    REFERENCES turns (id, conversation_id, agent_ref) ON DELETE CASCADE;

CREATE INDEX continuations_owner_agent_response_idx
    ON continuations (tenant_id, owner_ref, agent_ref, response_id);

CREATE TABLE stored_responses (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    agent_ref text NOT NULL,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    turn_id text NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
    continuation_id text NOT NULL UNIQUE REFERENCES continuations(id) ON DELETE CASCADE,
    response_id text NOT NULL,
    previous_response_id text,
    terminal_status text NOT NULL
        CHECK (terminal_status IN ('completed', 'incomplete', 'failed', 'cancelled')),
    public_response jsonb NOT NULL CHECK (jsonb_typeof(public_response) = 'object'),
    public_response_text text NOT NULL
        CHECK (octet_length(public_response_text) BETWEEN 2 AND 1048576),
    canonical_digest bytea NOT NULL CHECK (octet_length(canonical_digest) = 32),
    schema_marker text NOT NULL,
    canonical_size bigint NOT NULL CHECK (canonical_size BETWEEN 2 AND 1048576),
    through_seq bigint NOT NULL CHECK (through_seq >= 0),
    response_created_at timestamptz NOT NULL,
    terminal_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (terminal_at >= response_created_at),
    UNIQUE (tenant_id, owner_ref, agent_ref, response_id)
);

ALTER TABLE stored_responses
    ADD CONSTRAINT stored_responses_owned_conversation_fkey
    FOREIGN KEY (conversation_id, tenant_id, owner_ref)
    REFERENCES conversations (id, tenant_id, owner_ref) ON DELETE CASCADE;
ALTER TABLE stored_responses
    ADD CONSTRAINT stored_responses_turn_link_fkey
    FOREIGN KEY (turn_id, conversation_id, agent_ref)
    REFERENCES turns (id, conversation_id, agent_ref) ON DELETE CASCADE;
ALTER TABLE stored_responses
    ADD CONSTRAINT stored_responses_continuation_link_fkey
    FOREIGN KEY (continuation_id, tenant_id, owner_ref, conversation_id, turn_id,
                 agent_ref, response_id, through_seq)
    REFERENCES continuations
        (id, tenant_id, owner_ref, conversation_id, turn_id, agent_ref,
         response_id, through_seq) ON DELETE CASCADE;

CREATE INDEX stored_responses_conversation_turn_idx
    ON stored_responses (conversation_id, turn_id);
CREATE INDEX stored_responses_turn_idx ON stored_responses (turn_id);

CREATE FUNCTION reject_stored_response_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'terminal public responses are immutable'
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER stored_responses_immutable
BEFORE UPDATE ON stored_responses
FOR EACH ROW EXECUTE FUNCTION reject_stored_response_update();

CREATE FUNCTION reject_stored_response_turn_rewrite() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM stored_responses WHERE turn_id = OLD.id)
       AND (NEW.status, NEW.response_id, NEW.error, NEW.usage, NEW.completed_at)
           IS DISTINCT FROM
           (OLD.status, OLD.response_id, OLD.error, OLD.usage, OLD.completed_at) THEN
        RAISE EXCEPTION 'a turn with a stored terminal response is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER turns_stored_response_immutable
BEFORE UPDATE ON turns
FOR EACH ROW EXECUTE FUNCTION reject_stored_response_turn_rewrite();
