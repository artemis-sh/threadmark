ALTER TABLE append_batches
    ADD COLUMN request_version smallint,
    ADD COLUMN request_digest bytea,
    ADD COLUMN source text,
    ADD COLUMN turn_id text,
    ADD COLUMN tenant_id text,
    ADD COLUMN owner_ref text,
    ADD COLUMN agent_ref text,
    ADD COLUMN item_count integer,
    ADD COLUMN item_ids text[];

ALTER TABLE append_batches ADD CONSTRAINT append_batches_delegated_request_check CHECK (
    (request_version IS NULL AND request_digest IS NULL AND source IS NULL AND turn_id IS NULL
        AND tenant_id IS NULL AND owner_ref IS NULL AND agent_ref IS NULL AND item_count IS NULL
        AND item_ids IS NULL)
    OR
    (request_version IS NOT NULL AND request_digest IS NOT NULL AND source IS NOT NULL
        AND turn_id IS NOT NULL AND tenant_id IS NOT NULL AND owner_ref IS NOT NULL
        AND agent_ref IS NOT NULL AND item_count IS NOT NULL AND item_ids IS NOT NULL
        AND request_version = 1 AND octet_length(request_digest) = 32 AND source = 'agent'
        AND item_count BETWEEN 1 AND 100 AND cardinality(item_ids) = item_count
        AND first_seq > 0 AND last_seq >= first_seq
        AND item_count::bigint = last_seq - first_seq + 1)
);
