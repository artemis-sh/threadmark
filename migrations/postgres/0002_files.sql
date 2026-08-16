CREATE TABLE files (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    filename text NOT NULL,
    mime_type text NOT NULL,
    size bigint NOT NULL CHECK (size >= 0),
    storage_key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX files_tenant_owner_created_idx
    ON files (tenant_id, owner_ref, created_at DESC);
