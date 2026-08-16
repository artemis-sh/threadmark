CREATE TABLE file_uploads (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    file_id text NOT NULL UNIQUE,
    filename text NOT NULL,
    mime_type text NOT NULL,
    expected_size bigint NOT NULL CHECK (expected_size >= 0),
    staging_key text NOT NULL UNIQUE,
    candidate_key text,
    status text NOT NULL CHECK (status IN ('pending', 'finalizing', 'completed', 'cleanup_pending')),
    lease_token text,
    lease_expires_at timestamptz,
    expires_at timestamptz NOT NULL,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key),
    CHECK ((status = 'completed') = (completed_at IS NOT NULL))
);

CREATE INDEX file_uploads_owner_id_idx ON file_uploads (tenant_id, owner_ref, id);
CREATE INDEX file_uploads_expiry_idx ON file_uploads (expires_at) WHERE status <> 'completed';

CREATE TABLE object_deletion_outbox (
    id text PRIMARY KEY,
    storage_key text NOT NULL,
    version_id text,
    all_versions boolean NOT NULL DEFAULT false,
    not_before timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX object_deletion_outbox_version_idx
    ON object_deletion_outbox (storage_key, version_id)
    WHERE version_id IS NOT NULL;
CREATE UNIQUE INDEX object_deletion_outbox_all_versions_idx
    ON object_deletion_outbox (storage_key)
    WHERE all_versions;
ALTER TABLE object_deletion_outbox ADD CHECK (
    (all_versions AND version_id IS NULL) OR (NOT all_versions AND version_id IS NOT NULL)
);
