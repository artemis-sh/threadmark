-- SQLite schema, equivalent to the PostgreSQL set in ../postgres.
--
-- Presented as one migration because SQLite deployments start empty: there is no
-- installed base to migrate forward, so the incremental history in ../postgres
-- would add no value here. The PostgreSQL set remains the specification; this
-- file must express the same constraints.
--
-- Type mapping:
--   jsonb       -> text, validated with json_valid
--   timestamptz -> text, holding the RFC 3339 form sqlx encodes DateTime<Utc> as
--   bigint      -> integer (SQLite integers are 64-bit)
--   bytea       -> blob
--   smallint    -> integer
--
-- Timestamp defaults deliberately spell out RFC 3339 with a `T` separator and an
-- explicit `+00:00` offset, matching the form sqlx encodes DateTime<Utc> as.
-- SQLite compares these as text, so a default written by CURRENT_TIMESTAMP
-- (`YYYY-MM-DD HH:MM:SS`) would sort before every application-written value and
-- silently break predicates such as `not_before <= $1`.
-- The two PostgreSQL migrations that backfill existing rows (0005, 0006) have no
-- counterpart here, because a SQLite deployment starts empty.

PRAGMA foreign_keys = ON;

CREATE TABLE conversations (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    title text NOT NULL DEFAULT 'New conversation',
    metadata text NOT NULL DEFAULT '{}' CHECK (json_valid(metadata)),
    next_seq integer NOT NULL DEFAULT 1 CHECK (next_seq >= 1),
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    updated_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
);

CREATE INDEX conversations_tenant_owner_updated_idx
    ON conversations (tenant_id, owner_ref, updated_at DESC);

CREATE TABLE turns (
    id text PRIMARY KEY,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    agent_ref text NOT NULL,
    idempotency_key text,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'streaming', 'completed', 'incomplete', 'failed', 'cancelled')),
    response_id text,
    error text CHECK (error IS NULL OR json_valid(error)),
    usage text CHECK (usage IS NULL OR json_valid(usage)),
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    completed_at text,
    UNIQUE (id, conversation_id)
);

CREATE INDEX turns_conversation_created_idx ON turns (conversation_id, created_at);

CREATE UNIQUE INDEX turns_one_active_per_conversation_idx
    ON turns (conversation_id)
    WHERE status IN ('pending', 'streaming');

CREATE UNIQUE INDEX turns_conversation_id_idempotency_key_idx
    ON turns (conversation_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE append_batches (
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    idempotency_key text NOT NULL,
    first_seq integer NOT NULL,
    last_seq integer NOT NULL,
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    PRIMARY KEY (conversation_id, idempotency_key)
);

CREATE TABLE conversation_items (
    id text PRIMARY KEY,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    turn_id text,
    seq integer NOT NULL,
    source text NOT NULL CHECK (source IN ('user', 'agent', 'system')),
    payload text NOT NULL CHECK (json_valid(payload)),
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    UNIQUE (conversation_id, seq),
    FOREIGN KEY (turn_id, conversation_id)
        REFERENCES turns(id, conversation_id)
);

CREATE INDEX conversation_items_conversation_seq_idx
    ON conversation_items (conversation_id, seq);

-- PostgreSQL 0006 uses `ON DELETE SET NULL (turn_id)`, a column-list form SQLite
-- does not support. This trigger reproduces it: deleting a turn detaches its
-- items rather than cascading the delete to them.
CREATE TRIGGER conversation_items_detach_deleted_turn
AFTER DELETE ON turns
FOR EACH ROW
BEGIN
    UPDATE conversation_items
       SET turn_id = NULL
     WHERE turn_id = OLD.id AND conversation_id = OLD.conversation_id;
END;

CREATE TABLE continuations (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    conversation_id text NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    agent_ref text NOT NULL,
    response_id text NOT NULL,
    parent_response_id text,
    through_seq integer NOT NULL,
    state text CHECK (state IS NULL OR json_valid(state)),
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    UNIQUE (tenant_id, agent_ref, response_id)
);

CREATE INDEX continuations_conversation_agent_created_idx
    ON continuations (conversation_id, agent_ref, created_at DESC);

CREATE TABLE files (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    filename text NOT NULL,
    mime_type text NOT NULL,
    size integer NOT NULL CHECK (size >= 0),
    storage_key text NOT NULL UNIQUE,
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
);

CREATE INDEX files_tenant_owner_created_idx
    ON files (tenant_id, owner_ref, created_at DESC);

CREATE TABLE turn_starts (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    owner_ref text NOT NULL,
    client_id text NOT NULL,
    idempotency_key text NOT NULL,
    request_version integer NOT NULL CHECK (request_version > 0),
    request_digest blob NOT NULL CHECK (length(request_digest) = 32),
    conversation_id text NOT NULL,
    turn_id text NOT NULL UNIQUE,
    first_seq integer NOT NULL CHECK (first_seq > 0),
    last_seq integer NOT NULL CHECK (last_seq >= first_seq),
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key)
);

CREATE TABLE turn_start_items (
    turn_start_id text NOT NULL REFERENCES turn_starts(id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    item_id text NOT NULL,
    seq integer NOT NULL CHECK (seq > 0),
    PRIMARY KEY (turn_start_id, ordinal),
    UNIQUE (turn_start_id, item_id),
    UNIQUE (turn_start_id, seq)
);

CREATE TABLE conversation_item_files (
    item_id text NOT NULL REFERENCES conversation_items(id) ON DELETE CASCADE,
    file_id text NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    PRIMARY KEY (item_id, file_id)
);

CREATE INDEX conversation_item_files_file_id_idx
    ON conversation_item_files (file_id);

CREATE TABLE file_deletion_outbox (
    file_id text PRIMARY KEY,
    storage_key text NOT NULL,
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
);

CREATE TABLE turn_file_snapshots (
    id text PRIMARY KEY,
    turn_id text NOT NULL UNIQUE REFERENCES turns(id) ON DELETE CASCADE,
    authoritative integer NOT NULL DEFAULT 1,
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
);

CREATE TABLE turn_file_snapshot_files (
    snapshot_id text NOT NULL REFERENCES turn_file_snapshots(id) ON DELETE CASCADE,
    file_id text NOT NULL REFERENCES files(id) ON DELETE RESTRICT,
    PRIMARY KEY (snapshot_id, file_id)
);

CREATE INDEX turn_file_snapshot_files_file_id_idx
    ON turn_file_snapshot_files (file_id);

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
    expected_size integer NOT NULL CHECK (expected_size >= 0),
    staging_key text NOT NULL UNIQUE,
    candidate_key text,
    status text NOT NULL CHECK (status IN ('pending', 'finalizing', 'completed', 'cleanup_pending')),
    lease_token text,
    lease_expires_at text,
    expires_at text NOT NULL,
    completed_at text,
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    UNIQUE (tenant_id, owner_ref, client_id, idempotency_key),
    CHECK ((status = 'completed') = (completed_at IS NOT NULL))
);

CREATE INDEX file_uploads_owner_id_idx ON file_uploads (tenant_id, owner_ref, id);
CREATE INDEX file_uploads_expiry_idx ON file_uploads (expires_at) WHERE status <> 'completed';

CREATE TABLE object_deletion_outbox (
    id text PRIMARY KEY,
    storage_key text NOT NULL,
    version_id text,
    all_versions integer NOT NULL DEFAULT 0,
    not_before text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    created_at text NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    CHECK ((all_versions = 1 AND version_id IS NULL)
        OR (all_versions = 0 AND version_id IS NOT NULL))
);

CREATE UNIQUE INDEX object_deletion_outbox_version_idx
    ON object_deletion_outbox (storage_key, version_id)
    WHERE version_id IS NOT NULL;
CREATE UNIQUE INDEX object_deletion_outbox_all_versions_idx
    ON object_deletion_outbox (storage_key)
    WHERE all_versions = 1;
