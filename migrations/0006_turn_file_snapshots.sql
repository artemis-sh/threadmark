DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM conversation_items item
        JOIN turns turn_record ON turn_record.id = item.turn_id
        WHERE item.conversation_id <> turn_record.conversation_id
    ) THEN
        RAISE EXCEPTION 'cross-conversation item turn references require manual repair';
    END IF;
END $$;

ALTER TABLE turns ADD CONSTRAINT turns_id_conversation_id_key UNIQUE (id, conversation_id);
ALTER TABLE conversation_items DROP CONSTRAINT conversation_items_turn_id_fkey;
ALTER TABLE conversation_items
    ADD CONSTRAINT conversation_items_turn_conversation_fkey
    FOREIGN KEY (turn_id, conversation_id)
    REFERENCES turns(id, conversation_id)
    ON DELETE SET NULL (turn_id);

CREATE TABLE turn_file_snapshots (
    id text PRIMARY KEY,
    turn_id text NOT NULL UNIQUE REFERENCES turns(id) ON DELETE CASCADE,
    authoritative boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE turn_file_snapshot_files (
    snapshot_id text NOT NULL REFERENCES turn_file_snapshots(id) ON DELETE CASCADE,
    file_id text NOT NULL,
    PRIMARY KEY (snapshot_id, file_id),
    CONSTRAINT turn_file_snapshot_files_file_id_fkey
        FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE RESTRICT
);

CREATE INDEX turn_file_snapshot_files_file_id_idx
    ON turn_file_snapshot_files (file_id);
