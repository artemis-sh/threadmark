CREATE TABLE conversation_item_files (
    item_id text NOT NULL REFERENCES conversation_items(id) ON DELETE CASCADE,
    file_id text NOT NULL,
    PRIMARY KEY (item_id, file_id),
    CONSTRAINT conversation_item_files_file_id_fkey
        FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE RESTRICT
);

CREATE INDEX conversation_item_files_file_id_idx
    ON conversation_item_files (file_id);

INSERT INTO conversation_item_files (item_id, file_id)
SELECT DISTINCT item.id, file.id
FROM conversation_items item
CROSS JOIN LATERAL jsonb_path_query(
    item.payload,
    'strict $.** ? (@.type() == "string")'
) AS value
JOIN files file ON value #>> '{}' = 'threadmark://files/' || file.id
ON CONFLICT DO NOTHING;

CREATE TABLE file_deletion_outbox (
    file_id text PRIMARY KEY,
    storage_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
