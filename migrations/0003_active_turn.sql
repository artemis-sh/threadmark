CREATE UNIQUE INDEX turns_one_active_per_conversation_idx
    ON turns (conversation_id)
    WHERE status IN ('pending', 'streaming');
