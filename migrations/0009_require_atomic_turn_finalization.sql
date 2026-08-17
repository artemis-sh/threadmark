CREATE OR REPLACE FUNCTION require_atomic_turn_finalization()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status IN ('completed', 'incomplete', 'failed', 'cancelled')
       AND NOT EXISTS (SELECT 1 FROM turn_finalizations WHERE turn_id = NEW.id) THEN
        RAISE EXCEPTION 'terminal turns must be finalized atomically'
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER turns_require_atomic_finalization
AFTER INSERT OR UPDATE OF status ON turns
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION require_atomic_turn_finalization();
