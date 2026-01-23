CREATE INDEX IF NOT EXISTS idx_versions_by_valid_from
ON versions (kind, entity_id, valid_from);
