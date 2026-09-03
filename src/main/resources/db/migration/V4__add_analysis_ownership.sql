ALTER TABLE claims
    ADD COLUMN analysis_owner_token UUID,
    ADD COLUMN analysis_lease_until TIMESTAMPTZ;
