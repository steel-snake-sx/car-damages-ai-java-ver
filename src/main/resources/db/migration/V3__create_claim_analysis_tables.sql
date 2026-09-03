CREATE TABLE claim_analyses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id UUID NOT NULL UNIQUE REFERENCES claims(id) ON DELETE CASCADE,
    car_detected BOOLEAN NOT NULL,
    summary VARCHAR(1000) NOT NULL,
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE claim_analysis_findings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    position SMALLINT NOT NULL CHECK (position >= 0),
    part_name VARCHAR(120) NOT NULL,
    description VARCHAR(500) NOT NULL,
    severity VARCHAR(16) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH')),
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    UNIQUE (claim_id, position)
);

ALTER TABLE claims
    ADD COLUMN analysis_failure_reason VARCHAR(32) CHECK (
        analysis_failure_reason IN ('AI_UNAVAILABLE', 'AI_REQUEST_REJECTED', 'INVALID_AI_RESULT')
    );
