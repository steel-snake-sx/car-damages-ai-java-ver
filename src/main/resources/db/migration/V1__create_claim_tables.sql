CREATE TABLE claims (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    car_brand VARCHAR(80) NOT NULL,
    car_model VARCHAR(80) NOT NULL,
    car_year SMALLINT NOT NULL CHECK (car_year BETWEEN 1886 AND 2100),
    status VARCHAR(32) NOT NULL CHECK (
        status IN ('ANALYSIS_PENDING', 'ANALYZING', 'ANALYZED', 'ANALYSIS_FAILED')
    ),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE claim_images (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    claim_id UUID NOT NULL REFERENCES claims(id) ON DELETE CASCADE,
    storage_path VARCHAR(500) NOT NULL UNIQUE,
    original_filename VARCHAR(255) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_claim_images_claim_id ON claim_images(claim_id);
