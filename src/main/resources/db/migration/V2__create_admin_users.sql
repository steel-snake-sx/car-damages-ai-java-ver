CREATE TABLE admin_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(254) NOT NULL UNIQUE CHECK (BTRIM(email) <> ''),
    password_hash VARCHAR(60) NOT NULL CHECK (
        password_hash ~ '^\$2[aby]\$10\$[./0-9A-Za-z]{53}$'
    ),
    created_at TIMESTAMPTZ NOT NULL
);

INSERT INTO admin_users (email, password_hash, created_at)
VALUES (LOWER(BTRIM('${admin-email}')), '${admin-password-hash}', CURRENT_TIMESTAMP);
