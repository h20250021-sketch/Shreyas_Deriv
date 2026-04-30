CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    country_code CHAR(2),
    kyc_status VARCHAR(20) DEFAULT 'pending',
    referral_source VARCHAR(50),
    last_login TIMESTAMPTZ
);

CREATE TABLE accounts (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    account_type VARCHAR(20) NOT NULL,
    currency CHAR(3) NOT NULL,
    balance NUMERIC(18,8) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE positions (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT REFERENCES accounts(id),
    symbol VARCHAR(20) NOT NULL,
    direction VARCHAR(5) NOT NULL,
    stake NUMERIC(18,8),
    entry_price NUMERIC(18,8),
    current_price NUMERIC(18,8),
    pnl NUMERIC(18,8),
    opened_at TIMESTAMPTZ DEFAULT NOW(),
    closed_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'open'
);

CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    account_id BIGINT REFERENCES accounts(id),
    transaction_type VARCHAR(30) NOT NULL,
    amount NUMERIC(18,8),
    currency CHAR(3),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    reference VARCHAR(100)
);

CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    action VARCHAR(100),
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    ip_address INET
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_accounts_user_id ON accounts(user_id);
CREATE INDEX idx_positions_account_id ON positions(account_id);
