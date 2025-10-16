-- Таблица market_data
CREATE TABLE IF NOT EXISTS market_data (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc') NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc') NOT NULL,

    secid VARCHAR(36) NOT NULL,
    boardid VARCHAR(12) NOT NULL,
    instrument_type VARCHAR(10) NOT NULL,

    shortname VARCHAR(255),
    currency VARCHAR(10),
    list_level INTEGER,

    last_price NUMERIC(18,8),
    open_price NUMERIC(18,8),
    high_price NUMERIC(18,8),
    low_price NUMERIC(18,8),
    change_abs NUMERIC(18,8),
    change_percent NUMERIC(10,6),
    volume BIGINT,
    trades_count INTEGER,
    volatility_percent NUMERIC(10,6),
    capitalization NUMERIC(20,2),
    change_capitalization NUMERIC(20,2),

    annual_high NUMERIC(18,8),
    annual_low NUMERIC(18,8),

    maturity_date DATE,
    couponpercent NUMERIC(10,6),
    couponvalue NUMERIC(18,8),
    couponperiod INTEGER,
    next_coupon_date DATE,
    accruedint NUMERIC(18,8),
    full_price NUMERIC(18,8),
    effectiveyield NUMERIC(10,6),
    duration_days INTEGER,
    duration_years NUMERIC(10,6),
    facevalue NUMERIC(18,8),
    isin VARCHAR(50),

    lotsize INTEGER,
    issuesize BIGINT,
    issuesizeplaced BIGINT,

    CONSTRAINT uq_market_data_secid_boardid UNIQUE (secid, boardid)
);

-- Таблица candles
CREATE TABLE IF NOT EXISTS candles (
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    close NUMERIC(18,8) NOT NULL,
    volume BIGINT NOT NULL,
    PRIMARY KEY (ticker, date)
);

-- Таблица market_caps
CREATE TABLE IF NOT EXISTS market_caps (
    timestamp DATE PRIMARY KEY,
    cap NUMERIC(24,6) NOT NULL
);


CREATE TABLE IF NOT EXISTS coupons (
    secid VARCHAR(51) PRIMARY KEY,
    data JSONB NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc') NOT NULL
);

CREATE TABLE companies (
    secid TEXT PRIMARY KEY,
    description TEXT,
    founded INTEGER,
    headquarters TEXT,
    employees TEXT,
    sector TEXT,
    ceo TEXT,
    link TEXT
);