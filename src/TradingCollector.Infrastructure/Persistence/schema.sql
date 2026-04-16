CREATE TABLE IF NOT EXISTS ticks (
    id          BIGSERIAL    PRIMARY KEY,
    ticker      VARCHAR(20)  NOT NULL,
    price       NUMERIC(20,8) NOT NULL,
    volume      NUMERIC(20,8) NOT NULL,
    timestamp   TIMESTAMPTZ  NOT NULL,
    source      VARCHAR(50)  NOT NULL,
    received_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticks_ticker_ts ON ticks (ticker, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ticks_source    ON ticks (source);
