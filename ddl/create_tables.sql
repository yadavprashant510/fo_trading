CREATE DATABASE fo_trading;

CREATE TABLE exchanges (
    exchange_id SERIAL PRIMARY KEY,
    exchange_name VARCHAR(10) UNIQUE NOT NULL
);

CREATE TABLE instruments (
    instrument_id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    instrument_type VARCHAR(10) NOT NULL,
    exchange_id INT REFERENCES exchanges (exchange_id)
);

CREATE TABLE expiries (
    expiry_id SERIAL PRIMARY KEY,
    instrument_id INT REFERENCES instruments (instrument_id),
    expiry_dt DATE NOT NULL,
    strike_pr NUMERIC(10, 2),
    option_typ VARCHAR(2)
);

CREATE TABLE trades (
    trade_id BIGSERIAL PRIMARY KEY,
    instrument_id INT REFERENCES instruments (instrument_id),
    expiry_id INT REFERENCES expiries (expiry_id),
    trade_date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    val_inlakh BIGINT,
    open_int BIGINT,
    settle_pr NUMERIC(10, 2),
    chg_in_oi BIGINT,
    contracts BIGINT
);