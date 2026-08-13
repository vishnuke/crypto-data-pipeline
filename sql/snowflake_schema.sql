-- =============================================================================
-- Snowflake setup for the crypto pipeline
-- Run once, as ACCOUNTADMIN or a role with CREATE privileges.
-- Uses the smallest warehouse size (X-SMALL) with aggressive auto-suspend -
-- this is a low-volume streaming workload, no reason to pay for more, and
-- it keeps compute cost near-zero for a portfolio project.
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60          -- suspend after 60s idle
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS CRYPTO_DB;

CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.SILVER;

-- -----------------------------------------------------------------------------
-- BRONZE: raw landing zone. Append-only (via MERGE for idempotency), keeps
-- the full original JSON payload for replay/debugging/schema evolution.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CRYPTO_DB.BRONZE.CRYPTO_RAW (
    event_id            STRING        NOT NULL,   -- stable hash: dedupe key
    symbol               STRING        NOT NULL,
    price_usd            FLOAT,
    change_24h_pct        FLOAT,
    source_updated_at    TIMESTAMP_NTZ,             -- epoch from the API, as timestamp
    ingested_at          TIMESTAMP_NTZ NOT NULL,
    raw_payload          VARIANT,                   -- full original event, for auditability
    loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_crypto_raw PRIMARY KEY (event_id)  -- Snowflake doesn't enforce, but documents intent
);

-- -----------------------------------------------------------------------------
-- SILVER: clean, typed, deduplicated, analytics-ready. This is what
-- Power BI / analysts should query, never Bronze directly.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS CRYPTO_DB.SILVER.CRYPTO_PRICES (
    event_id            STRING        NOT NULL,
    symbol               STRING        NOT NULL,
    price_usd            FLOAT         NOT NULL,
    change_24h_pct        FLOAT,
    source_updated_at    TIMESTAMP_NTZ,
    ingested_at          TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT pk_crypto_prices PRIMARY KEY (event_id)
)
CLUSTER BY (symbol, ingested_at);   -- speeds up "latest price per symbol" / time-range BI queries

-- -----------------------------------------------------------------------------
-- Convenience view for Power BI: latest price per symbol
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW CRYPTO_DB.SILVER.V_LATEST_PRICES AS
SELECT symbol, price_usd, change_24h_pct, ingested_at
FROM (
    SELECT
        symbol, price_usd, change_24h_pct, ingested_at,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ingested_at DESC) AS rn
    FROM CRYPTO_DB.SILVER.CRYPTO_PRICES
)
WHERE rn = 1;

-- -----------------------------------------------------------------------------
-- Least-privilege role for Airflow's Snowflake connection.
-- Swap in your own password / auth method (key-pair auth recommended
-- for production instead of a plaintext password - see README).
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS CRYPTO_LOADER_ROLE;
GRANT USAGE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_LOADER_ROLE;
GRANT USAGE ON DATABASE CRYPTO_DB TO ROLE CRYPTO_LOADER_ROLE;
GRANT USAGE ON SCHEMA CRYPTO_DB.BRONZE TO ROLE CRYPTO_LOADER_ROLE;
GRANT USAGE ON SCHEMA CRYPTO_DB.SILVER TO ROLE CRYPTO_LOADER_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CRYPTO_DB.BRONZE TO ROLE CRYPTO_LOADER_ROLE;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA CRYPTO_DB.SILVER TO ROLE CRYPTO_LOADER_ROLE;

-- Read-only role for Power BI
CREATE ROLE IF NOT EXISTS CRYPTO_BI_ROLE;
GRANT USAGE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_BI_ROLE;
GRANT USAGE ON DATABASE CRYPTO_DB TO ROLE CRYPTO_BI_ROLE;
GRANT USAGE ON SCHEMA CRYPTO_DB.SILVER TO ROLE CRYPTO_BI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CRYPTO_DB.SILVER TO ROLE CRYPTO_BI_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CRYPTO_DB.SILVER TO ROLE CRYPTO_BI_ROLE;

-- Remember to: GRANT ROLE CRYPTO_LOADER_ROLE TO USER <your_airflow_service_user>;
--              GRANT ROLE CRYPTO_BI_ROLE TO USER <your_powerbi_service_user>;
