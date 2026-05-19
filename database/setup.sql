-- ============================================================
-- APT Stock Exchange — Database Setup
-- ============================================================

-- Drop existing objects (safe re-run)
DROP TRIGGER IF EXISTS orders_notify_trigger   ON orders;
DROP TRIGGER IF EXISTS stocks_notify_trigger   ON stocks;
DROP TRIGGER IF EXISTS trades_notify_trigger   ON trades;
DROP FUNCTION IF EXISTS notify_change();
DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS stocks;

-- ── Stocks table (the "market") ──────────────────────────────
-- Admin owns these. Each stock has a total available quantity.
CREATE TABLE stocks (
    id            SERIAL PRIMARY KEY,
    symbol        VARCHAR(20)   NOT NULL UNIQUE,
    company_name  VARCHAR(100)  NOT NULL,
    last_price    DECIMAL(10,2) NOT NULL DEFAULT 0,
    available_qty INTEGER       NOT NULL DEFAULT 0 CHECK (available_qty >= 0),
    updated_at    TIMESTAMP     DEFAULT NOW()
);

-- ── Orders table ─────────────────────────────────────────────
-- Clients place BUY or SELL orders. Orders stay PENDING until matched.
CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    placed_by     VARCHAR(50)   NOT NULL,            -- client_1 .. client_5
    stock_symbol  VARCHAR(20)   NOT NULL REFERENCES stocks(symbol),
    order_type    VARCHAR(10)   NOT NULL CHECK (order_type IN ('BUY','SELL')),
    quantity      INTEGER       NOT NULL CHECK (quantity > 0),
    price         DECIMAL(10,2) NOT NULL CHECK (price > 0),
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','matched','rejected','cancelled')),
    updated_at    TIMESTAMP     DEFAULT NOW()
);

-- ── Trades table ─────────────────────────────────────────────
-- When a BUY and SELL order are matched, a trade is recorded.
CREATE TABLE trades (
    id            SERIAL PRIMARY KEY,
    buy_order_id  INTEGER       NOT NULL REFERENCES orders(id),
    sell_order_id INTEGER       NOT NULL REFERENCES orders(id),
    stock_symbol  VARCHAR(20)   NOT NULL,
    quantity      INTEGER       NOT NULL,
    price         DECIMAL(10,2) NOT NULL,
    executed_at   TIMESTAMP     DEFAULT NOW()
);

-- ── Universal notify function ─────────────────────────────────
-- One function handles all three tables. Sends table name + op + row.
CREATE OR REPLACE FUNCTION notify_change()
RETURNS trigger AS $$
DECLARE
    payload JSON;
    row_data JSON;
BEGIN
    IF TG_OP = 'DELETE' THEN
        row_data = row_to_json(OLD);
    ELSE
        row_data = row_to_json(NEW);
    END IF;

    payload = json_build_object(
        'table',     TG_TABLE_NAME,
        'operation', TG_OP,
        'data',      row_data
    );

    PERFORM pg_notify('exchange_channel', payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── Attach trigger to all 3 tables ───────────────────────────
CREATE TRIGGER orders_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_change();

CREATE TRIGGER stocks_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON stocks
FOR EACH ROW EXECUTE FUNCTION notify_change();

CREATE TRIGGER trades_notify_trigger
AFTER INSERT OR UPDATE OR DELETE ON trades
FOR EACH ROW EXECUTE FUNCTION notify_change();

-- ── Seed: Initial stock inventory ────────────────────────────
INSERT INTO stocks (symbol, company_name, last_price, available_qty) VALUES
    ('HDFCBANK',  'HDFC Bank Ltd.',           1645.50,  500),
    ('RELIANCE',  'Reliance Industries Ltd.', 2890.75,  300),
    ('TCS',       'Tata Consultancy Services', 3920.00, 200),
    ('INFY',      'Infosys Ltd.',              1478.25,  400),
    ('WIPRO',     'Wipro Ltd.',                 478.60,  600),
    ('SBIN',      'State Bank of India',        812.30,  750),
    ('ICICIBANK', 'ICICI Bank Ltd.',           1102.45,  450),
    ('BAJFINANCE','Bajaj Finance Ltd.',        6745.00,  150);