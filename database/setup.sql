-- Drop existing objects if re-running
DROP TRIGGER IF EXISTS orders_change_trigger ON orders;
DROP FUNCTION IF EXISTS notify_order_change();
DROP TABLE IF EXISTS orders;

-- Create orders table with trading fields
CREATE TABLE orders (
    id            SERIAL PRIMARY KEY,
    placed_by     VARCHAR(50)    NOT NULL,
    option_type   VARCHAR(10)    NOT NULL CHECK (option_type IN ('CALL', 'PUT')),
    stock         VARCHAR(50)    NOT NULL,
    quantity      INTEGER        NOT NULL CHECK (quantity > 0),
    price         DECIMAL(10,2)  NOT NULL CHECK (price > 0),
    status        VARCHAR(20)    NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','executed','settled','cancelled','rejected')),
    updated_at    TIMESTAMP      DEFAULT NOW()
);

-- Trigger function
CREATE OR REPLACE FUNCTION notify_order_change()
RETURNS trigger AS $$
DECLARE
    payload JSON;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload = json_build_object('operation', TG_OP, 'data', row_to_json(OLD));
    ELSE
        payload = json_build_object('operation', TG_OP, 'data', row_to_json(NEW));
    END IF;

    PERFORM pg_notify('orders_channel', payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to orders table
CREATE TRIGGER orders_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_order_change();
