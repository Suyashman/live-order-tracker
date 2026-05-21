CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    customer_name   VARCHAR(255) NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    status          VARCHAR(50) CHECK (status IN ('pending', 'shipped', 'delivered')) DEFAULT 'pending',
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Trigger function: fires on INSERT, UPDATE, DELETE
CREATE OR REPLACE FUNCTION notify_order_change()
RETURNS trigger AS $$
DECLARE
    payload JSON;
BEGIN
    IF TG_OP = 'DELETE' THEN
        payload = json_build_object(
            'operation', TG_OP,
            'data', row_to_json(OLD)
        );
    ELSE
        payload = json_build_object(
            'operation', TG_OP,
            'data', row_to_json(NEW)
        );
    END IF;

    PERFORM pg_notify('orders_channel', payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS orders_change_trigger ON orders;
CREATE TRIGGER orders_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_order_change();
