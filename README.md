# Real-Time Order Update System

A backend service that pushes live database changes to connected clients — no polling, no page refreshes.

Built for the **Apt (Atypical Technologies Pvt. Ltd.)** internship assignment.

---

## What This Project Does

Whenever a row in the `orders` table is inserted, updated, or deleted in the database, every connected client receives the update **instantly** and **automatically**.

This is the same pattern used in real-world trading platforms, where order status must be reflected in real-time across dashboards without any manual refresh.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                               │
│                                                                 │
│   orders table  ──►  Trigger Function  ──►  pg_notify()        │
└──────────────────────────────┬──────────────────────────────────┘
                               │  LISTEN / NOTIFY (event channel)
┌──────────────────────────────▼──────────────────────────────────┐
│                     Python Backend (FastAPI)                    │
│                                                                 │
│   asyncpg LISTEN  ──►  Parse Payload  ──►  WebSocket Broadcast │
└──────────────────────────────┬──────────────────────────────────┘
                               │  WebSocket (ws://)
┌──────────────────────────────▼──────────────────────────────────┐
│                    Browser Client (HTML/JS)                     │
│                                                                 │
│   WebSocket.onmessage  ──►  Update Live Orders Table           │
└─────────────────────────────────────────────────────────────────┘
```

**Flow in plain English:**

1. Someone inserts or updates a row in the `orders` table
2. A PostgreSQL trigger fires automatically and calls `pg_notify()` with the changed row as JSON
3. The Python backend (which is LISTENing on that channel) receives the notification instantly
4. The backend broadcasts the update over WebSocket to all connected browser clients
5. The browser updates the live orders table — no refresh needed

---

## Why This Approach?

### The Problem with Polling

The naive solution is to have the client ask the server every few seconds: *"anything new?"*. This is called **polling**, and it has serious problems:

- **Wasteful** — 99% of requests return nothing new
- **Delayed** — updates only arrive at the next poll interval
- **Doesn't scale** — 1000 clients × polling every second = 1000 requests/second for no reason

In algo trading, where order status can change in milliseconds, polling is completely unacceptable.

### Why PostgreSQL LISTEN/NOTIFY?

PostgreSQL has a built-in pub/sub mechanism:

- A **trigger** on the `orders` table fires on every INSERT, UPDATE, or DELETE
- The trigger calls `pg_notify('orders_channel', payload)` — publishing an event with the changed row as JSON
- The backend **LISTENs** on `orders_channel` and receives the event the moment it fires

This means:
- **Zero polling** — the DB itself pushes the event
- **No extra infrastructure** — no Kafka, no Redis, no message broker needed
- **Instant** — the backend is notified in the same transaction that changed the data
- **Reliable** — if the backend is connected, it will not miss a single event

### Why WebSockets?

WebSockets maintain a persistent, two-way connection between the server and the browser:

- Once connected, the server can **push** data to the client at any time
- No repeated HTTP requests from the client
- Low overhead — ideal for high-frequency updates like order status in a trading system

Compare this to HTTP long-polling or Server-Sent Events — WebSockets are the most efficient choice when bidirectional, low-latency communication is needed.

### Why FastAPI?

- Native support for `async/await` — critical for handling concurrent WebSocket connections efficiently
- Clean and minimal — easy to read and extend
- `asyncpg` integrates seamlessly for async PostgreSQL connections

---

## Tech Stack

| Component | Technology | Reason |
|-----------|------------|--------|
| Database | PostgreSQL | Native LISTEN/NOTIFY support |
| Backend | Python + FastAPI | Async-first, WebSocket support |
| DB Connection | asyncpg | Async PostgreSQL driver |
| Client Push | WebSockets | Low-latency, persistent connection |
| Frontend | HTML + Vanilla JS | Simple, no build step needed |

---

## Project Structure

```
realtime-orders/
│
├── backend/
│   ├── main.py              # FastAPI app — WebSocket server + DB listener
│   ├── database.py          # PostgreSQL connection and LISTEN setup
│   └── requirements.txt     # Python dependencies
│
├── database/
│   └── setup.sql            # Creates orders table, trigger function, and trigger
│
├── client/
│   └── index.html           # Browser client — live orders dashboard
│
└── README.md
```

---

## Prerequisites

- Python 3.10+
- PostgreSQL 13+
- pip

---

## Setup & Running

### Step 1 — Clone the Repository

```bash
git clone https://github.com/your-username/realtime-orders.git
cd realtime-orders
```

### Step 2 — Set Up PostgreSQL

Make sure PostgreSQL is running. Then create the database and set up the table and trigger:

```bash
psql -U postgres -c "CREATE DATABASE orders_db;"
psql -U postgres -d orders_db -f db/setup.sql
```

This will:
- Create the `orders` table
- Create a trigger function that calls `pg_notify()` on any change
- Attach the trigger to the table

### Step 3 — Install Python Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### Step 4 — Configure Environment

Create a `.env` file in the `backend/` folder:

```env
DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/orders_db
```

Replace `yourpassword` with your actual PostgreSQL password.

### Step 5 — Start the Backend

```bash
cd backend
uvicorn main:app --reload
```

The server will start at `http://localhost:8000`.

### Step 6 — Open the Client

Open `client/index.html` directly in your browser. You will see a live orders dashboard.

### Step 7 — Test It

In a new terminal, connect to the database and insert or update a row:

```bash
psql -U postgres -d orders_db
```

```sql
-- Insert a new order
INSERT INTO orders (customer_name, product_name, status)
VALUES ('Rahul Sharma', 'NIFTY Call Option', 'pending');

-- Update status
UPDATE orders SET status = 'shipped' WHERE customer_name = 'Rahul Sharma';

-- Delete an order
DELETE FROM orders WHERE customer_name = 'Rahul Sharma';
```

Watch the browser dashboard update **instantly** after each command — no refresh needed.

---

## Database Schema

```sql
CREATE TABLE orders (
    id             SERIAL PRIMARY KEY,
    customer_name  VARCHAR(255) NOT NULL,
    product_name   VARCHAR(255) NOT NULL,
    status         VARCHAR(50) CHECK (status IN ('pending', 'shipped', 'delivered')),
    updated_at     TIMESTAMP DEFAULT NOW()
);
```

---

## How the Trigger Works

```sql
-- Trigger function: fires on INSERT, UPDATE, DELETE
CREATE OR REPLACE FUNCTION notify_order_change()
RETURNS trigger AS $$
DECLARE
    payload JSON;
BEGIN
    -- Build JSON payload with event type and row data
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

    -- Publish to the 'orders_channel'
    PERFORM pg_notify('orders_channel', payload::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach trigger to orders table
CREATE TRIGGER orders_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_order_change();
```

Every time a row changes, PostgreSQL automatically calls this function, which publishes a JSON event to `orders_channel`. The Python backend receives it immediately.

---

## Scalability Considerations

This architecture is designed with growth in mind:

- **Multiple clients** — the WebSocket server can handle many concurrent connections; every connected client receives every update
- **No polling overhead** — the system is event-driven; resource usage stays flat regardless of how often clients check in
- **Stateless backend** — the FastAPI server does not store any order state itself; it purely forwards events from the DB to clients
- **Horizontal scaling** — for higher loads, the LISTEN/NOTIFY pattern can be replaced with a message broker like Redis Pub/Sub or Kafka, with minimal changes to the backend interface

For a production trading system, the next steps would be:

1. Add authentication to the WebSocket endpoint so only authorized clients connect
2. Filter events per client — traders should only see their own orders
3. Add a Redis layer between the DB and the backend for fan-out to multiple backend instances
4. Persist missed events so clients that reconnect can catch up

---

## Key Design Decisions Summary

| Decision | Alternative Considered | Why This Was Better |
|----------|----------------------|---------------------|
| PostgreSQL LISTEN/NOTIFY | Polling the DB every second | Event-driven, zero latency, no wasted queries |
| WebSockets | HTTP long-polling | Persistent connection, true server push |
| asyncpg | psycopg2 | Native async support, no blocking |
| FastAPI | Flask | Async-first, built-in WebSocket support |
| Trigger in DB | App-level event on write | Works for any DB client, not just this app |

---

## Author

Suyash Sunam 
Submitted for Apt (Atypical Technologies Pvt. Ltd.) Backend Internship Assignment
