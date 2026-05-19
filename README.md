# Real-Time Order Notification System

> Backend assignment submission for **Apt (Atypical Technologies Pvt. Ltd.)** — a technology company building algo trading products for the Indian stock market.
>
> The assignment asked: *"Design and implement a system where clients automatically receive updates whenever data in the database changes — without polling."*

---

## The Problem

Most naive solutions to this problem use **polling** — the client repeatedly asks the server "anything new?" every few seconds. This is wasteful, introduces latency, and completely falls apart at scale. In a trading system where order status changes in milliseconds, polling is unacceptable.

The challenge is to build a system where the **database itself notifies the backend** when something changes, and the backend **immediately pushes** that change to every connected client.

---

## The Solution

PostgreSQL has a built-in pub/sub mechanism called `LISTEN`/`NOTIFY`. A trigger fires on every `INSERT`, `UPDATE`, or `DELETE` on the `orders` table and calls `pg_notify()` with the changed row as a JSON payload. The Python backend holds a single persistent connection that LISTENs on that channel — the moment any row changes, the backend is notified in the **same database transaction** and immediately broadcasts it over WebSocket to all connected clients.

```
Someone changes a row in the orders table
              ↓
PostgreSQL trigger fires automatically
              ↓
pg_notify() publishes JSON payload to 'orders_channel'
              ↓
Python backend receives it instantly (asyncpg LISTEN)
              ↓
WebSocket broadcasts to every connected browser tab
              ↓
Dashboard updates live — no refresh, no polling
```

Zero polling. Zero extra infrastructure. The entire chain from a SQL command to a live browser update completes in **under 100 milliseconds**.

---

## Why This Is Scalable

The assignment evaluation criteria specifically asked about scalability and design thinking. Here is how this system addresses that:

**No polling overhead**
Every polling-based system has a fundamental problem: resource usage scales with the number of clients, even when nothing is happening. With 1000 clients polling every second, that is 1000 requests per second returning empty responses. This system is purely event-driven — the backend only does work when something actually changes. Resource usage stays flat at idle regardless of how many clients are connected.

**Persistent WebSocket connections**
HTTP is request-response — the client always has to initiate. WebSockets maintain a persistent connection, so the server can push data to the client at any time with virtually no overhead. Once connected, there is no repeated handshake, no repeated headers, no repeated parsing. This is the same model used in real trading terminals.

**Database-level triggers instead of application-level events**
The trigger lives inside PostgreSQL itself, not in the application code. This means any change to the `orders` table — regardless of which service or tool caused it — will fire a notification. If a database admin runs a direct SQL update, clients still get notified. The system is not fragile to writes happening outside the application.

**Stateless backend**
The FastAPI server holds no order state of its own. It only listens to the database and forwards events. This means you can run multiple instances of the backend behind a load balancer. The only change needed to support this at scale is replacing `pg_notify` with Redis Pub/Sub or Kafka as the message bus — the WebSocket interface and the client code stay exactly the same.

**Concurrent connection handling**
FastAPI is built on Python's asyncio. The WebSocket broadcast and the database listener both run on the same async event loop without blocking each other. Hundreds of concurrent WebSocket connections can be served from a single process without spawning threads.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       PostgreSQL                             │
│                                                              │
│   orders table                                               │
│       └── Trigger: notify_order_change()                     │
│                 └── pg_notify('orders_channel', JSON)        │
└──────────────────────────┬───────────────────────────────────┘
                           │  LISTEN / NOTIFY
                           │  (single persistent connection, no polling)
┌──────────────────────────▼───────────────────────────────────┐
│                  Python Backend  (FastAPI)                   │
│                                                              │
│  asyncpg LISTEN  ──►  on_db_change()  ──►  WS broadcast     │
│                                                              │
│  REST endpoints: GET /orders  POST /orders  PATCH /orders    │
└──────────────────────────┬───────────────────────────────────┘
                           │  WebSocket  ws://localhost:8000/ws
┌──────────────────────────▼───────────────────────────────────┐
│               Browser Client  (index.html)                   │
│                                                              │
│  WebSocket.onmessage  ──►  Live orders dashboard             │
└──────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Why |
|---|---|---|
| Database | PostgreSQL | Native `LISTEN`/`NOTIFY` — no extra message broker needed |
| DB → Backend | asyncpg | Async PostgreSQL driver with first-class listener support |
| Backend | Python + FastAPI | Async-first framework, built-in WebSocket support |
| Backend → Client | WebSockets | Persistent connection, true server push, no polling |
| Frontend | HTML + Vanilla JS | No build step — easy to run and demonstrate |

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

The trigger fires on every `INSERT`, `UPDATE`, and `DELETE`:

```sql
CREATE OR REPLACE FUNCTION notify_order_change()
RETURNS trigger AS $$
DECLARE payload JSON;
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

CREATE TRIGGER orders_change_trigger
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION notify_order_change();
```

---

## Project Structure

```
realtime-orders/
│
├── backend/
│   ├── main.py              # FastAPI server — WebSocket endpoint + REST API
│   ├── database.py          # PostgreSQL connection and LISTEN setup
│   └── requirements.txt
│
├── client/
│   └── index.html           # Live orders dashboard
│
├── database/
│   └── setup.sql            # Table + trigger definition
│
├── .gitignore
└── README.md
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL 13+

### 1. Clone the repository

```bash
git clone https://github.com/Suyashman/live-order-tracker.git
cd realtime-orders
```

### 2. Set up the database

```bash
psql -U postgres -c "CREATE DATABASE orders_db;"
psql -U postgres -d orders_db -f database/setup.sql
```

### 3. Configure environment

Create `backend/.env`:

```env
DATABASE_URL=postgresql://postgres:yourpassword@localhost:5432/orders_db
```

### 4. Install dependencies and run

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

### 5. Open the client

Open `client/index.html` in your browser.

### 6. Test it

In a new terminal:

```bash
psql -U postgres -d orders_db
```

```sql
-- Insert a new order
INSERT INTO orders (customer_name, product_name, status)
VALUES ('Rahul Sharma', 'Laptop', 'pending');

-- Update status
UPDATE orders SET status = 'shipped' WHERE id = 1;

-- Delete
DELETE FROM orders WHERE id = 1;
```

Watch the dashboard update instantly after each command.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/orders` | Fetch all current orders |
| `POST` | `/orders` | Create a new order |
| `PATCH` | `/orders/{id}` | Update order status |
| `DELETE` | `/orders/{id}` | Delete an order |
| `WS` | `/ws` | WebSocket — receive all DB change events |

**WebSocket event format:**
```json
{
  "operation": "UPDATE",
  "data": {
    "id": 1,
    "customer_name": "Rahul Sharma",
    "product_name": "Laptop",
    "status": "shipped",
    "updated_at": "2026-05-20T15:47:22Z"
  }
}
```

---

## Additional Branches

This repository has two feature branches that extend the core system further, built to explore what a real-world application of this architecture looks like.

### `feature/trading-simulation`

Extends the system into a multi-user trading platform with 5 client profiles and an admin. Clients can place orders, change status directly from a dashboard, and all tabs update live via the same WebSocket architecture.

- Replaces the simple `orders` table with trading-specific fields (`placed_by`, `option_type`, `stock`, `quantity`, `price`)
- Order lifecycle: `pending → executed → settled` (or `rejected` / `cancelled`)
- Admin can execute, settle, or reject any order from a live dashboard
- Login page with profile picker — open multiple tabs, each with a different user

### `feature/stock-exchange`

Takes the trading simulation further and builds a realistic stock exchange with a price-based order matching engine.

- Three tables: `orders`, `stocks`, `trades` — all with triggers attached
- **Partial fill support** — if a BUY of 5 matches a SELL of 3, the trade executes for 3 and a new pending order of 2 is automatically created for the remainder
- Live order book showing aggregated bids and asks per price level with spread
- 8 seeded stocks (HDFCBANK, RELIANCE, TCS, INFY, WIPRO, SBIN, ICICIBANK, BAJFINANCE)
- Toast popup notifications when orders are placed, matched, rejected, or cancelled
- Concurrent-safe matching using `FOR UPDATE SKIP LOCKED` in PostgreSQL

Both branches use the same core architecture — PostgreSQL `LISTEN`/`NOTIFY` + WebSocket — demonstrating that the pattern scales from a simple notification system to a full order matching engine without changing the fundamental design.

---

## Author

**Suyashman**
Submitted for Apt (Atypical Technologies Pvt. Ltd.) Backend Internship Assignment — May 2026
