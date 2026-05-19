"""
APT Stock Exchange — Backend
============================
FastAPI server that:
  1. LISTENs to PostgreSQL via asyncpg (no polling)
  2. Broadcasts all DB changes over WebSocket to all connected clients
  3. Runs a simple price-based order matching engine on every new order
  4. Exposes REST endpoints for stocks, orders, trades
"""

import asyncio
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import get_connection, listen_for_changes


# ── WebSocket Manager ─────────────────────────────────────────────────────────

class ConnectionManager:
    """Tracks all active WebSocket connections and broadcasts to all of them."""

    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)
        print(f"[WS] +1 client — total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)
        print(f"[WS] -1 client — total: {len(self.active)}")

    async def broadcast(self, payload: str):
        dead = set()
        for ws in self.active:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.active.discard(ws)


manager = ConnectionManager()


# ── Order Matching Engine ─────────────────────────────────────────────────────

async def match_orders(new_order_id: int, conn: asyncpg.Connection = None):
    """
    Called after every new BUY or SELL order is inserted.
    Finds a compatible opposing order (price-compatible, same stock, pending)
    and creates a trade if a match is found.

    Matching rules:
      - BUY order matches the cheapest available SELL at or below buy price
      - SELL order matches the highest available BUY at or above sell price
    """
    close_conn = False
    if conn is None:
        conn = await get_connection()
        close_conn = True

    try:
        async with conn.transaction():
            order = await conn.fetchrow(
                "SELECT * FROM orders WHERE id = $1 AND status = 'pending' FOR UPDATE",
                new_order_id
            )
            if not order:
                return

            if order['order_type'] == 'BUY':
                # Find cheapest SELL at or below this buy price
                match = await conn.fetchrow(
                    """
                    SELECT * FROM orders
                    WHERE stock_symbol = $1
                      AND order_type   = 'SELL'
                      AND status       = 'pending'
                      AND price       <= $2
                      AND placed_by   != $3
                    ORDER BY price ASC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    order['stock_symbol'], order['price'], order['placed_by']
                )
            else:
                # Find highest BUY at or above this sell price
                match = await conn.fetchrow(
                    """
                    SELECT * FROM orders
                    WHERE stock_symbol = $1
                      AND order_type   = 'BUY'
                      AND status       = 'pending'
                      AND price       >= $2
                      AND placed_by   != $3
                    ORDER BY price DESC, id ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    order['stock_symbol'], order['price'], order['placed_by']
                )

            if not match:
                return  # No match yet — stays pending

            # Determine trade price (price of the older resting order)
            trade_price = match['price']
            trade_qty   = min(order['quantity'], match['quantity'])

            buy_id  = new_order_id if order['order_type'] == 'BUY' else match['id']
            sell_id = match['id']  if order['order_type'] == 'BUY' else new_order_id

            # Mark both orders as matched
            await conn.execute(
                "UPDATE orders SET status='matched', updated_at=NOW() WHERE id = ANY($1::int[])",
                [order['id'], match['id']]
            )

            # Record the trade
            await conn.execute(
                """
                INSERT INTO trades (buy_order_id, sell_order_id, stock_symbol, quantity, price)
                VALUES ($1, $2, $3, $4, $5)
                """,
                buy_id, sell_id, order['stock_symbol'], trade_qty, trade_price
            )

            # Update stock last price
            await conn.execute(
                "UPDATE stocks SET last_price=$1, updated_at=NOW() WHERE symbol=$2",
                trade_price, order['stock_symbol']
            )

    finally:
        if close_conn:
            await conn.close()


# ── PostgreSQL Notify Callback ────────────────────────────────────────────────

def on_db_change(connection, pid, channel, payload):
    """
    Called by asyncpg the moment PostgreSQL fires pg_notify().
    Schedules a WebSocket broadcast to all connected clients.
    Also triggers the matching engine for new pending orders.
    """
    print(f"[DB] Notify: {payload[:120]}")
    asyncio.ensure_future(manager.broadcast(payload))

    # Kick off matching engine for new orders
    try:
        evt = json.loads(payload)
        if (evt.get('table') == 'orders'
                and evt.get('operation') == 'INSERT'
                and evt['data'].get('status') == 'pending'):
            asyncio.ensure_future(match_orders(evt['data']['id']))
    except Exception as e:
        print(f"[Matcher] Error: {e}")


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = await listen_for_changes(on_db_change)
    app.state.listen_conn = conn
    yield
    await conn.close()
    print("[DB] Listener closed.")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="APT Stock Exchange API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic Models ───────────────────────────────────────────────────────────

class NewOrder(BaseModel):
    placed_by:    str
    stock_symbol: str
    order_type:   str    # BUY or SELL
    quantity:     int
    price:        float

class StatusUpdate(BaseModel):
    status: str


# ── Stock Endpoints ───────────────────────────────────────────────────────────

@app.get("/stocks")
async def get_stocks():
    conn = await get_connection()
    try:
        rows = await conn.fetch("SELECT * FROM stocks ORDER BY symbol")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


# ── Order Endpoints ───────────────────────────────────────────────────────────

@app.get("/orders")
async def get_all_orders():
    """Admin: all orders."""
    conn = await get_connection()
    try:
        rows = await conn.fetch("SELECT * FROM orders ORDER BY updated_at DESC")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@app.get("/orders/{client_id}")
async def get_client_orders(client_id: str):
    """Client: their own orders only."""
    conn = await get_connection()
    try:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE placed_by=$1 ORDER BY updated_at DESC",
            client_id
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@app.post("/orders")
async def place_order(order: NewOrder):
    """Place a new BUY or SELL order. Matching engine runs automatically."""
    if order.order_type.upper() not in ('BUY', 'SELL'):
        return {"error": "order_type must be BUY or SELL"}
    if order.quantity < 1:
        return {"error": "quantity must be >= 1"}
    if order.price <= 0:
        return {"error": "price must be > 0"}

    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO orders (placed_by, stock_symbol, order_type, quantity, price, status)
            VALUES ($1, $2, $3, $4, $5, 'pending')
            RETURNING *
            """,
            order.placed_by,
            order.stock_symbol.upper(),
            order.order_type.upper(),
            order.quantity,
            order.price,
        )
        return dict(row)
    finally:
        await conn.close()


@app.patch("/orders/{order_id}/status")
async def update_order_status(order_id: int, body: StatusUpdate):
    """Admin: reject or cancel an order."""
    allowed = {'rejected', 'cancelled'}
    if body.status not in allowed:
        return {"error": f"Admin can only set: {allowed}"}

    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            "UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2 AND status='pending' RETURNING *",
            body.status, order_id
        )
        if not row:
            return {"error": "Order not found or not pending"}
        return dict(row)
    finally:
        await conn.close()


# ── Trade Endpoints ───────────────────────────────────────────────────────────

@app.get("/trades")
async def get_trades():
    conn = await get_connection()
    try:
        rows = await conn.fetch("SELECT * FROM trades ORDER BY executed_at DESC LIMIT 50")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


# ── Market Depth (Order Book) ─────────────────────────────────────────────────

@app.get("/orderbook/{symbol}")
async def get_order_book(symbol: str):
    """Returns aggregated pending BUY and SELL orders for a stock."""
    conn = await get_connection()
    try:
        buys = await conn.fetch(
            """
            SELECT price, SUM(quantity) as total_qty, COUNT(*) as num_orders
            FROM orders
            WHERE stock_symbol=$1 AND order_type='BUY' AND status='pending'
            GROUP BY price ORDER BY price DESC LIMIT 10
            """, symbol.upper()
        )
        sells = await conn.fetch(
            """
            SELECT price, SUM(quantity) as total_qty, COUNT(*) as num_orders
            FROM orders
            WHERE stock_symbol=$1 AND order_type='SELL' AND status='pending'
            GROUP BY price ORDER BY price ASC LIMIT 10
            """, symbol.upper()
        )
        return {
            "symbol": symbol.upper(),
            "bids": [dict(r) for r in buys],
            "asks": [dict(r) for r in sells],
        }
    finally:
        await conn.close()


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()   # Keep alive; client can send pings
    except WebSocketDisconnect:
        manager.disconnect(ws)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "APT Stock Exchange is running"}