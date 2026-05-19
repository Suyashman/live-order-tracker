import asyncio
from contextlib import asynccontextmanager
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from database import listen_for_changes, get_connection


# ── WebSocket Manager ─────────────────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        print(f"[WS] Client connected — total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        print(f"[WS] Client disconnected — total: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        dead = set()
        for ws in self.active_connections:
            try:
                await ws.send_text(message)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.active_connections.discard(ws)


manager = ConnectionManager()


# ── DB Notify Callback ────────────────────────────────────────────────────────

def on_db_change(connection, pid, channel, payload):
    print(f"[DB] Change received: {payload}")
    asyncio.ensure_future(manager.broadcast(payload))


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = await listen_for_changes(on_db_change)
    app.state.listen_conn = conn
    yield
    await conn.close()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="APT Real-Time Trading Orders", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic Models ───────────────────────────────────────────────────────────

class NewOrder(BaseModel):
    placed_by:   str
    option_type: str   # CALL or PUT
    stock:       str
    quantity:    int
    price:       float

class StatusUpdate(BaseModel):
    status: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"message": "APT Trading Orders API is running."}


@app.get("/orders")
async def get_all_orders():
    """Admin: fetch all orders."""
    conn = await get_connection()
    try:
        rows = await conn.fetch("SELECT * FROM orders ORDER BY updated_at DESC")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@app.get("/orders/{client_id}")
async def get_client_orders(client_id: str):
    """Client: fetch only their own orders."""
    conn = await get_connection()
    try:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE placed_by = $1 ORDER BY updated_at DESC",
            client_id
        )
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@app.post("/orders")
async def place_order(order: NewOrder):
    """Client places a new order."""
    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO orders (placed_by, option_type, stock, quantity, price, status)
            VALUES ($1, $2, $3, $4, $5, 'pending')
            RETURNING *
            """,
            order.placed_by,
            order.option_type.upper(),
            order.stock,
            order.quantity,
            order.price,
        )
        return dict(row)
    finally:
        await conn.close()


@app.patch("/orders/{order_id}/status")
async def update_status(order_id: int, body: StatusUpdate):
    """Admin or client updates an order status."""
    valid = {'pending', 'executed', 'settled', 'cancelled', 'rejected'}
    if body.status not in valid:
        return {"error": f"Invalid status. Must be one of: {valid}"}

    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            """
            UPDATE orders
            SET status = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING *
            """,
            body.status,
            order_id,
        )
        if row is None:
            return {"error": "Order not found"}
        return dict(row)
    finally:
        await conn.close()


@app.delete("/orders/{order_id}")
async def delete_order(order_id: int):
    conn = await get_connection()
    try:
        await conn.execute("DELETE FROM orders WHERE id = $1", order_id)
        return {"message": f"Order {order_id} deleted"}
    finally:
        await conn.close()


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
