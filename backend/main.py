import asyncio
import json
from contextlib import asynccontextmanager
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from database import listen_for_changes, get_connection


# ── Connected WebSocket clients ──────────────────────────────────────────────

class ConnectionManager:
    """Tracks all active WebSocket connections and broadcasts messages to them."""

    def __init__(self):
        self.active_connections: Set[WebSocket] = set() #this creates empty set for storing  websockets for the n number of clients (c1,wbs1)

    async def connect(self, websocket: WebSocket):
        await websocket.accept() # accepts a websocket connection
        self.active_connections.add(websocket) # adds client to the active client list
        print(f"Client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        print(f"Client disconnected. Total clients: {len(self.active_connections)}") # disconnect

    async def broadcast(self, message: str):
        """Send a message to every connected client."""
        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.add(connection)
        # Clean up any broken connections
        for conn in disconnected:
            self.active_connections.discard(conn)


manager = ConnectionManager()


# ── PostgreSQL LISTEN callback ────────────────────────────────────────────────

def on_db_change(connection, pid, channel, payload):
    """
    Called by asyncpg whenever PostgreSQL fires pg_notify() on 'orders_channel'.
    Schedules a broadcast to all WebSocket clients.
    """
    print(f"DB change received: {payload}")
    asyncio.ensure_future(manager.broadcast(payload))


# ── App Lifespan (startup / shutdown) ────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: open the LISTEN connection
    listen_conn = await listen_for_changes(on_db_change)
    app.state.listen_conn = listen_conn
    yield  # function pauses , everything before this was startup , after this is shutdown , it proceeds from yield when you do ctrl c ...
    # Shutdown: close the connection cleanly
    await listen_conn.close()
    print("Database listener closed.")


# ── FastAPI App ───────────────────────────────────────────────────────────────

app = FastAPI(title="Real-Time Orders API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"], #allow requests from any domain like methods get post patch delete 
    allow_headers=["*"],
)


# ── REST Endpoints ────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"message": "Real-Time Orders Server is running."}


@app.get("/orders")
async def get_orders():
    """Fetch all current orders from the database."""
    conn = await get_connection()
    try:
        rows = await conn.fetch("SELECT * FROM orders ORDER BY updated_at DESC")
        return [dict(row) for row in rows]  # converts the database rows into JSON
    finally:
        await conn.close()


@app.post("/orders")
async def create_order(order: dict):
    """Insert a new order — triggers the DB notification automatically."""
    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO orders (customer_name, product_name, status)
            VALUES ($1, $2, $3)
            RETURNING *
            """,
            order["customer_name"],
            order["product_name"],
            order.get("status", "pending"),
        )
        return dict(row)
    finally:
        await conn.close()


@app.patch("/orders/{order_id}")
async def update_order_status(order_id: int, body: dict):
    """Update an order's status — triggers the DB notification automatically."""
    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            """
            UPDATE orders
            SET status = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING *
            """,
            body["status"],
            order_id,
        )
        if row is None:
            return {"error": "Order not found"}
        return dict(row)
    finally:
        await conn.close()


@app.delete("/orders/{order_id}")
async def delete_order(order_id: int):
    """Delete an order — triggers the DB notification automatically."""
    conn = await get_connection()
    try:
        await conn.execute("DELETE FROM orders WHERE id = $1", order_id)
        return {"message": f"Order {order_id} deleted"}
    finally:
        await conn.close()


# ── WebSocket Endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Clients connect here. They stay connected and receive every DB change
    as a JSON message the moment it happens.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive; we don't expect messages from the client
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
