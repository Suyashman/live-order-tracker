import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)


async def listen_for_changes(callback) -> asyncpg.Connection:
    """
    Opens a dedicated long-lived connection that LISTENs on 'exchange_channel'.
    PostgreSQL pushes a notification to this connection whenever any trigger fires.
    The callback receives the raw JSON payload.
    """
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.add_listener("exchange_channel", callback)
    print("[DB] Listening on 'exchange_channel'...")
    return conn