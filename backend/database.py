import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def get_connection():
    """Create and return a single asyncpg connection."""
    conn = await asyncpg.connect(DATABASE_URL)
    return conn


async def listen_for_changes(callback):
    """
    Opens a dedicated connection that LISTENs on 'orders_channel'.
    Whenever PostgreSQL fires pg_notify(), the callback is called with the payload.
    This connection stays open for the lifetime of the server.
    """
    conn = await asyncpg.connect(DATABASE_URL)

    await conn.add_listener("orders_channel", callback)

    print("Listening for database changes on 'orders_channel'...")

    return conn
