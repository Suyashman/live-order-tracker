import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


async def get_connection():
    return await asyncpg.connect(DATABASE_URL)


async def listen_for_changes(callback):
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.add_listener("orders_channel", callback)
    print("Listening on 'orders_channel'...")
    return conn
