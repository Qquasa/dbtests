from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import URL, create_engine, text
from config import settings
import asyncio

sync_engien = create_engine(
    url=settings.DATABASE_URL_psycopg,
    echo=True,
    pool_size=5,
    max_overflow=10, #standart
)

async_engien = create_async_engine(
    url=settings.DATABASE_URL_asyncpg,
    echo=True,
    pool_size=5,
    max_overflow=10, #standart
)

with sync_engien.connect() as conn:
    res = conn.execute(text("SELECT VERSION()"))
    print(f'{res.first()=}')

async def get_123():
    async with sync_engien.connect() as conn:
        res = await conn.execute(text("SELECT VERSION()"))
        print(f'{res.first()=}')
        
asyncio.run(get_123)