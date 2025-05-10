import os
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import text
from backend.db import engine

DATABASE_URL = os.getenv("DATABASE_URL")

async def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database using asyncpg.
    """
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set.")
    return await engine.AsyncConnection(DATABASE_URL)

async def article_exists(conn: AsyncConnection, link: str, guid: Optional[str] = None) -> bool:
    """
    Check if an article already exists in the news_articles table by link or guid.
    Args:
        conn (AsyncConnection): SQLAlchemy async connection.
        link (str): The URL of the article.
        guid (Optional[str]): The RSS GUID of the article, if available.
    Returns:
        bool: True if the article exists, False otherwise.
    """
    if guid:
        query = text("""
            SELECT 1 FROM news_articles WHERE link = :link OR guid = :guid LIMIT 1;
        """)
        result = await conn.execute(query, {"link": link, "guid": guid})
    else:
        query = text("""
            SELECT 1 FROM news_articles WHERE link = :link LIMIT 1;
        """)
        result = await conn.execute(query, {"link": link})
    return result.first() is not None 