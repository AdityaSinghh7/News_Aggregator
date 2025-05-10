import asyncio
import json
import os
from sqlalchemy.ext.asyncio import AsyncConnection
from backend.db import engine
from sqlalchemy import text

# Paths to JSON files
SOURCE_FILES = [
    os.path.join(os.path.dirname(__file__), '../sources/india_news_sources.json'),
    os.path.join(os.path.dirname(__file__), '../sources/pakistan_news_sources.json'),
    os.path.join(os.path.dirname(__file__), '../sources/international_news_sources.json'),
    os.path.join(os.path.dirname(__file__), '../sources/government_news_sources.json'),
]

# Map JSON keys to DB columns (for government sources, handle missing fields)
SCHEMA_FIELDS = [
    'name', 'website', 'type', 'rss', 'language', 'region', 'country',
    'source_category', 'government_entity', 'reliability_score', 'scraping_method', 'notes'
]



async def upsert_news_source(conn: AsyncConnection, source: dict):
    placeholders = ', '.join([f':{field}' for field in SCHEMA_FIELDS])
    update_fields = ', '.join([f"{field}=EXCLUDED.{field}" for field in SCHEMA_FIELDS if field not in ['name', 'website']])
    stmt = text(f"""
    INSERT INTO news_sources ({', '.join(SCHEMA_FIELDS)})
    VALUES ({placeholders})
    ON CONFLICT (name, website) DO UPDATE SET
    {update_fields},
    updated_at = now()
    """)

    await conn.execute(stmt, {field: source.get(field) for field in SCHEMA_FIELDS})

async def main():
    async with engine.begin() as conn:
        for file_path in SOURCE_FILES:
            with open(file_path, 'r') as f:
                sources = json.load(f)
                for source in sources:
                    # For government sources, handle 'scraping_requirements' -> 'scraping_method'
                    if 'scraping_requirements' in source and 'scraping_method' not in source:
                        source['scraping_method'] = source['scraping_requirements']
                    await upsert_news_source(conn, source)
    print('Database population complete.')

if __name__ == '__main__':
    asyncio.run(main())
