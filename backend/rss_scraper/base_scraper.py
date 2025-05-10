import asyncio
import logging
import feedparser
from datetime import datetime
from typing import List, Optional, Any
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy import text
from bs4 import BeautifulSoup
from newspaper import Article
from backend.db import engine
import concurrent.futures
from backend.rss_scraper.deduplication import article_exists
import json

logging.basicConfig(level=logging.INFO)

# Define keywords for filtering articles. This list can be updated or moved to a config.
KEYWORDS = ["india", "pakistan", "kashmir", "border", "military", "ceasefire", "diplomacy", "geopolitics", "ladakh", "loc"]

ARTICLE_FIELDS = [
    'source_id', 'title', 'link', 'image_url', 'summary', 'published_at', 'guid', 'raw_rss'
]

def clean_html(html_content: Optional[str]) -> Optional[str]:
    if not html_content:
        return None
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text().strip()

def extract_image_from_html(html_content: Optional[str]) -> Optional[str]:
    if not html_content:
        return None
    soup = BeautifulSoup(html_content, 'html.parser')
    img_tag = soup.find('img')
    if img_tag and img_tag.get('src'):
        return img_tag['src']
    return None



async def insert_article(conn, article: dict):
    # Make a copy of the article dict to avoid modifying the original
    article_copy = article.copy()
    
    # Convert raw_rss dict to JSON string if it exists
    if 'raw_rss' in article_copy and article_copy['raw_rss'] is not None:
        article_copy['raw_rss'] = json.dumps(article_copy['raw_rss'])
    
    placeholders = ', '.join([f':{field}' for field in ARTICLE_FIELDS])
    stmt = text(f"""
        INSERT INTO news_articles ({', '.join(ARTICLE_FIELDS)})
        VALUES ({placeholders})
        ON CONFLICT (link) DO NOTHING
    """)
    await conn.execute(stmt, {field: article_copy.get(field) for field in ARTICLE_FIELDS})

# Place this at the module level, not inside the class
async def fetch_news_sources(conn: AsyncConnection):
    query = text("""
        SELECT id, rss FROM news_sources WHERE rss IS NOT NULL AND rss != ''
    """)
    result = await conn.execute(query)
    return result.fetchall()

class BaseRSSScraper:
    # Shared thread pool for all instances
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)  # Increase as needed

    def __init__(self, engine, source_id, rss_url, keywords):
        self.engine = engine
        self.source_id = source_id
        self.rss_url = rss_url
        self.keywords = [k.lower() for k in keywords]
    
    def _extract_image_url(self, entry: Any) -> Optional[str]:
        image_url = None
        # 1. Try media_content (can be list or dict)
        media_content = entry.get('media_content')
        if media_content:
            if isinstance(media_content, list) and media_content:
                image_url = media_content[0].get('url')
            elif isinstance(media_content, dict):
                image_url = media_content.get('url')

        # 2. Try enclosures if not found
        if not image_url:
            enclosures = entry.get('enclosures')
            if enclosures and isinstance(enclosures, list) and enclosures:
                for enc in enclosures:
                    if enc.get('type', '').startswith('image'):
                        image_url = enc.get('href')
                        break
        
        # 3. Try parsing from description or content:encoded if still not found
        if not image_url:
            image_url = extract_image_from_html(entry.get('description'))
        if not image_url:
            image_url = extract_image_from_html(entry.get('content:encoded'))
            
        return image_url

    def _extract_summary(self, entry: Any) -> Optional[str]:
        summary_html = entry.get('summary') or entry.get('description') or entry.get('content:encoded')
        return clean_html(summary_html)

    def _has_keywords(self, title: Optional[str], summary_text: Optional[str]) -> bool:
        """
        Enhanced keyword logic:
        1. Article must mention BOTH 'india' and 'pakistan'
        2. OR contain the phrase 'operation sindoor'
        3. OR contain 'jammu' or 'kashmir'
        4. OR match any of the original keywords (fallback)
        """
        if not self.keywords:
            return True

        search_text = []
        if title: search_text.append(title.lower())
        if summary_text: search_text.append(summary_text.lower())
        combined_text = " ".join(search_text)

        if not combined_text:
            return False

        # 1. Both 'india' and 'pakistan'
        if 'india' in combined_text and 'pakistan' in combined_text:
            return True
        # 2. 'operation sindoor'
        if 'operation sindoor' in combined_text:
            return True
        # 3. 'jammu' or 'kashmir'
        if 'jammu' in combined_text or 'kashmir' in combined_text:
            return True
        # 4. Fallback: any of the original keywords
        return any(keyword in combined_text for keyword in self.keywords)

    async def _fetch_article_fallback(self, url: str) -> dict:
        """
        Use newspaper3k in a thread pool to extract summary and top image from the article page.
        Returns a dict with keys: 'summary', 'image_url'.
        """
        loop = asyncio.get_event_loop()
        def extract():
            article = Article(url)
            try:
                article.download(timeout=30)
                article.parse()
                article.nlp()  # For summary
                return {
                    'summary': article.summary if hasattr(article, 'summary') else None,
                    'image_url': article.top_image if hasattr(article, 'top_image') else None
                }
            except Exception:
                return {'summary': None, 'image_url': None}
        return await loop.run_in_executor(BaseRSSScraper.executor, extract)

    async def fetch_and_store(self):
        logging.info(f"Fetching RSS: {self.rss_url} for source ID: {self.source_id}")
        try:
            feed = feedparser.parse(self.rss_url)
            if feed.bozo:
                error_details = str(feed.bozo_exception) if hasattr(feed, 'bozo_exception') else "Unknown parsing error"
                logging.warning(f"Non-well-formed RSS feed for {self.rss_url}: {error_details}")
        except Exception as e:
            error_type = type(e).__name__
            error_message = str(e)
            logging.error(f"Error parsing RSS feed {self.rss_url}: {error_type} - {error_message}")
            # Reraise to propagate to the main task error handling
            raise
        
        # Check if feed contains entries
        if not hasattr(feed, 'entries') or len(feed.entries) == 0:
            error_msg = f"No entries found in RSS feed {self.rss_url} for source ID: {self.source_id}"
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        for entry in feed.entries:
            title = entry.get('title')
            link = entry.get('link')

            if not title or not link:
                logging.warning(f"Skipping entry with missing title or link from {self.rss_url}")
                continue

            guid = entry.get('id') or entry.get('guid') # Use 'id' as primary guid, fallback to 'guid'
            summary_text = self._extract_summary(entry)
            image_url = self._extract_image_url(entry)

            # Fallback logic: If summary or image is missing, try to fetch from article page
            if not summary_text or not image_url:
                fallback_data = await self._fetch_article_fallback(link)
                if not summary_text:
                    summary_text = fallback_data.get('summary')
                if not image_url:
                    image_url = fallback_data.get('image_url')

            # Keyword Filtering
            logging.info(f"Checking article: {title} ({link})")
            if not self._has_keywords(title, summary_text):
                logging.info(f"Filtered out by keywords: {title}")
                continue
            logging.info(f"Passed keyword filter: {title}")

            published = entry.get('published') or entry.get('updated')
            published_at = None
            if published:
                try:
                    # feedparser.struct_time to datetime
                    dt_struct = entry.get('published_parsed') or entry.get('updated_parsed')
                    if dt_struct:
                        published_at = datetime(*dt_struct[:6])
                except Exception as e:
                    logging.warning(f"Could not parse date for {link}: {published}, Error: {e}")
                    published_at = None

            # Deduplication check before attempting to insert
            try:
                async with self.engine.begin() as conn:
                    if await article_exists(conn, link, guid):
                        logging.info(f"Duplicate found, skipping: {link}")
                        continue
            except Exception as e:
                logging.warning(f"Error checking for duplicate article {link}: {e}")
                # Continue anyway, worst case we'll hit the ON CONFLICT

            # Ensure raw_rss is JSON serializable
            raw_rss_data = dict(entry) # Convert feedparser.FeedParserDict to dict
            # Remove non-serializable items like _کسانی (if any)
            for key in list(raw_rss_data.keys()):
                if key.startswith('_'):
                    del raw_rss_data[key]

            article = {
                'source_id': self.source_id,
                'title': title,
                'link': link,
                'image_url': image_url,
                'summary': summary_text,
                'published_at': published_at,
                'guid': guid,
                'raw_rss': raw_rss_data, # Storing the dict version
            }
            try:
                logging.info(f"Inserting article: {title}")
                async with self.engine.begin() as conn:
                    await insert_article(conn, article)
                logging.info(f"Inserted article: {title[:50]}...")
                # return  # <-- Break after first successful insert
            except Exception as e:
                logging.error(f"Error inserting article {link}: {e}")

async def main():
    async with engine.begin() as conn:
        sources = await fetch_news_sources(conn)
        if not sources:
            logging.info("No news sources found to process.")
            return
        
        tasks = []
        for source_id, rss_url in sources:
            scraper = BaseRSSScraper(engine, source_id, rss_url, KEYWORDS)
            tasks.append(scraper.fetch_and_store())
        
        await asyncio.gather(*tasks)

    logging.info("RSS scraping complete.")

if __name__ == '__main__':
    # To see debug logs for keyword skipping, change level to DEBUG
    # logging.basicConfig(level=logging.DEBUG) 
    asyncio.run(main()) 