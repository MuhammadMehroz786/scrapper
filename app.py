#!/usr/bin/env python3
"""
Nisbets Scraper - Railway App
Automatic product scraping with web dashboard
"""

import os
import json
import re
import time
import random
import threading
from datetime import datetime
from urllib.parse import urljoin

import cloudscraper
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template_string
from apscheduler.schedulers.background import BackgroundScheduler

# Configuration
DATA_DIR = os.environ.get('DATA_DIR', '/app/data')
APP_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))  # Products per run
SCRAPE_INTERVAL = int(os.environ.get('SCRAPE_INTERVAL', 30))  # Minutes between runs
AUTO_START = os.environ.get('AUTO_START', 'true').lower() == 'true'

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'images'), exist_ok=True)

app = Flask(__name__)

# Global state
scraper_status = {
    'running': False,
    'last_run': None,
    'products_scraped': 0,
    'total_urls': 0,
    'current_index': 0,
    'failed_count': 0,
    'current_product': None,
    'error': None
}


class NisbetsScraper:
    def __init__(self):
        self.base_url = "https://www.nisbets.co.uk"
        self.scraper = None
        self.products = []
        self.failed_urls = []
        self.progress_file = os.path.join(DATA_DIR, 'progress.json')
        self.output_file = os.path.join(DATA_DIR, 'products.json')
        self.urls_file = os.path.join(DATA_DIR, 'product_urls.txt')
        self.images_dir = os.path.join(DATA_DIR, 'images')
        self.last_scraped_index = 0

    def init_scraper(self):
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'linux', 'desktop': True},
            delay=5
        )
        self.scraper.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
        })

    def random_delay(self, min_sec=1, max_sec=3):
        time.sleep(random.uniform(min_sec, max_sec))

    def fetch_page(self, url, retries=3):
        for attempt in range(retries):
            try:
                response = self.scraper.get(url, timeout=30)
                if response.status_code == 200:
                    return response.text
                elif response.status_code == 404:
                    return None
                else:
                    self.random_delay(3, 6)
            except Exception as e:
                self.random_delay(3, 6)
        return None

    def download_image(self, url, sku, index):
        try:
            ext = '.jpg'
            if '.png' in url.lower():
                ext = '.png'

            filename = f"{sku}_{index}{ext}"
            filepath = os.path.join(self.images_dir, filename)

            if os.path.exists(filepath):
                return filepath

            response = self.scraper.get(url, timeout=30)
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                return filepath
        except:
            pass
        return None

    def extract_product(self, html, url):
        product = {
            'title': '',
            'body_html': '',
            'vendor': 'Nisbets',
            'product_type': '',
            'tags': [],
            'status': 'draft',
            'variants': [],
            'images': [],
            'metafields': [],
            'source_url': url,
            'source_sku': '',
            'source_price': '',
            'scraped_at': datetime.now().isoformat()
        }

        try:
            soup = BeautifulSoup(html, 'lxml')

            # SKU from URL
            url_match = re.search(r'/([a-zA-Z]{1,4}\d{2,6})$', url)
            if url_match:
                product['source_sku'] = url_match.group(1).upper()

            # Title
            title_elem = soup.select_one('h1')
            if title_elem:
                product['title'] = title_elem.get_text(strip=True)

            # Price
            price = None
            for selector in ['.product-price', '.price', '[data-price]']:
                price_elem = soup.select_one(selector)
                if price_elem:
                    price_match = re.search(r'£([\d,]+\.?\d*)', price_elem.get_text())
                    if price_match:
                        price = price_match.group(1).replace(',', '')
                        product['source_price'] = price
                        break

            # Description
            for selector in ['.product-description', '.description', '#product-description']:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    product['body_html'] = str(desc_elem)
                    break

            # Brand
            for selector in ['.product-brand', '.brand-name', '[data-brand]']:
                brand_elem = soup.select_one(selector)
                if brand_elem:
                    product['vendor'] = brand_elem.get_text(strip=True)
                    break

            # Images
            images = []
            seen = set()
            for img in soup.find_all('img'):
                src = img.get('src') or img.get('data-src') or ''
                if 'prodimage' in src and 'media.nisbets.com' in src:
                    src = re.sub(r'/(small_new|medium|medium2_new|large_new)/', '/largezoom/', src)
                    base = re.sub(r'.*/([^/]+)\.(jpg|png).*', r'\1', src.lower())
                    if base not in seen:
                        seen.add(base)
                        # Download image
                        local = self.download_image(src, product['source_sku'], len(images)+1)
                        if local:
                            images.append({
                                'src': src,
                                'local_path': local,
                                'filename': os.path.basename(local)
                            })
            product['images'] = images[:10]

            # Variant - UK pricing
            product['variants'].append({
                'title': 'Default',
                'price': price or '0.00',
                'sku': product['source_sku'],
                'inventory_management': 'shopify',
                'inventory_policy': 'deny',
                'requires_shipping': True,
                'taxable': True,
                'weight_unit': 'kg',
                'currency': 'GBP'
            })

            # Add UK-specific metafields
            product['metafields'].append({
                'namespace': 'product',
                'key': 'currency',
                'value': 'GBP',
                'type': 'single_line_text_field'
            })
            product['metafields'].append({
                'namespace': 'product',
                'key': 'country_of_origin',
                'value': 'United Kingdom',
                'type': 'single_line_text_field'
            })

            # Tags - include UK source
            product['tags'].append(f"SKU:{product['source_sku']}")
            product['tags'].append('UK')
            product['tags'].append('Nisbets UK')
            product['tags'].append('GBP')

        except Exception as e:
            pass

        return product

    def load_urls(self):
        # Check data directory first
        if os.path.exists(self.urls_file):
            with open(self.urls_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]

        # Check bundled URLs file in app directory
        bundled_file = os.path.join(APP_DIR, 'product_urls.txt')
        if os.path.exists(bundled_file):
            with open(bundled_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]

        return []

    def load_progress(self):
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    self.last_scraped_index = data.get('last_index', 0)
                    self.failed_urls = data.get('failed_urls', [])
                    return True
            except:
                pass
        return False

    def save_progress(self, index):
        with open(self.progress_file, 'w') as f:
            json.dump({
                'last_index': index,
                'failed_urls': self.failed_urls,
                'timestamp': datetime.now().isoformat()
            }, f)

    def load_products(self):
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r') as f:
                    data = json.load(f)
                    self.products = data.get('products', [])
                    return True
            except:
                pass
        return False

    def save_products(self):
        with open(self.output_file, 'w') as f:
            json.dump({
                'info': {
                    'source': 'Nisbets UK',
                    'updated': datetime.now().isoformat(),
                    'total': len(self.products),
                    'failed': len(self.failed_urls)
                },
                'products': self.products,
                'failed_urls': self.failed_urls
            }, f, indent=2)

    def run_batch(self, batch_size=100):
        global scraper_status

        scraper_status['running'] = True
        scraper_status['error'] = None

        try:
            self.init_scraper()
            urls = self.load_urls()

            if not urls:
                scraper_status['error'] = 'No URLs found. Run URL scraper first.'
                scraper_status['running'] = False
                return

            self.load_products()
            self.load_progress()

            scraper_status['total_urls'] = len(urls)
            scraper_status['current_index'] = self.last_scraped_index
            scraper_status['products_scraped'] = len(self.products)

            start = self.last_scraped_index
            end = min(start + batch_size, len(urls))

            for i in range(start, end):
                url = urls[i]
                scraper_status['current_index'] = i + 1
                scraper_status['current_product'] = url

                html = self.fetch_page(url)
                if html:
                    product = self.extract_product(html, url)
                    if product.get('title'):
                        self.products.append(product)
                        scraper_status['products_scraped'] = len(self.products)
                else:
                    self.failed_urls.append(url)
                    scraper_status['failed_count'] = len(self.failed_urls)

                self.last_scraped_index = i + 1
                self.random_delay(1, 2)

                # Save every 25 products
                if (i + 1) % 25 == 0:
                    self.save_products()
                    self.save_progress(i + 1)

            self.save_products()
            self.save_progress(self.last_scraped_index)
            scraper_status['last_run'] = datetime.now().isoformat()

        except Exception as e:
            scraper_status['error'] = str(e)
        finally:
            scraper_status['running'] = False
            scraper_status['current_product'] = None


# URL Scraper (collects product URLs)
class URLScraper:
    def __init__(self):
        self.base_url = "https://www.nisbets.co.uk"
        self.scraper = None
        self.product_links = set()
        self.category_links = set()
        self.visited = set()
        self.urls_file = os.path.join(DATA_DIR, 'product_urls.txt')

    def init_scraper(self):
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'linux', 'desktop': True},
            delay=5
        )

    def is_product_url(self, url):
        if not url.startswith(self.base_url):
            return False
        path = url.split('?')[0]
        skip = ['/c/', '/cat/', '/login', '/basket', '/checkout', '/help', '/blog']
        if any(s in path.lower() for s in skip):
            return False
        return bool(re.search(r'/[a-zA-Z]{1,4}\d{2,6}$', path))

    def is_category_url(self, url):
        if not url.startswith(self.base_url):
            return False
        path = url.split('?')[0].lower()
        skip = ['/login', '/basket', '/checkout', '/account', '/help', '.pdf', '.jpg']
        if any(s in path for s in skip):
            return False
        patterns = ['/c/', '-equipment', '-supplies', 'catering', 'refrigeration']
        return any(p in path for p in patterns)

    def scrape_page(self, url):
        try:
            r = self.scraper.get(url, timeout=30)
            if r.status_code != 200:
                return
            soup = BeautifulSoup(r.text, 'lxml')
            for a in soup.find_all('a', href=True):
                full = urljoin(self.base_url, a['href']).split('?')[0].rstrip('/')
                if self.is_product_url(full):
                    self.product_links.add(full)
                elif self.is_category_url(full) and full not in self.visited:
                    self.category_links.add(full)
        except:
            pass

    def run(self, max_pages=500):
        global scraper_status
        scraper_status['running'] = True
        scraper_status['error'] = None

        try:
            self.init_scraper()
            self.scrape_page(self.base_url)

            pages = 0
            while self.category_links and pages < max_pages:
                url = self.category_links.pop()
                if url in self.visited:
                    continue
                self.visited.add(url)
                self.scrape_page(url)
                pages += 1
                scraper_status['current_product'] = f"URLs: {len(self.product_links)} | Pages: {pages}"
                time.sleep(random.uniform(1, 2))

                if pages % 50 == 0:
                    self.save_urls()

            self.save_urls()
            scraper_status['total_urls'] = len(self.product_links)
        except Exception as e:
            scraper_status['error'] = str(e)
        finally:
            scraper_status['running'] = False

    def save_urls(self):
        with open(self.urls_file, 'w') as f:
            for url in sorted(self.product_links):
                f.write(f"{url}\n")


# Scheduler
scheduler = BackgroundScheduler()
product_scraper = NisbetsScraper()
url_scraper = URLScraper()


def scheduled_scrape():
    if not scraper_status['running']:
        product_scraper.run_batch(BATCH_SIZE)


# Routes
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Nisbets Scraper Dashboard</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .status { padding: 20px; border-radius: 8px; margin: 20px 0; }
        .running { background: #e3f2fd; border: 1px solid #2196f3; }
        .idle { background: #e8f5e9; border: 1px solid #4caf50; }
        .error { background: #ffebee; border: 1px solid #f44336; }
        .stat { display: inline-block; margin: 10px 20px; text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #1976d2; }
        .stat-label { color: #666; }
        button { padding: 10px 20px; font-size: 16px; cursor: pointer; margin: 5px; }
        .btn-primary { background: #1976d2; color: white; border: none; border-radius: 4px; }
        .btn-secondary { background: #757575; color: white; border: none; border-radius: 4px; }
        pre { background: #f5f5f5; padding: 10px; overflow-x: auto; }
    </style>
</head>
<body>
    <h1>Nisbets Scraper Dashboard</h1>

    <div class="status {{ 'running' if status.running else ('error' if status.error else 'idle') }}">
        <h2>Status: {{ 'Running' if status.running else ('Error' if status.error else 'Idle') }}</h2>
        {% if status.error %}
        <p style="color: red;">{{ status.error }}</p>
        {% endif %}
        {% if status.current_product %}
        <p>Current: {{ status.current_product[:60] }}...</p>
        {% endif %}
    </div>

    <div style="text-align: center;">
        <div class="stat">
            <div class="stat-value">{{ status.products_scraped }}</div>
            <div class="stat-label">Products Scraped</div>
        </div>
        <div class="stat">
            <div class="stat-value">{{ status.current_index }} / {{ status.total_urls }}</div>
            <div class="stat-label">Progress</div>
        </div>
        <div class="stat">
            <div class="stat-value">{{ status.failed_count }}</div>
            <div class="stat-label">Failed</div>
        </div>
    </div>

    <div style="margin: 30px 0;">
        <form action="/start-urls" method="post" style="display:inline;">
            <button type="submit" class="btn-secondary" {{ 'disabled' if status.running else '' }}>
                Scrape URLs
            </button>
        </form>
        <form action="/start" method="post" style="display:inline;">
            <button type="submit" class="btn-primary" {{ 'disabled' if status.running else '' }}>
                Start Product Scraper
            </button>
        </form>
    </div>

    <p>Last run: {{ status.last_run or 'Never' }}</p>
    <p>Auto-scrape: Every {{ interval }} minutes ({{ batch }} products per batch)</p>
    <p><small>Page auto-refreshes every 10 seconds</small></p>
</body>
</html>
'''

@app.route('/')
def dashboard():
    return render_template_string(
        DASHBOARD_HTML,
        status=scraper_status,
        interval=SCRAPE_INTERVAL,
        batch=BATCH_SIZE
    )

@app.route('/status')
def status():
    return jsonify(scraper_status)

@app.route('/start', methods=['POST'])
def start_scraper():
    if not scraper_status['running']:
        thread = threading.Thread(target=product_scraper.run_batch, args=(BATCH_SIZE,))
        thread.start()
    return jsonify({'status': 'started'})

@app.route('/start-urls', methods=['POST'])
def start_url_scraper():
    if not scraper_status['running']:
        thread = threading.Thread(target=url_scraper.run, args=(1000,))
        thread.start()
    return jsonify({'status': 'started'})

@app.route('/products')
def get_products():
    output_file = os.path.join(DATA_DIR, 'products.json')
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            return jsonify(json.load(f))
    return jsonify({'products': []})

@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})


def auto_start_scraping():
    """Auto-start scraping after a short delay to let the app initialize"""
    time.sleep(5)  # Wait for app to start
    urls = product_scraper.load_urls()
    if urls and AUTO_START:
        print(f"Auto-starting scraper with {len(urls)} URLs...")
        scraper_status['total_urls'] = len(urls)
        product_scraper.run_batch(BATCH_SIZE)


if __name__ == '__main__':
    # Start scheduler for continuous scraping
    scheduler.add_job(scheduled_scrape, 'interval', minutes=SCRAPE_INTERVAL)
    scheduler.start()

    # Auto-start scraping in background
    if AUTO_START:
        startup_thread = threading.Thread(target=auto_start_scraping, daemon=True)
        startup_thread.start()

    # Run Flask
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
