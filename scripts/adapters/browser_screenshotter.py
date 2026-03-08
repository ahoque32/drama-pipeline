#!/usr/bin/env python3
"""
Browser Screenshotter - Web page screenshot & scraping automation
Uses Scrapling (StealthyFetcher + page_action) with Camoufox fallback.

Activate venv: source ~/scrapling-env/bin/activate
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse


class BrowserScreenshotter:
    """Capture screenshots and scrape content using Scrapling or Camoufox."""

    def __init__(self):
        self._scrapling_ok = None
        self._camoufox_ok = None

    @property
    def scrapling_available(self):
        if self._scrapling_ok is None:
            try:
                from scrapling.fetchers import StealthyFetcher
                self._scrapling_ok = True
            except ImportError:
                self._scrapling_ok = False
        return self._scrapling_ok

    @property
    def camoufox_available(self):
        if self._camoufox_ok is None:
            try:
                from camoufox.sync_api import Camoufox
                self._camoufox_ok = True
            except ImportError:
                self._camoufox_ok = False
        return self._camoufox_ok

    def _sanitize_filename(self, url: str) -> str:
        parsed = urlparse(url)
        domain = parsed.netloc.replace('www.', '').replace('.', '_')
        path = parsed.path.strip('/').replace('/', '_')[:30]
        ts = datetime.now().strftime('%H%M%S')
        return f"{domain}_{path}_{ts}" if path else f"{domain}_{ts}"

    def capture_screenshot(self, url: str, output_path: Path = None,
                           full_page: bool = True, wait_time: int = 3) -> Dict:
        """Capture screenshot. Tries Scrapling first, falls back to raw Camoufox."""
        print(f"[Screenshot] Capturing: {url}")

        if output_path is None:
            output_path = Path(f"/tmp/{self._sanitize_filename(url)}.png")
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if self.scrapling_available:
            result = self._capture_scrapling(url, output_path, full_page)
            if result['success']:
                return result
            print(f"[Screenshot] Scrapling failed ({result.get('error')}), trying Camoufox...")

        if self.camoufox_available:
            return self._capture_camoufox(url, output_path, full_page, wait_time)

        return {'success': False, 'error': 'No browser engine available'}

    def _capture_scrapling(self, url: str, output_path: Path, full_page: bool) -> Dict:
        """Screenshot via Scrapling StealthyFetcher + page_action."""
        try:
            from scrapling.fetchers import StealthyFetcher

            captured = {}

            def take_screenshot(page):
                page.screenshot(path=str(output_path), full_page=full_page)
                captured['done'] = True

            page = StealthyFetcher.fetch(
                url, headless=True, network_idle=True,
                page_action=take_screenshot
            )

            if captured.get('done') and output_path.exists() and output_path.stat().st_size > 0:
                print(f"[Screenshot] Saved (Scrapling): {output_path} ({output_path.stat().st_size} bytes)")
                return {
                    'success': True,
                    'local_path': str(output_path),
                    'source_url': url,
                    'full_page': full_page,
                    'engine': 'scrapling',
                    'title': page.css('title::text').get(),
                    'page_length': len(page.html_content)
                }
            return {'success': False, 'error': 'Screenshot not captured'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _capture_camoufox(self, url: str, output_path: Path,
                          full_page: bool, wait_time: int) -> Dict:
        """Screenshot via raw Camoufox."""
        try:
            from camoufox.sync_api import Camoufox

            with Camoufox(headless=True) as browser:
                page = browser.new_page()
                page.goto(url, wait_until='networkidle', timeout=15000)
                time.sleep(wait_time)
                page.screenshot(path=str(output_path), full_page=full_page)
                title = page.title()
                page.close()

            if output_path.exists() and output_path.stat().st_size > 0:
                print(f"[Screenshot] Saved (Camoufox): {output_path}")
                return {
                    'success': True,
                    'local_path': str(output_path),
                    'source_url': url,
                    'full_page': full_page,
                    'engine': 'camoufox',
                    'title': title
                }
            return {'success': False, 'error': 'Screenshot file empty'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def scrape_page(self, url: str, selectors: Dict[str, str] = None,
                    adaptive: bool = False) -> Dict:
        """Scrape structured data via Scrapling StealthyFetcher (browser-backed).

        Args:
            url: Target URL
            selectors: Dict of {field_name: css_selector}
            adaptive: Use adaptive element tracking (survives site redesigns)
        """
        if not self.scrapling_available:
            return {'success': False, 'error': 'Scrapling not available'}
        try:
            from scrapling.fetchers import StealthyFetcher
            page = StealthyFetcher.fetch(url, headless=True, network_idle=True)
            result = {
                'success': True, 'url': url,
                'title': page.css('title::text').get(),
                'page_length': len(page.html_content),
                'engine': 'scrapling-stealthy'
            }
            if selectors:
                result['data'] = self._extract(page, selectors, adaptive)
            return result
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def scrape_http(self, url: str, selectors: Dict[str, str] = None) -> Dict:
        """Fast HTTP scrape — no browser, TLS fingerprint impersonation only."""
        if not self.scrapling_available:
            return {'success': False, 'error': 'Scrapling not available'}
        try:
            from scrapling.fetchers import Fetcher
            page = Fetcher.get(url)
            result = {
                'success': True, 'url': url,
                'title': page.css('title::text').get(),
                'page_length': len(page.html_content),
                'engine': 'scrapling-http'
            }
            if selectors:
                result['data'] = self._extract(page, selectors)
            return result
        except Exception as e:
            return {'success': False, 'error': str(e)}

    @staticmethod
    def _extract(page, selectors: Dict[str, str], adaptive: bool = False) -> Dict:
        data = {}
        for name, sel in selectors.items():
            if adaptive:
                elements = page.css(sel, adaptive=True)
            else:
                elements = page.css(sel)
            if '::text' in sel:
                data[name] = elements.getall() if elements else []
            else:
                data[name] = [e.html for e in elements] if elements else []
        return data

    def auto_capture(self, url: str, output_dir: Path = None) -> Dict:
        if output_dir:
            output_path = output_dir / f"{self._sanitize_filename(url)}.png"
        else:
            output_path = None
        return self.capture_screenshot(url, output_path)


# Module-level convenience functions
def capture_screenshot(url: str, output_path: Path = None, **kw) -> Dict:
    return BrowserScreenshotter().capture_screenshot(url, output_path, **kw)

def auto_capture(url: str, output_dir: Path = None) -> Dict:
    return BrowserScreenshotter().auto_capture(url, output_dir)

def scrape_page(url: str, selectors: Dict[str, str] = None, **kw) -> Dict:
    return BrowserScreenshotter().scrape_page(url, selectors, **kw)

def scrape_http(url: str, selectors: Dict[str, str] = None) -> Dict:
    return BrowserScreenshotter().scrape_http(url, selectors)
