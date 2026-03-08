#!/usr/bin/env python3
"""
X Media Fetcher - X API v2 + Grok AI Integration
Fetches tweets, media, and suggests search queries for drama content
"""

import json
import os
import re
import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse


class XMediaFetcher:
    """Fetch media from X/Twitter using X API v2 and Grok AI for analysis."""
    
    def __init__(self):
        # Load credentials from env with fallback to hardcoded values
        self.x_bearer_token = __import__('urllib.parse', fromlist=['unquote']).unquote(os.environ.get(
            'X_BEARER_TOKEN', ''
        ))
        self.xai_api_key = os.environ.get(
            'XAI_API_KEY', ''
        )
        self.xai_endpoint = 'https://api.x.ai/v1/chat/completions'
        self.x_api_base = 'https://api.x.com/2'
        
        # Track rate limit info
        self.rate_limit_remaining = None
        self.rate_limit_reset = None
    
    def _x_api_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make authenticated request to X API v2."""
        url = f"{self.x_api_base}{endpoint}"
        if params:
            import urllib.parse as _urlp; query_string = '&'.join([f"{k}={_urlp.quote(str(v))}" for k, v in params.items()])
            url = f"{url}?{query_string}"
        
        headers = {
            'Authorization': f'Bearer {self.x_bearer_token}',
            'User-Agent': 'DramaPipeline/1.0',
            'Content-Type': 'application/json'
        }
        
        print(f"[XMediaFetcher] API Request: {url}")
        
        try:
            req = urllib.request.Request(url, headers=headers, method='GET')
            with urllib.request.urlopen(req, timeout=30) as response:
                # Track rate limits
                self.rate_limit_remaining = response.headers.get('x-rate-limit-remaining')
                self.rate_limit_reset = response.headers.get('x-rate-limit-reset')
                
                if self.rate_limit_remaining:
                    print(f"[XMediaFetcher] Rate limit remaining: {self.rate_limit_remaining}")
                
                data = json.loads(response.read().decode('utf-8'))
                return data
        except urllib.error.HTTPError as e:
            print(f"[XMediaFetcher] HTTP Error {e.code}: {e.reason}")
            if e.code == 429:
                print("[XMediaFetcher] Rate limit hit - skipping this request")
            return None
        except Exception as e:
            print(f"[XMediaFetcher] API Error: {e}")
            return None
    
    def _grok_request(self, messages: List[Dict], temperature: float = 0.7) -> Optional[str]:
        """Make request to Grok AI API."""
        headers = {
            'Authorization': f'Bearer {self.xai_api_key}',
            'User-Agent': 'DramaPipeline/1.0',
            'Content-Type': 'application/json'
        }
        
        payload = {
            'model': 'grok-3',
            'messages': messages,
            'temperature': temperature
        }
        
        print(f"[XMediaFetcher] Grok AI request: {len(messages)} messages")
        
        try:
            req = urllib.request.Request(
                self.xai_endpoint,
                data=json.dumps(payload).encode('utf-8'),
                headers=headers,
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=60) as response:
                data = json.loads(response.read().decode('utf-8'))
                return data['choices'][0]['message']['content']
        except Exception as e:
            print(f"[XMediaFetcher] Grok API Error: {e}")
            return None
    
    def extract_tweet_id(self, url: str) -> Optional[str]:
        """Extract tweet ID from X/Twitter URL."""
        patterns = [
            r'x\.com/\w+/status/(\d+)',
            r'twitter\.com/\w+/status/(\d+)',
            r'x\.com/\w+/status/(\d+)\?',
            r'twitter\.com/\w+/status/(\d+)\?'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        return None
    
    def fetch_tweet_media(self, tweet_id: str) -> Dict:
        """
        Fetch tweet with media using X API v2.
        Returns dict with success, local_path, media_type, source_url
        """
        print(f"[XMediaFetcher] Fetching tweet {tweet_id}")
        
        endpoint = f"/tweets/{tweet_id}"
        params = {
            'expansions': 'attachments.media_keys,author_id',
            'media.fields': 'url,preview_image_url,type,variants,alt_text',
            'tweet.fields': 'created_at,public_metrics,context_annotations',
            'user.fields': 'username,profile_image_url'
        }
        
        data = self._x_api_request(endpoint, params)
        
        if not data or 'data' not in data:
            print(f"[XMediaFetcher] Failed to fetch tweet {tweet_id}")
            return {
                'success': False,
                'local_path': None,
                'media_type': None,
                'source_url': f"https://x.com/i/status/{tweet_id}",
                'error': 'API request failed'
            }
        
        tweet = data['data']
        media_list = []
        
        # Extract media from includes
        if 'includes' in data and 'media' in data['includes']:
            media_list = data['includes']['media']
        
        if not media_list:
            print(f"[XMediaFetcher] No media found in tweet {tweet_id}")
            return {
                'success': False,
                'local_path': None,
                'media_type': 'text_only',
                'source_url': f"https://x.com/i/status/{tweet_id}",
                'tweet_text': tweet.get('text', ''),
                'error': 'No media in tweet'
            }
        
        # Download the first media item (most important one)
        media = media_list[0]
        media_type = media.get('type', 'unknown')
        
        print(f"[XMediaFetcher] Found media type: {media_type}")
        
        # Determine download URL based on media type
        download_url = None
        if media_type == 'photo':
            download_url = media.get('url')
        elif media_type == 'video' or media_type == 'animated_gif':
            # Get best quality variant
            variants = media.get('variants', [])
            if variants:
                # Sort by bitrate, prefer MP4
                mp4_variants = [v for v in variants if v.get('content_type') == 'video/mp4']
                if mp4_variants:
                    download_url = max(mp4_variants, key=lambda x: x.get('bit_rate', 0))['url']
                else:
                    download_url = variants[0]['url']
        
        if not download_url:
            return {
                'success': False,
                'local_path': None,
                'media_type': media_type,
                'source_url': f"https://x.com/i/status/{tweet_id}",
                'error': 'No downloadable URL found'
            }
        
        # Create assets directory
        assets_dir = Path.home() / 'drama-pipeline' / 'assets' / datetime.now().strftime('%Y-%m-%d')
        assets_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine file extension
        ext = '.jpg' if media_type == 'photo' else '.mp4'
        if media_type == 'animated_gif':
            ext = '.mp4'
        
        # Generate filename
        filename = f"tweet_{tweet_id}_{media_type}{ext}"
        output_path = assets_dir / filename
        
        # Download the media
        print(f"[XMediaFetcher] Downloading: {download_url}")
        try:
            req = urllib.request.Request(download_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            with urllib.request.urlopen(req, timeout=60) as response:
                with open(output_path, 'wb') as f:
                    f.write(response.read())
            
            print(f"[XMediaFetcher] Downloaded to: {output_path}")
            
            return {
                'success': True,
                'local_path': str(output_path),
                'media_type': media_type,
                'source_url': f"https://x.com/i/status/{tweet_id}",
                'tweet_text': tweet.get('text', ''),
                'media_count': len(media_list)
            }
        except Exception as e:
            print(f"[XMediaFetcher] Download error: {e}")
            return {
                'success': False,
                'local_path': None,
                'media_type': media_type,
                'source_url': f"https://x.com/i/status/{tweet_id}",
                'error': str(e)
            }
    
    def search_recent_tweets(self, query: str, max_results: int = 10) -> List[Dict]:
        """
        Search recent tweets with media.
        Returns list of tweet dicts with media info.
        """
        print(f"[XMediaFetcher] Searching tweets: {query}")
        
        endpoint = "/tweets/search/recent"
        params = {
            'query': f"{query} has:media -is:retweet",
            'max_results': str(min(max_results, 100)),
            'expansions': 'attachments.media_keys,author_id',
            'media.fields': 'url,preview_image_url,type,variants',
            'tweet.fields': 'created_at,public_metrics',
            'user.fields': 'username'
        }
        
        data = self._x_api_request(endpoint, params)
        
        if not data or 'data' not in data:
            print(f"[XMediaFetcher] No results for query: {query}")
            return []
        
        tweets = data['data']
        media_map = {}
        
        if 'includes' in data and 'media' in data['includes']:
            for m in data['includes']['media']:
                media_map[m['media_key']] = m
        
        results = []
        for tweet in tweets:
            media_keys = tweet.get('attachments', {}).get('media_keys', [])
            tweet_media = [media_map.get(k) for k in media_keys if k in media_map]
            
            results.append({
                'id': tweet['id'],
                'text': tweet.get('text', ''),
                'created_at': tweet.get('created_at'),
                'metrics': tweet.get('public_metrics', {}),
                'media': tweet_media,
                'url': f"https://x.com/i/status/{tweet['id']}"
            })
        
        print(f"[XMediaFetcher] Found {len(results)} tweets with media")
        return results
    
    def grok_suggest_search(self, line_text: str) -> List[str]:
        """
        Use Grok AI to suggest X search queries for a script line.
        Returns list of search query strings.
        """
        print(f"[XMediaFetcher] Asking Grok for search suggestions: {line_text[:60]}...")
        
        messages = [
            {
                'role': 'system',
                'content': 'You are an X/Twitter search expert. Given a script line from a drama video, suggest 2-3 BROAD search queries to find tweets with media (photos/videos) about this topic. Rules: Use the actual person/topic name. Keep queries SHORT (2-4 words). Do NOT use exact quotes from the script. Do NOT use operators like filter:, filetype:, -filter:. Return ONLY queries, one per line.'
            },
            {
                'role': 'user',
                'content': f'Script line: "{line_text}"\n\nSuggest short, broad X search queries (use real names/topics):'
            }
        ]
        
        response = self._grok_request(messages, temperature=0.8)
        
        if not response:
            print("[XMediaFetcher] Grok request failed, using fallback")
            # Fallback: extract keywords
            words = line_text.split()[:5]
            return [' '.join(words)]
        
        # Parse queries from response
        queries = [q.strip() for q in response.strip().split('\n') if q.strip()]
        # Clean queries: strip invalid X API operators Grok sometimes includes
        import re as _re
        cleaned = []
        for q in queries:
            if q.startswith('-') or q.startswith('*') or q.startswith('#'):
                continue
            # Remove invalid operators
            q = _re.sub(r'-filter:\\w+', '', q)
            q = _re.sub(r'filter:\\w+', '', q)  
            q = _re.sub(r'filetype:\\w+', '', q)
            q = q.strip()
            if q:
                cleaned.append(q)
        queries = cleaned
        
        print(f"[XMediaFetcher] Grok suggested {len(queries)} queries")
        for q in queries:
            print(f"  - {q}")
        
        return queries[:3]  # Max 3 queries
    
    def grok_analyze_thread(self, tweets: List[Dict]) -> Optional[Dict]:
        """
        Use Grok AI to pick the most visual/impactful tweet from a thread.
        Returns the best tweet dict.
        """
        if not tweets:
            return None
        
        if len(tweets) == 1:
            return tweets[0]
        
        print(f"[XMediaFetcher] Asking Grok to analyze {len(tweets)} tweets")
        
        # Format tweets for Grok
        tweets_text = '\n\n'.join([
            f"Tweet {i+1} (ID: {t['id']}):\n{t['text'][:200]}...\nMedia: {len(t.get('media', []))} items"
            for i, t in enumerate(tweets[:5])  # Limit to first 5
        ])
        
        messages = [
            {
                'role': 'system',
                'content': 'You are analyzing a tweet thread to find the most visual/impactful tweet for a video. Consider: visual content, drama level, quotability. Respond with ONLY the tweet number (1, 2, 3, etc.).'
            },
            {
                'role': 'user',
                'content': f'Tweet thread:\n\n{tweets_text}\n\nWhich tweet is most visual/impactful? Respond with just the number:'
            }
        ]
        
        response = self._grok_request(messages, temperature=0.5)
        
        if response:
            try:
                # Extract number from response
                num = int(re.search(r'\d+', response).group())
                if 1 <= num <= len(tweets):
                    print(f"[XMediaFetcher] Grok selected tweet #{num}")
                    return tweets[num - 1]
            except:
                pass
        
        # Fallback: pick tweet with most media
        best = max(tweets, key=lambda t: len(t.get('media', [])))
        print(f"[XMediaFetcher] Fallback: selected tweet with most media")
        return best
    
    def fetch_media_from_url(self, url: str) -> Dict:
        """
        Main entry point - fetch media from any X/Twitter URL.
        Returns standardized result dict.
        """
        tweet_id = self.extract_tweet_id(url)
        
        if not tweet_id:
            print(f"[XMediaFetcher] Could not extract tweet ID from: {url}")
            return {
                'success': False,
                'local_path': None,
                'media_type': None,
                'source_url': url,
                'error': 'Invalid X/Twitter URL'
            }
        
        return self.fetch_tweet_media(tweet_id)
    
    def find_and_download_media(self, line_text: str) -> Dict:
        """
        Full pipeline: suggest search, find tweets, download best media.
        For script lines without a source URL.
        Returns standardized result dict.
        """
        print(f"[XMediaFetcher] Finding media for: {line_text[:60]}...")
        
        # Get search suggestions from Grok
        queries = self.grok_suggest_search(line_text)
        
        if not queries:
            return {
                'success': False,
                'local_path': None,
                'media_type': None,
                'source_url': None,
                'error': 'No search queries generated'
            }
        
        # Try each query
        for query in queries:
            tweets = self.search_recent_tweets(query, max_results=10)
            
            if tweets:
                # Pick best tweet
                best_tweet = self.grok_analyze_thread(tweets)
                
                if best_tweet:
                    # Download media from this tweet
                    result = self.fetch_tweet_media(best_tweet['id'])
                    
                    if result['success']:
                        result['search_query'] = query
                        result['found_tweet_id'] = best_tweet['id']
                        return result
        
        # Nothing found
        print(f"[XMediaFetcher] No media found for any query")
        return {
            'success': False,
            'local_path': None,
            'media_type': None,
            'source_url': None,
            'error': 'No media found via search',
            'queries_tried': queries
        }


# Convenience function for direct usage
def fetch_x_media(url: str) -> Dict:
    """Fetch media from X URL."""
    fetcher = XMediaFetcher()
    return fetcher.fetch_media_from_url(url)


def find_media_for_line(line_text: str) -> Dict:
    """Find and download media for a script line without source URL."""
    fetcher = XMediaFetcher()
    return fetcher.find_and_download_media(line_text)


if __name__ == '__main__':
    # Test
    import sys
    
    if len(sys.argv) > 1:
        url = sys.argv[1]
        result = fetch_x_media(url)
        print(json.dumps(result, indent=2))
    else:
        # Test search
        result = find_media_for_line("MrBeast janitor millionaire revealed")
        print(json.dumps(result, indent=2))
