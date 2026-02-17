#!/usr/bin/env python3
"""
ScoutInstagram - Instagram Post Fetching Module
Fetches posts from Instagram using the gram CLI tool.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


def fetch_instagram_posts(handle: str) -> List[Dict]:
    """
    Fetch recent posts from an Instagram account using gram CLI.
    
    Args:
        handle: Instagram username (without @)
        
    Returns:
        List of post dictionaries in standard format
    """
    # Ensure required env vars are set
    required_env = ['SESSION_ID', 'CSRF_TOKEN', 'DS_USER_ID']
    for env_var in required_env:
        if not os.environ.get(env_var):
            print(f"[ScoutInstagram] Warning: {env_var} not set, skipping {handle}")
            return []
    
    try:
        # Run gram CLI to fetch posts
        cmd = ['gram', 'posts', handle, '-n', '20', '--json']
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode != 0:
            print(f"[ScoutInstagram] gram error for {handle}: {result.stderr}")
            return []
        
        # Parse JSON output
        posts_data = json.loads(result.stdout)
        
        if not isinstance(posts_data, list):
            print(f"[ScoutInstagram] Unexpected response format for {handle}")
            return []
        
        # Filter to last 24h and convert format
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        posts = []
        
        for post in posts_data:
            # Parse timestamp
            try:
                # Instagram timestamps are usually in seconds since epoch
                timestamp = post.get('taken_at', 0)
                if isinstance(timestamp, (int, float)):
                    post_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                else:
                    continue
                
                # Filter to last 24h
                if post_time < cutoff_time:
                    continue
                
                # Calculate engagement: likes + comments*3
                likes = post.get('like_count', 0) or 0
                comments = post.get('comment_count', 0) or 0
                engagement = likes + (comments * 3)
                
                # Build caption text
                caption = ""
                if post.get('caption'):
                    if isinstance(post['caption'], dict):
                        caption = post['caption'].get('text', '')
                    else:
                        caption = str(post['caption'])
                
                posts.append({
                    'id': str(post.get('pk', post.get('id', 'unknown'))),
                    'text': caption,
                    'created_at': post_time.isoformat(),
                    'source': f"@{handle}",
                    'source_url': f"https://instagram.com/p/{post.get('code', '')}/" if post.get('code') else f"https://instagram.com/{handle}/",
                    'likes': likes,
                    'comments': comments,
                    'engagement': engagement,
                    'media_type': post.get('media_type', 'unknown'),
                    '_type': 'instagram'
                })
                
            except Exception as e:
                print(f"[ScoutInstagram] Error processing post: {e}")
                continue
        
        print(f"[ScoutInstagram] Fetched {len(posts)} posts from @{handle} (last 24h)")
        return posts
        
    except subprocess.TimeoutExpired:
        print(f"[ScoutInstagram] Timeout fetching posts from {handle}")
        return []
    except json.JSONDecodeError as e:
        print(f"[ScoutInstagram] JSON parse error for {handle}: {e}")
        return []
    except Exception as e:
        print(f"[ScoutInstagram] Error fetching {handle}: {e}")
        return []


def fetch_all_instagram_posts(config: Optional[Dict] = None) -> List[Dict]:
    """
    Fetch posts from all configured Instagram accounts.
    
    Args:
        config: Optional config dict (loads from file if not provided)
        
    Returns:
        List of all posts from all configured accounts
    """
    if config is None:
        config = load_config()
    
    instagram_sources = config.get('sources', {}).get('instagram', [])
    all_posts = []
    
    for source in instagram_sources:
        handle = source.get('handle', '').lstrip('@')
        if not handle:
            continue
        
        posts = fetch_instagram_posts(handle)
        all_posts.extend(posts)
    
    return all_posts


if __name__ == "__main__":
    # Test mode
    import argparse
    parser = argparse.ArgumentParser(description='Fetch Instagram posts')
    parser.add_argument('handle', nargs='?', help='Instagram handle to fetch')
    args = parser.parse_args()
    
    if args.handle:
        posts = fetch_instagram_posts(args.handle.lstrip('@'))
        print(f"\nFound {len(posts)} posts from @{args.handle}:")
        for post in posts:
            print(f"  - {post['id'][:12]}... | {post['likes']} likes | {post['comments']} comments | engagement: {post['engagement']}")
            print(f"    {post['text'][:100]}...")
    else:
        # Test with config
        posts = fetch_all_instagram_posts()
        print(f"\nTotal posts from all sources: {len(posts)}")
