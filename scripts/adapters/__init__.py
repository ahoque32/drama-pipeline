"""
AssetHunter Adapters Package

Provides media fetching and downloading capabilities for the drama pipeline.

Adapters:
    x_media_fetcher: X/Twitter API v2 + Grok AI integration
    video_clipper: yt-dlp + ffmpeg for video downloads
    browser_screenshotter: Web page screenshot automation
"""

from .x_media_fetcher import XMediaFetcher, fetch_x_media, find_media_for_line
from .video_clipper import VideoClipper, download_clip, auto_download
from .browser_screenshotter import BrowserScreenshotter, capture_screenshot, auto_capture

__all__ = [
    'XMediaFetcher',
    'fetch_x_media',
    'find_media_for_line',
    'VideoClipper',
    'download_clip',
    'auto_download',
    'BrowserScreenshotter',
    'capture_screenshot',
    'auto_capture',
]
