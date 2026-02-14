#!/usr/bin/env python3
"""
YouTube Uploader Module

Uploads videos to YouTube using Data API v3.
Supports OAuth 2.0, Shorts optimization, thumbnails, and rate limit handling.

Usage:
    python youtube_uploader.py --file video.mp4 --title "My Video" --privacy private
    python youtube_uploader.py --file short.mp4 --title "Short" --shorts --privacy public
    python youtube_uploader.py --file video.mp4 --thumbnail thumb.jpg --title "With Thumb"
"""

import argparse
import json
import logging
import mimetypes
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Callable, Dict, Any, List

# Google API imports
try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaFileUpload, ResumableMediaUpload
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    GOOGLE_LIBS_AVAILABLE = True
except ImportError:
    GOOGLE_LIBS_AVAILABLE = False

# Constants
SCOPES = ['https://www.googleapis.com/auth/youtube.upload']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
QUOTA_LIMIT = 10000

# Paths
CREDENTIALS_DIR = Path.home() / '.openclaw' / 'credentials'
TOKEN_PATH = CREDENTIALS_DIR / 'youtube-token.json'
CLIENT_SECRETS_PATH = CREDENTIALS_DIR / 'youtube-client-secrets.json'
QUOTA_PATH = CREDENTIALS_DIR / 'youtube-quota.json'
UPLOADS_DIR = Path(__file__).parent.parent / 'uploads'

# Shorts detection
SHORTS_ASPECT_RATIO = 9 / 16  # 0.5625
SHORTS_WIDTH = 1080
SHORTS_HEIGHT = 1920
SHORTS_HASHTAG = '#Shorts'

# Category IDs
CATEGORY_PEOPLE_BLOGS = '22'
CATEGORY_ENTERTAINMENT = '24'

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger('youtube_uploader')


@dataclass
class QuotaTracker:
    """Tracks YouTube API quota usage."""
    used: int = 0
    limit: int = QUOTA_LIMIT
    date: str = field(default_factory=lambda: date.today().isoformat())
    
    @classmethod
    def load(cls) -> 'QuotaTracker':
        """Load quota tracker from file."""
        if QUOTA_PATH.exists():
            try:
                data = json.loads(QUOTA_PATH.read_text())
                # Reset if it's a new day
                if data.get('date') != date.today().isoformat():
                    return cls()
                return cls(**data)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to load quota tracker: {e}")
        return cls()
    
    def save(self) -> None:
        """Save quota tracker to file."""
        CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
        QUOTA_PATH.write_text(json.dumps({
            'used': self.used,
            'limit': self.limit,
            'date': self.date
        }, indent=2))
    
    def check_quota(self, cost: int = 1) -> bool:
        """Check if there's enough quota remaining."""
        return (self.used + cost) <= self.limit
    
    def use_quota(self, cost: int = 1) -> None:
        """Record quota usage."""
        self.used += cost
        self.save()
    
    def remaining(self) -> int:
        """Get remaining quota."""
        return self.limit - self.used


@dataclass
class UploadResult:
    """Result of a video upload."""
    video_id: str
    url: str
    upload_timestamp: str
    title: str
    privacy_status: str
    shorts_optimized: bool = False
    mock: bool = False


class YouTubeUploader:
    """YouTube video uploader with OAuth 2.0 and quota management."""
    
    def __init__(self, mock_mode: bool = False):
        self.mock_mode = mock_mode or not GOOGLE_LIBS_AVAILABLE
        self.credentials: Optional[Credentials] = None
        self.service: Optional[Any] = None
        self.quota = QuotaTracker.load()
        
        if not self.mock_mode:
            self._authenticate()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Clean up if needed
        pass
    
    def _authenticate(self) -> None:
        """Authenticate with YouTube API using OAuth 2.0."""
        if not CLIENT_SECRETS_PATH.exists():
            logger.warning(f"Client secrets not found at {CLIENT_SECRETS_PATH}")
            logger.info("Running in mock mode - no uploads will be performed")
            self.mock_mode = True
            return
        
        # Load existing token
        if TOKEN_PATH.exists():
            try:
                self.credentials = Credentials.from_authorized_user_file(
                    str(TOKEN_PATH), SCOPES
                )
            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")
        
        # Refresh or create new credentials
        if not self.credentials or not self.credentials.valid:
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                logger.info("Refreshing access token...")
                self.credentials.refresh(Request())
            else:
                logger.info("Starting OAuth flow - browser will open...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(CLIENT_SECRETS_PATH), SCOPES
                )
                self.credentials = flow.run_local_server(port=0)
            
            # Save token for future runs
            CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
            TOKEN_PATH.write_text(self.credentials.to_json())
            logger.info(f"Token saved to {TOKEN_PATH}")
        
        # Build YouTube service
        self.service = build(API_SERVICE_NAME, API_VERSION, credentials=self.credentials)
        logger.info("Successfully authenticated with YouTube API")
    
    def _detect_shorts_format(self, file_path: Path) -> bool:
        """Detect if video is in Shorts format (9:16 aspect ratio)."""
        try:
            # Try to use ffprobe if available
            import subprocess
            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
                 '-show_entries', 'stream=width,height', '-of', 'json', str(file_path)],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                stream = data.get('streams', [{}])[0]
                width = stream.get('width', 0)
                height = stream.get('height', 0)
                if width and height:
                    ratio = width / height
                    # Check if close to 9:16 (0.5625)
                    is_shorts = abs(ratio - SHORTS_ASPECT_RATIO) < 0.05
                    logger.debug(f"Video dimensions: {width}x{height}, ratio: {ratio:.4f}, shorts: {is_shorts}")
                    return is_shorts
        except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
            logger.debug(f"Could not detect video format: {e}")
        
        # Fallback: check filename for shorts indicators
        filename_lower = file_path.name.lower()
        shorts_indicators = ['short', 'shorts', 'vertical', '9x16', '9_16', '1080x1920']
        return any(ind in filename_lower for ind in shorts_indicators)
    
    def _optimize_for_shorts(
        self, title: str, description: str, tags: List[str], category_id: str
    ) -> tuple:
        """Optimize metadata for YouTube Shorts."""
        # Add #Shorts hashtag if missing
        if SHORTS_HASHTAG not in description:
            description = f"{description}\n\n{SHORTS_HASHTAG}".strip()
        
        # Ensure shorts tag is present
        if 'Shorts' not in tags:
            tags = tags + ['Shorts']
        
        # Default to People & Blogs category for Shorts
        if not category_id:
            category_id = CATEGORY_PEOPLE_BLOGS
        
        return title, description, tags, category_id
    
    def _save_upload_record(self, result: UploadResult, metadata: Dict[str, Any]) -> None:
        """Save upload record to JSON file."""
        UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
        
        record_file = UPLOADS_DIR / f"{date.today().isoformat()}.json"
        
        # Load existing records
        records = []
        if record_file.exists():
            try:
                records = json.loads(record_file.read_text())
            except json.JSONDecodeError:
                records = []
        
        # Add new record
        record = {
            'video_id': result.video_id,
            'url': result.url,
            'upload_timestamp': result.upload_timestamp,
            'title': result.title,
            'privacy_status': result.privacy_status,
            'shorts_optimized': result.shorts_optimized,
            'mock': result.mock,
            'metadata': metadata
        }
        records.append(record)
        
        # Save back
        record_file.write_text(json.dumps(records, indent=2))
        logger.info(f"Upload record saved to {record_file}")
    
    def upload_video(
        self,
        file_path: str,
        title: str,
        description: str = "",
        tags: Optional[List[str]] = None,
        category_id: str = "",
        privacy_status: str = "private",
        shorts: bool = False,
        progress_callback: Optional[Callable[[int], None]] = None
    ) -> UploadResult:
        """
        Upload a video to YouTube.
        
        Args:
            file_path: Path to video file
            title: Video title
            description: Video description
            tags: List of tags
            category_id: YouTube category ID
            privacy_status: private, unlisted, or public
            shorts: Force Shorts optimization
            progress_callback: Optional callback for upload progress (0-100)
        
        Returns:
            UploadResult with video details
        
        Raises:
            ValueError: If privacy_status is invalid
            RuntimeError: If quota exceeded or upload fails
        """
        # Validate inputs
        valid_privacy = ['private', 'unlisted', 'public']
        if privacy_status not in valid_privacy:
            raise ValueError(f"privacy_status must be one of {valid_privacy}")
        
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            raise FileNotFoundError(f"Video file not found: {file_path}")
        
        tags = tags or []
        
        # Check for Shorts format
        is_shorts = shorts or self._detect_shorts_format(file_path_obj)
        if is_shorts:
            title, description, tags, category_id = self._optimize_for_shorts(
                title, description, tags, category_id
            )
            logger.info("Shorts format detected - optimizing metadata")
        
        # Check quota (video.insert costs approximately 1600 units)
        quota_cost = 1600
        if not self.quota.check_quota(quota_cost):
            raise RuntimeError(
                f"Quota exceeded. Used: {self.quota.used}/{self.quota.limit}. "
                f"Resets at midnight Pacific Time."
            )
        
        if self.mock_mode:
            # Mock upload
            logger.info("[MOCK MODE] Would upload video:")
            logger.info(f"  File: {file_path}")
            logger.info(f"  Title: {title}")
            logger.info(f"  Privacy: {privacy_status}")
            logger.info(f"  Shorts: {is_shorts}")
            
            mock_id = f"mock_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            result = UploadResult(
                video_id=mock_id,
                url=f"https://youtube.com/watch?v={mock_id}",
                upload_timestamp=datetime.now().isoformat(),
                title=title,
                privacy_status=privacy_status,
                shorts_optimized=is_shorts,
                mock=True
            )
            
            self._save_upload_record(result, {
                'file_path': str(file_path_obj),
                'description': description,
                'tags': tags,
                'category_id': category_id
            })
            
            return result
        
        # Real upload
        body = {
            'snippet': {
                'title': title,
                'description': description,
                'tags': tags,
                'categoryId': category_id or CATEGORY_ENTERTAINMENT
            },
            'status': {
                'privacyStatus': privacy_status,
                'selfDeclaredMadeForKids': False
            }
        }
        
        # Determine media type
        media_type, _ = mimetypes.guess_type(str(file_path_obj))
        if not media_type:
            media_type = 'video/mp4'
        
        media = MediaFileUpload(
            str(file_path_obj),
            mimetype=media_type,
            resumable=True
        )
        
        try:
            logger.info(f"Starting upload: {title}")
            request = self.service.videos().insert(
                part=','.join(body.keys()),
                body=body,
                media_body=media
            )
            
            # Execute upload with progress tracking
            response = None
            while response is None:
                status, response = request.next_chunk()
                if status and progress_callback:
                    progress_callback(int(status.progress() * 100))
            
            video_id = response['id']
            result = UploadResult(
                video_id=video_id,
                url=f"https://youtube.com/watch?v={video_id}",
                upload_timestamp=datetime.now().isoformat(),
                title=title,
                privacy_status=privacy_status,
                shorts_optimized=is_shorts,
                mock=False
            )
            
            # Track quota usage
            self.quota.use_quota(quota_cost)
            logger.info(f"Upload complete: {result.url}")
            logger.info(f"Quota used: {self.quota.used}/{self.quota.limit}")
            
            # Save record
            self._save_upload_record(result, body)
            
            return result
            
        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in str(e):
                raise RuntimeError(
                    f"YouTube API quota exceeded. Used: {self.quota.used}/{self.quota.limit}"
                )
            raise RuntimeError(f"Upload failed: {e}")
    
    def upload_thumbnail(self, video_id: str, thumbnail_path: str) -> bool:
        """
        Upload a thumbnail for a video.
        
        Args:
            video_id: YouTube video ID
            thumbnail_path: Path to thumbnail image (JPG/PNG, max 2MB)
        
        Returns:
            True if successful
        
        Raises:
            FileNotFoundError: If thumbnail file doesn't exist
            RuntimeError: If upload fails or quota exceeded
        """
        thumbnail_path_obj = Path(thumbnail_path)
        if not thumbnail_path_obj.exists():
            raise FileNotFoundError(f"Thumbnail not found: {thumbnail_path}")
        
        # Check file size (max 2MB)
        file_size = thumbnail_path_obj.stat().st_size
        if file_size > 2 * 1024 * 1024:
            raise ValueError(f"Thumbnail too large: {file_size} bytes (max 2MB)")
        
        # Check quota (thumbnail.set costs approximately 50 units)
        quota_cost = 50
        if not self.quota.check_quota(quota_cost):
            raise RuntimeError(f"Quota exceeded. Remaining: {self.quota.remaining()}")
        
        if self.mock_mode:
            logger.info(f"[MOCK MODE] Would upload thumbnail for {video_id}: {thumbnail_path}")
            return True
        
        try:
            media = MediaFileUpload(str(thumbnail_path_obj))
            self.service.thumbnails().set(
                videoId=video_id,
                media_body=media
            ).execute()
            
            self.quota.use_quota(quota_cost)
            logger.info(f"Thumbnail uploaded for {video_id}")
            return True
            
        except HttpError as e:
            raise RuntimeError(f"Thumbnail upload failed: {e}")
    
    def update_privacy(self, video_id: str, privacy_status: str) -> bool:
        """
        Update video privacy status.
        
        Args:
            video_id: YouTube video ID
            privacy_status: private, unlisted, or public
        
        Returns:
            True if successful
        """
        valid_privacy = ['private', 'unlisted', 'public']
        if privacy_status not in valid_privacy:
            raise ValueError(f"privacy_status must be one of {valid_privacy}")
        
        quota_cost = 50
        if not self.quota.check_quota(quota_cost):
            raise RuntimeError(f"Quota exceeded. Remaining: {self.quota.remaining()}")
        
        if self.mock_mode:
            logger.info(f"[MOCK MODE] Would update {video_id} privacy to {privacy_status}")
            return True
        
        try:
            self.service.videos().update(
                part='status',
                body={
                    'id': video_id,
                    'status': {
                        'privacyStatus': privacy_status
                    }
                }
            ).execute()
            
            self.quota.use_quota(quota_cost)
            logger.info(f"Updated {video_id} privacy to {privacy_status}")
            return True
            
        except HttpError as e:
            raise RuntimeError(f"Privacy update failed: {e}")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Upload videos to YouTube',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --file video.mp4 --title "My Video" --privacy private
  %(prog)s --file short.mp4 --title "Short Title" --shorts --privacy public
  %(prog)s --file video.mp4 --thumbnail thumb.jpg --title "With Thumbnail"
        """
    )
    
    parser.add_argument('--file', '-f', required=True, help='Video file path')
    parser.add_argument('--title', '-t', required=True, help='Video title')
    parser.add_argument('--description', '-d', default='', help='Video description')
    parser.add_argument('--tags', help='Comma-separated tags')
    parser.add_argument('--category', '-c', help='Category ID (default: 22 for Shorts, 24 otherwise)')
    parser.add_argument('--privacy', '-p', default='private',
                        choices=['private', 'unlisted', 'public'],
                        help='Privacy status (default: private)')
    parser.add_argument('--shorts', '-s', action='store_true',
                        help='Force Shorts optimization')
    parser.add_argument('--thumbnail', help='Thumbnail image path')
    parser.add_argument('--mock', action='store_true',
                        help='Run in mock mode (no actual upload)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Parse tags
    tags = [t.strip() for t in args.tags.split(',')] if args.tags else []
    
    try:
        with YouTubeUploader(mock_mode=args.mock) as uploader:
            # Upload video
            result = uploader.upload_video(
                file_path=args.file,
                title=args.title,
                description=args.description,
                tags=tags,
                category_id=args.category or '',
                privacy_status=args.privacy,
                shorts=args.shorts,
                progress_callback=lambda p: logger.info(f"Upload progress: {p}%")
            )
            
            # Upload thumbnail if provided
            if args.thumbnail:
                uploader.upload_thumbnail(result.video_id, args.thumbnail)
            
            # Output results
            print(f"\n{'='*50}")
            print(f"Upload {'(MOCK) ' if result.mock else ''}Complete!")
            print(f"{'='*50}")
            print(f"Video ID:  {result.video_id}")
            print(f"URL:       {result.url}")
            print(f"Timestamp: {result.upload_timestamp}")
            print(f"Privacy:   {result.privacy_status}")
            print(f"Shorts:    {'Yes' if result.shorts_optimized else 'No'}")
            print(f"Quota:     {uploader.quota.used}/{uploader.quota.limit}")
            print(f"{'='*50}\n")
            
            # Return JSON for piping
            output = {
                'video_id': result.video_id,
                'url': result.url,
                'upload_timestamp': result.upload_timestamp,
                'privacy_status': result.privacy_status,
                'shorts_optimized': result.shorts_optimized,
                'mock': result.mock,
                'quota_used': uploader.quota.used,
                'quota_limit': uploader.quota.limit
            }
            print(json.dumps(output))
            
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
