#!/usr/bin/env python3
"""
YouTubeUploader - OAuth setup and auto-upload with metadata
Handles OAuth2 flow, video upload, and metadata management
"""

import json
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import urllib.request
import urllib.error
import urllib.parse

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


class YouTubeUploader:
    """YouTube video upload handler with OAuth2."""
    
    # OAuth2 endpoints
    OAUTH_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
    YOUTUBE_UPLOAD_URL = "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status,contentDetails"
    YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
    
    # OAuth scopes needed
    SCOPES = [
        "https://www.googleapis.com/auth/youtube.upload",
        "https://www.googleapis.com/auth/youtube.readonly"
    ]
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # OAuth credentials
        self.client_id = os.environ.get('YOUTUBE_CLIENT_ID', '')
        self.client_secret = os.environ.get('YOUTUBE_CLIENT_SECRET', '')
        
        # Token storage
        self.token_dir = self.pipeline_dir / "tokens"
        self.token_dir.mkdir(exist_ok=True)
        self.token_file = self.token_dir / "youtube_tokens.json"
        
        # Load existing tokens
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = None
        self._load_tokens()
    
    def _load_tokens(self):
        """Load OAuth tokens from file."""
        if self.token_file.exists():
            with open(self.token_file) as f:
                data = json.load(f)
                self.access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')
                self.token_expiry = data.get('expiry')
    
    def _save_tokens(self, access_token: str, refresh_token: str, expires_in: int):
        """Save OAuth tokens to file."""
        import time
        expiry = time.time() + expires_in
        
        data = {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'expiry': expiry,
            'saved_at': datetime.utcnow().isoformat() + 'Z'
        }
        
        with open(self.token_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expiry = expiry
        
        # Secure the file
        os.chmod(self.token_file, 0o600)
    
    def is_authenticated(self) -> bool:
        """Check if we have valid authentication."""
        return self.access_token is not None and self.refresh_token is not None
    
    def get_auth_url(self) -> str:
        """Generate OAuth authorization URL."""
        if not self.client_id:
            raise ValueError("YOUTUBE_CLIENT_ID not set")
        
        params = {
            'client_id': self.client_id,
            'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',  # Out-of-band for CLI
            'response_type': 'code',
            'scope': ' '.join(self.SCOPES),
            'access_type': 'offline',
            'prompt': 'consent'
        }
        
        return f"{self.OAUTH_AUTH_URL}?{urllib.parse.urlencode(params)}"
    
    def exchange_code(self, auth_code: str) -> bool:
        """Exchange authorization code for tokens."""
        if not self.client_id or not self.client_secret:
            raise ValueError("YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET required")
        
        data = {
            'code': auth_code,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'redirect_uri': 'urn:ietf:wg:oauth:2.0:oob',
            'grant_type': 'authorization_code'
        }
        
        try:
            req = urllib.request.Request(
                self.OAUTH_TOKEN_URL,
                data=urllib.parse.urlencode(data).encode('utf-8'),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                access_token = result.get('access_token')
                refresh_token = result.get('refresh_token')
                expires_in = result.get('expires_in', 3600)
                
                if access_token and refresh_token:
                    self._save_tokens(access_token, refresh_token, expires_in)
                    print(f"[YouTubeUploader] Tokens saved successfully")
                    return True
                else:
                    print(f"[YouTubeUploader] Missing tokens in response: {result}")
                    return False
                    
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            print(f"[YouTubeUploader] Token exchange failed: {e.code} - {error_body}")
            return False
        except Exception as e:
            print(f"[YouTubeUploader] Token exchange error: {e}")
            return False
    
    def refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token."""
        if not self.refresh_token:
            print("[YouTubeUploader] No refresh token available")
            return False
        
        if not self.client_id or not self.client_secret:
            raise ValueError("YOUTUBE_CLIENT_ID and YOUTUBE_CLIENT_SECRET required")
        
        data = {
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'refresh_token'
        }
        
        try:
            req = urllib.request.Request(
                self.OAUTH_TOKEN_URL,
                data=urllib.parse.urlencode(data).encode('utf-8'),
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                access_token = result.get('access_token')
                expires_in = result.get('expires_in', 3600)
                
                if access_token:
                    # Keep existing refresh token (may not be returned)
                    self._save_tokens(access_token, self.refresh_token, expires_in)
                    print(f"[YouTubeUploader] Access token refreshed")
                    return True
                else:
                    return False
                    
        except Exception as e:
            print(f"[YouTubeUploader] Token refresh failed: {e}")
            return False
    
    def ensure_valid_token(self) -> bool:
        """Ensure we have a valid access token."""
        import time
        
        if not self.is_authenticated():
            print("[YouTubeUploader] Not authenticated. Run: python scripts/youtube_uploader.py --auth")
            return False
        
        # Check if token is expired or about to expire (5 min buffer)
        if self.token_expiry and time.time() > (self.token_expiry - 300):
            print("[YouTubeUploader] Token expired, refreshing...")
            return self.refresh_access_token()
        
        return True
    
    def build_video_metadata(self, title: str, description: str, 
                            tags: List[str] = None, 
                            category_id: str = "24",  # Entertainment
                            privacy_status: str = "private") -> Dict:
        """Build video metadata for upload."""
        # Add standard tags
        default_tags = ["drama", "commentary", "news", "trending", "shorts"]
        all_tags = list(set((tags or []) + default_tags))
        
        # Truncate title if needed (YouTube limit is 100 chars)
        if len(title) > 100:
            title = title[:97] + "..."
        
        # Build description with standard footer
        full_description = f"{description}\n\n"
        full_description += "#Drama #Commentary #Trending\n\n"
        full_description += "Sources linked in video. Fair use for commentary."
        
        metadata = {
            "snippet": {
                "title": title,
                "description": full_description,
                "tags": all_tags[:15],  # YouTube allows max 15 tags
                "categoryId": category_id
            },
            "status": {
                "privacyStatus": privacy_status,
                "selfDeclaredMadeForKids": False
            },
            "contentDetails": {
                "licensedContent": False
            }
        }
        
        return metadata
    
    def initiate_upload(self, metadata: Dict, video_path: Path) -> Optional[str]:
        """Initiate resumable upload and return upload URL."""
        if not self.ensure_valid_token():
            return None
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "X-Upload-Content-Length": str(video_path.stat().st_size),
            "X-Upload-Content-Type": "video/mp4"
        }
        
        data = json.dumps(metadata).encode('utf-8')
        
        try:
            req = urllib.request.Request(
                self.YOUTUBE_UPLOAD_URL,
                data=data,
                headers=headers,
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                # Get the upload URL from Location header
                upload_url = response.headers.get('Location')
                if upload_url:
                    print(f"[YouTubeUploader] Upload initiated")
                    return upload_url
                else:
                    print("[YouTubeUploader] No upload URL in response")
                    return None
                    
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            print(f"[YouTubeUploader] Upload initiation failed: {e.code} - {error_body}")
            return None
        except Exception as e:
            print(f"[YouTubeUploader] Upload initiation error: {e}")
            return None
    
    def upload_video_file(self, upload_url: str, video_path: Path, 
                         progress_callback=None) -> Optional[Dict]:
        """Upload video file to YouTube."""
        file_size = video_path.stat().st_size
        
        # Read file in chunks
        chunk_size = 1024 * 1024  # 1MB chunks
        uploaded = 0
        
        try:
            with open(video_path, 'rb') as f:
                # For simplicity, upload entire file at once for smaller videos
                # For larger videos, implement chunked upload with resumable sessions
                if file_size <= 50 * 1024 * 1024:  # Under 50MB
                    video_data = f.read()
                    
                    req = urllib.request.Request(
                        upload_url,
                        data=video_data,
                        headers={
                            "Content-Type": "video/mp4",
                            "Content-Length": str(file_size)
                        },
                        method='PUT'
                    )
                    
                    with urllib.request.urlopen(req, timeout=300) as response:
                        result = json.loads(response.read().decode('utf-8'))
                        
                        video_id = result.get('id')
                        print(f"[YouTubeUploader] Upload complete! Video ID: {video_id}")
                        
                        return {
                            'video_id': video_id,
                            'title': result.get('snippet', {}).get('title'),
                            'status': result.get('status', {}).get('privacyStatus'),
                            'url': f"https://youtube.com/shorts/{video_id}"
                        }
                else:
                    # For larger files, would need chunked upload
                    print(f"[YouTubeUploader] Large file upload not yet implemented")
                    return None
                    
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            print(f"[YouTubeUploader] Upload failed: {e.code} - {error_body}")
            return None
        except Exception as e:
            print(f"[YouTubeUploader] Upload error: {e}")
            return None
    
    def upload_video(self, video_path: Path, title: str, description: str,
                    tags: List[str] = None, privacy_status: str = "private") -> Optional[Dict]:
        """Full upload workflow: metadata + video file."""
        print(f"[YouTubeUploader] Starting upload: {video_path.name}")
        
        # Build metadata
        metadata = self.build_video_metadata(title, description, tags, privacy_status=privacy_status)
        
        # Initiate upload
        upload_url = self.initiate_upload(metadata, video_path)
        if not upload_url:
            return None
        
        # Upload video file
        result = self.upload_video_file(upload_url, video_path)
        
        if result:
            # Save upload record
            self._save_upload_record(result, video_path, metadata)
        
        return result
    
    def _save_upload_record(self, result: Dict, video_path: Path, metadata: Dict):
        """Save upload record for tracking."""
        uploads_dir = self.pipeline_dir / "uploads"
        uploads_dir.mkdir(exist_ok=True)
        
        date_str = datetime.now().strftime("%Y-%m-%d")
        record_file = uploads_dir / f"{date_str}.json"
        
        records = []
        if record_file.exists():
            with open(record_file) as f:
                records = json.load(f)
        
        record = {
            'uploaded_at': datetime.utcnow().isoformat() + 'Z',
            'video_id': result.get('video_id'),
            'url': result.get('url'),
            'title': metadata.get('snippet', {}).get('title'),
            'local_path': str(video_path),
            'privacy_status': result.get('status')
        }
        
        records.append(record)
        
        with open(record_file, 'w') as f:
            json.dump(records, f, indent=2)
        
        print(f"[YouTubeUploader] Upload record saved")
    
    def get_channel_info(self) -> Optional[Dict]:
        """Get authenticated channel info."""
        if not self.ensure_valid_token():
            return None
        
        url = f"{self.YOUTUBE_API_BASE}/channels?part=snippet,statistics&mine=true"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            req = urllib.request.Request(url, headers=headers)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                items = result.get('items', [])
                if items:
                    channel = items[0]
                    return {
                        'id': channel.get('id'),
                        'title': channel.get('snippet', {}).get('title'),
                        'description': channel.get('snippet', {}).get('description'),
                        'subscriber_count': channel.get('statistics', {}).get('subscriberCount'),
                        'video_count': channel.get('statistics', {}).get('videoCount')
                    }
                return None
                
        except Exception as e:
            print(f"[YouTubeUploader] Channel info error: {e}")
            return None


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='YouTubeUploader - OAuth and video upload')
    parser.add_argument('--auth', action='store_true', help='Start OAuth authentication flow')
    parser.add_argument('--auth-code', help='Authorization code from OAuth callback')
    parser.add_argument('--upload', help='Path to video file to upload')
    parser.add_argument('--title', help='Video title')
    parser.add_argument('--description', help='Video description')
    parser.add_argument('--tags', help='Comma-separated tags')
    parser.add_argument('--privacy', default='private', 
                       choices=['private', 'unlisted', 'public'],
                       help='Privacy status')
    parser.add_argument('--channel-info', action='store_true', help='Get channel info')
    parser.add_argument('--status', action='store_true', help='Check authentication status')
    
    args = parser.parse_args()
    
    uploader = YouTubeUploader()
    
    if args.status:
        if uploader.is_authenticated():
            print("✅ Authenticated with YouTube")
            channel = uploader.get_channel_info()
            if channel:
                print(f"Channel: {channel.get('title')}")
                print(f"Subscribers: {channel.get('subscriber_count', 'N/A')}")
        else:
            print("❌ Not authenticated")
            print(f"Run: python scripts/youtube_uploader.py --auth")
        return 0
    
    if args.auth:
        auth_url = uploader.get_auth_url()
        print("=" * 60)
        print("YOUTUBE OAUTH AUTHENTICATION")
        print("=" * 60)
        print("\n1. Open this URL in your browser:")
        print(f"\n{auth_url}\n")
        print("2. Sign in and authorize the application")
        print("3. Copy the authorization code")
        print("4. Run: python scripts/youtube_uploader.py --auth-code CODE")
        print("=" * 60)
        return 0
    
    if args.auth_code:
        success = uploader.exchange_code(args.auth_code)
        if success:
            print("✅ Authentication successful!")
            channel = uploader.get_channel_info()
            if channel:
                print(f"Connected to channel: {channel.get('title')}")
            return 0
        else:
            print("❌ Authentication failed")
            return 1
    
    if args.channel_info:
        channel = uploader.get_channel_info()
        if channel:
            print(json.dumps(channel, indent=2))
            return 0
        else:
            print("Failed to get channel info")
            return 1
    
    if args.upload:
        if not uploader.is_authenticated():
            print("❌ Not authenticated. Run with --auth first")
            return 1
        
        video_path = Path(args.upload)
        if not video_path.exists():
            print(f"❌ Video file not found: {video_path}")
            return 1
        
        title = args.title or video_path.stem
        description = args.description or "Drama commentary video"
        tags = args.tags.split(',') if args.tags else []
        
        result = uploader.upload_video(
            video_path=video_path,
            title=title,
            description=description,
            tags=tags,
            privacy_status=args.privacy
        )
        
        if result:
            print("\n✅ Upload successful!")
            print(f"Video ID: {result.get('video_id')}")
            print(f"URL: {result.get('url')}")
            return 0
        else:
            print("\n❌ Upload failed")
            return 1
    
    print("Use --auth to authenticate, --upload to upload a video, or --status to check auth")
    return 0


if __name__ == "__main__":
    sys.exit(main())
