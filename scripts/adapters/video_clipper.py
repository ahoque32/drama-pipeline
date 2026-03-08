#!/usr/bin/env python3
"""
Video Clipper - yt-dlp + ffmpeg Integration
Downloads and trims video clips from 1800+ sites
"""

import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse


class VideoClipper:
    """Download and trim video clips using yt-dlp and ffmpeg."""
    
    MAX_CLIP_DURATION = 30  # seconds - max clip length for Shorts content
    
    def __init__(self):
        # Paths to tools
        self.yt_dlp_path = os.environ.get('YT_DLP_PATH', '/opt/homebrew/bin/yt-dlp')
        self.ffmpeg_path = os.environ.get('FFMPEG_PATH', '/opt/homebrew/bin/ffmpeg')
        
        # Verify tools exist
        self._verify_tools()
    
    def _verify_tools(self):
        """Check that yt-dlp and ffmpeg are available."""
        for tool, path in [('yt-dlp', self.yt_dlp_path), ('ffmpeg', self.ffmpeg_path)]:
            if not os.path.exists(path):
                # Try to find in PATH
                try:
                    result = subprocess.run(['which', tool], capture_output=True, text=True)
                    if result.returncode == 0:
                        if tool == 'yt-dlp':
                            self.yt_dlp_path = result.stdout.strip()
                        else:
                            self.ffmpeg_path = result.stdout.strip()
                        print(f"[VideoClipper] Found {tool} at: {result.stdout.strip()}")
                    else:
                        print(f"[VideoClipper] WARNING: {tool} not found at {path}")
                except Exception as e:
                    print(f"[VideoClipper] WARNING: Could not verify {tool}: {e}")
            else:
                print(f"[VideoClipper] {tool} verified at: {path}")
    
    def _run_yt_dlp(self, url: str, output_path: Path, options: List[str] = None) -> bool:
        """Run yt-dlp to download video."""
        cmd = [
            self.yt_dlp_path,
            '-o', str(output_path),
            '--no-playlist',
            '--format', 'bestvideo[height<=1080]+bestaudio/best[height<=1080]/best',
            '--merge-output-format', 'mp4',
            '--retries', '5',
            '--fragment-retries', '5',
            '-4',
        ]

        # Optional auth for private/restricted platforms (Instagram, etc.)
        cookies_file = os.environ.get('YT_DLP_COOKIES_FILE') or os.environ.get('IG_COOKIES_FILE')
        cookies_browser = os.environ.get('YT_DLP_COOKIES_FROM_BROWSER')
        if cookies_file:
            cmd.extend(['--cookies', cookies_file])
        else:
            # Default to browser cookies for better YouTube/IG reliability
            cmd.extend(['--cookies-from-browser', cookies_browser or 'chrome'])
        
        if options:
            cmd.extend(options)
        
        cmd.append(url)
        
        print(f"[VideoClipper] Running: {' '.join(cmd[:6])}... {url}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print(f"[VideoClipper] Download successful: {output_path}")
                return True
            else:
                print(f"[VideoClipper] Download failed: {result.stderr[:500]}")
                return False
        except subprocess.TimeoutExpired:
            print(f"[VideoClipper] Download timed out after 5 minutes")
            return False
        except Exception as e:
            print(f"[VideoClipper] Download error: {e}")
            return False
    
    def _run_ffmpeg(self, input_path: Path, output_path: Path, 
                    start_time: float = None, duration: float = None,
                    extract_thumbnail: bool = False) -> bool:
        """Run ffmpeg to trim video or extract thumbnail."""
        cmd = [self.ffmpeg_path, '-y', '-i', str(input_path)]
        
        if extract_thumbnail:
            # Extract thumbnail at 1 second
            cmd.extend(['-ss', '1', '-vframes', '1'])
        else:
            # Trim video
            if start_time is not None:
                cmd.extend(['-ss', str(start_time)])
            if duration is not None:
                cmd.extend(['-t', str(duration)])
            # Copy codec for speed, but re-encode if trimming
            if start_time is not None or duration is not None:
                cmd.extend(['-c:v', 'libx264', '-c:a', 'aac', '-preset', 'fast'])
            else:
                cmd.extend(['-c', 'copy'])
        
        cmd.append(str(output_path))
        
        print(f"[VideoClipper] Running ffmpeg: {' '.join(cmd[:8])}...")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode == 0:
                print(f"[VideoClipper] ffmpeg success: {output_path}")
                return True
            else:
                print(f"[VideoClipper] ffmpeg failed: {result.stderr[:500]}")
                return False
        except Exception as e:
            print(f"[VideoClipper] ffmpeg error: {e}")
            return False
    
    def _get_video_duration(self, video_path: Path) -> float:
        """Get video duration using ffprobe."""
        ffprobe_path = self.ffmpeg_path.replace('ffmpeg', 'ffprobe')
        
        cmd = [
            ffprobe_path,
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            str(video_path)
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return float(result.stdout.strip())
        except Exception as e:
            print(f"[VideoClipper] ffprobe error: {e}")
        
        return 0.0
    
    def _sanitize_filename(self, text: str) -> str:
        """Create safe filename from text."""
        # Remove non-alphanumeric chars, keep spaces
        safe = re.sub(r'[^\w\s-]', '', text)
        # Replace spaces with underscores
        safe = safe.replace(' ', '_')
        # Limit length
        return safe[:50]
    
    def download_video(self, url: str, output_name: str = None,
                       trim_start: float = None, trim_duration: float = None,
                       max_duration: float = None) -> Dict:
        """
        Download video from URL and optionally trim it.
        
        Args:
            url: Video URL (YouTube, TikTok, X/Twitter, Instagram, etc.)
            output_name: Base name for output files (without extension)
            trim_start: Start time in seconds for trimming
            trim_duration: Duration in seconds for trimming
            max_duration: Maximum allowed duration (will trim if longer)
        
        Returns:
            Dict with success, video_path, thumbnail_path, duration, source_url
        """
        print(f"[VideoClipper] Downloading video from: {url}")
        
        # Create output directory
        date_str = datetime.now().strftime('%Y-%m-%d')
        # Use the canonical pipeline dir (Dante workspace)
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from utils import get_pipeline_dir
            pipeline_dir = get_pipeline_dir()
        except:
            pipeline_dir = Path.home() / 'drama-pipeline'
        clips_dir = pipeline_dir / 'assets' / date_str / 'clips'
        clips_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate output name if not provided
        if not output_name:
            parsed = urlparse(url)
            domain = parsed.netloc.replace('www.', '').split('.')[0]
            timestamp = datetime.now().strftime('%H%M%S')
            output_name = f"{domain}_{timestamp}"
        
        output_name = self._sanitize_filename(output_name)
        
        # Download to temp location first
        temp_path = clips_dir / f"{output_name}_temp.mp4"
        final_path = clips_dir / f"{output_name}.mp4"
        thumbnail_path = clips_dir / f"{output_name}_thumb.jpg"
        
        # Download video
        if not self._run_yt_dlp(url, temp_path):
            # Clean up temp file if exists
            if temp_path.exists():
                temp_path.unlink()
            
            return {
                'success': False,
                'video_path': None,
                'thumbnail_path': None,
                'duration': 0.0,
                'source_url': url,
                'error': 'Download failed'
            }
        
        # Get actual duration
        actual_duration = self._get_video_duration(temp_path)
        print(f"[VideoClipper] Downloaded video duration: {actual_duration:.1f}s")
        
        # Determine trimming
        needs_trim = False
        start_time = trim_start or 0
        duration = trim_duration
        
        if max_duration and actual_duration > max_duration:
            print(f"[VideoClipper] Video exceeds max duration ({max_duration}s), will trim")
            needs_trim = True
            duration = max_duration
        elif trim_duration:
            needs_trim = True
            duration = trim_duration
        
        # Apply trimming if needed
        if needs_trim:
            print(f"[VideoClipper] Trimming: start={start_time}s, duration={duration}s")
            if self._run_ffmpeg(temp_path, final_path, start_time, duration):
                # Remove temp file
                temp_path.unlink()
                final_duration = duration
            else:
                # Fallback: use untrimmed
                print(f"[VideoClipper] Trim failed, using untrimmed")
                temp_path.rename(final_path)
                final_duration = actual_duration
        else:
            # No trimming needed
            temp_path.rename(final_path)
            final_duration = actual_duration
        
        # Extract thumbnail
        print(f"[VideoClipper] Extracting thumbnail...")
        self._run_ffmpeg(final_path, thumbnail_path, extract_thumbnail=True)
        
        # Verify files exist
        video_exists = final_path.exists()
        thumb_exists = thumbnail_path.exists()
        
        if not video_exists:
            return {
                'success': False,
                'video_path': None,
                'thumbnail_path': None,
                'duration': 0.0,
                'source_url': url,
                'error': 'Output file not created'
            }
        
        print(f"[VideoClipper] Complete: {final_path}")
        
        return {
            'success': True,
            'video_path': str(final_path),
            'thumbnail_path': str(thumbnail_path) if thumb_exists else None,
            'duration': final_duration,
            'source_url': url,
            'trimmed': needs_trim,
            'original_duration': actual_duration
        }
    
    def download_x_video(self, tweet_url: str) -> Dict:
        """
        Download video from X/Twitter post.
        X videos are handled by yt-dlp natively.
        """
        print(f"[VideoClipper] Downloading X video: {tweet_url}")
        
        # Extract tweet ID for filename
        tweet_id = None
        match = re.search(r'status/(\d+)', tweet_url)
        if match:
            tweet_id = match.group(1)
        
        output_name = f"x_video_{tweet_id}" if tweet_id else "x_video"
        
        # X videos for Shorts should be max 15 seconds
        return self.download_video(
            url=tweet_url,
            output_name=output_name,
            max_duration=self.MAX_CLIP_DURATION
        )
    
    def download_tiktok(self, url: str) -> Dict:
        """Download TikTok video."""
        print(f"[VideoClipper] Downloading TikTok: {url}")
        
        # Extract username/video_id for filename
        match = re.search(r'tiktok\.com/@([^/]+)/video/(\d+)', url)
        if match:
            username, video_id = match.groups()
            output_name = f"tiktok_{username}_{video_id[:10]}"
        else:
            output_name = "tiktok_video"
        
        return self.download_video(
            url=url,
            output_name=output_name,
            max_duration=self.MAX_CLIP_DURATION
        )
    
    def download_youtube_clip(self, url: str, start: float = None, 
                              duration: float = None) -> Dict:
        """
        Download YouTube video or clip.
        For Shorts, we auto-trim to MAX_CLIP_DURATION.
        """
        print(f"[VideoClipper] Downloading YouTube: {url}")
        
        # Check if it's a Shorts URL
        is_short = 'shorts' in url.lower()
        
        # Extract video ID
        video_id = None
        patterns = [
            r'youtube\.com/shorts/([\w-]+)',
            r'youtube\.com/watch\?v=([\w-]+)',
            r'youtu\.be/([\w-]+)'
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                video_id = match.group(1)
                break
        
        output_name = f"yt_{video_id}" if video_id else "youtube_video"
        
        max_dur = self.MAX_CLIP_DURATION if is_short else None
        
        return self.download_video(
            url=url,
            output_name=output_name,
            trim_start=start,
            trim_duration=duration,
            max_duration=max_dur
        )
    

    def search_and_clip(self, query: str, max_duration: int = 15, max_results: int = 3) -> Optional[Dict]:
        """Search YouTube for a topic and download the best short clip."""
        import subprocess
        
        if not self.yt_dlp_path:
            return {'success': False, 'error': 'yt-dlp not found'}
        
        try:
            # Search YouTube
            search_query = f"ytsearch{max_results}:{query}"
            
            # First get video info
            search_cmd = [self.yt_dlp_path, '--dump-json', '--no-download', search_query]
            cookies_browser = os.environ.get('YT_DLP_COOKIES_FROM_BROWSER')
            if cookies_browser:
                search_cmd.extend(['--cookies-from-browser', cookies_browser])
            result = subprocess.run(
                search_cmd,
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode != 0:
                return {'success': False, 'error': f'Search failed: {result.stderr[:100]}'}
            
            # Parse results — each line is a JSON object
            videos = []
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        videos.append(json.loads(line))
                    except:
                        pass
            
            if not videos:
                return {'success': False, 'error': 'No YouTube results'}
            
            # Pick shortest video under 10 minutes, skip anything over 10 min
            best = None
            for v in videos:
                dur = v.get('duration', 999)
                if dur and dur < 600:  # Under 10 min
                    if best is None or dur < best.get('duration', 999):
                        best = v
            
            if not best:
                # All results too long — skip
                return {'success': False, 'error': 'All YouTube results over 10 min, skipping'}
            
            url = best.get('webpage_url') or best.get('url')
            title = best.get('title', 'unknown')
            duration = best.get('duration', 0)
            
            print(f"[VideoClipper] Found: {title} ({duration}s)")
            
            # Download a 15s clip from the most engaging part (30% in)
            start_time = max(0, int(duration * 0.3)) if duration > max_duration else 0
            
            return self.download_youtube_clip(
                url, 
                start=start_time, 
                duration=max_duration,
                
            )
            
        except subprocess.TimeoutExpired:
            return {'success': False, 'error': 'YouTube search timed out'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def download_instagram(self, url: str) -> Dict:
        """Download Instagram video/reel."""
        print(f"[VideoClipper] Downloading Instagram: {url}")
        
        output_name = "instagram_video"
        match = re.search(r'instagram\.com/(?:p|reel)/([^/]+)', url)
        if match:
            output_name = f"ig_{match.group(1)[:15]}"
        
        return self.download_video(
            url=url,
            output_name=output_name,
            max_duration=self.MAX_CLIP_DURATION
        )
    
    def auto_download(self, url: str) -> Dict:
        """
        Auto-detect platform and download appropriately.
        Main entry point for generic URLs.
        """
        url_lower = url.lower()
        
        if 'x.com' in url_lower or 'twitter.com' in url_lower:
            return self.download_x_video(url)
        elif 'tiktok.com' in url_lower:
            return self.download_tiktok(url)
        elif 'youtube.com' in url_lower or 'youtu.be' in url_lower:
            return self.download_youtube_clip(url)
        elif 'instagram.com' in url_lower:
            return self.download_instagram(url)
        else:
            # Generic download
            return self.download_video(url, max_duration=self.MAX_CLIP_DURATION)


# Convenience functions
def download_clip(url: str, **kwargs) -> Dict:
    """Download video clip from URL."""
    clipper = VideoClipper()
    return clipper.download_video(url, **kwargs)


def auto_download(url: str) -> Dict:
    """Auto-detect platform and download video."""
    clipper = VideoClipper()
    return clipper.auto_download(url)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        url = sys.argv[1]
        result = auto_download(url)
        print(json.dumps(result, indent=2))
    else:
        print("Usage: python video_clipper.py <url>")
