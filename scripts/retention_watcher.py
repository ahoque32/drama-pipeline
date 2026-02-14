#!/usr/bin/env python3
"""
RetentionWatcher - YouTube Analytics integration for performance tracking
Monitors video performance, retention metrics, and generates reports

Usage:
    python scripts/retention_watcher.py --video-id ABC123  # Single video
    python scripts/retention_watcher.py --date 2026-02-14  # All videos from date
    python scripts/retention_watcher.py --weekly-report    # Weekly insights
"""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger('retention_watcher')


@dataclass
class VideoMetrics:
    """Video performance metrics."""
    video_id: str
    title: str
    published_at: str
    
    # Basic metrics
    views: int = 0
    likes: int = 0
    comments: int = 0
    subscribers_gained: int = 0
    subscribers_lost: int = 0
    
    # Retention metrics
    avg_view_duration_sec: float = 0.0
    video_length_sec: float = 0.0
    retention_percentage: float = 0.0
    
    # Retention graph data
    retention_graph: List[Dict] = field(default_factory=list)
    cliff_points: List[Dict] = field(default_factory=list)
    
    # Status
    status: str = "unknown"  # excellent (>95%), good (70-95%), warning (<70%)
    alerts: List[str] = field(default_factory=list)


@dataclass
class WinningFormula:
    """Saved winning formula for high-performing videos."""
    video_id: str
    title: str
    retention_percentage: float
    video_length_sec: float
    topic_category: str
    hook_structure: str
    saved_at: str = field(default_factory=lambda: datetime.now().isoformat())


class RetentionWatcher:
    """YouTube Analytics monitoring and performance tracking."""
    
    # Paths
    CREDENTIALS_DIR = Path.home() / '.openclaw' / 'credentials'
    TOKEN_PATH = CREDENTIALS_DIR / 'youtube-token.json'
    
    # API settings
    ANALYTICS_API_BASE = "https://youtubeanalytics.googleapis.com/v2"
    DATA_API_BASE = "https://www.googleapis.com/youtube/v3"
    SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly',
              'https://www.googleapis.com/auth/youtube.readonly']
    
    # Retention thresholds
    RETENTION_EXCELLENT = 95.0
    RETENTION_GOOD = 70.0
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # Setup directories
        self.analytics_dir = self.pipeline_dir / "analytics"
        self.analytics_dir.mkdir(exist_ok=True)
        self.templates_dir = self.pipeline_dir / "templates"
        self.templates_dir.mkdir(exist_ok=True)
        self.uploads_dir = self.pipeline_dir / "uploads"
        
        # Load credentials
        self.credentials = None
        self.access_token = None
        self._load_credentials()
    
    def _load_credentials(self) -> None:
        """Load OAuth credentials from token file."""
        if not self.TOKEN_PATH.exists():
            logger.warning(f"Token not found at {self.TOKEN_PATH}")
            logger.info("Run youtube_uploader.py first to authenticate")
            return
        
        try:
            with open(self.TOKEN_PATH) as f:
                token_data = json.load(f)
                self.access_token = token_data.get('token')
                if not self.access_token:
                    # Try alternate format
                    self.access_token = token_data.get('access_token')
                logger.info("Loaded YouTube credentials")
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
    
    def _make_api_request(self, url: str, headers: Optional[Dict] = None) -> Optional[Dict]:
        """Make authenticated API request."""
        if not self.access_token:
            logger.error("No access token available")
            return None
        
        import urllib.request
        import urllib.error
        
        request_headers = headers or {}
        request_headers['Authorization'] = f'Bearer {self.access_token}'
        request_headers['Accept'] = 'application/json'
        
        try:
            req = urllib.request.Request(url, headers=request_headers)
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            logger.error(f"API error {e.code}: {error_body}")
            if e.code == 401:
                logger.error("Token expired - re-authenticate with youtube_uploader.py")
            return None
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def _parse_duration(self, duration_iso: str) -> float:
        """Parse ISO 8601 duration (PT#M#S) to seconds."""
        if not duration_iso:
            return 0.0
        
        # Pattern: PT#M#S or PT#H#M#S
        pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
        match = re.match(pattern, duration_iso)
        
        if not match:
            return 0.0
        
        hours = int(match.group(1) or 0)
        minutes = int(match.group(2) or 0)
        seconds = int(match.group(3) or 0)
        
        return hours * 3600 + minutes * 60 + seconds
    
    def get_video_info(self, video_id: str) -> Optional[Dict]:
        """Get video info from YouTube Data API."""
        url = f"{self.DATA_API_BASE}/videos?part=snippet,contentDetails,statistics&id={video_id}"
        
        data = self._make_api_request(url)
        if not data or 'items' not in data or not data['items']:
            return None
        
        item = data['items'][0]
        snippet = item.get('snippet', {})
        content = item.get('contentDetails', {})
        stats = item.get('statistics', {})
        
        return {
            'video_id': video_id,
            'title': snippet.get('title', 'Unknown'),
            'published_at': snippet.get('publishedAt', ''),
            'duration_iso': content.get('duration', 'PT0S'),
            'duration_sec': self._parse_duration(content.get('duration', 'PT0S')),
            'views': int(stats.get('viewCount', 0)),
            'likes': int(stats.get('likeCount', 0)),
            'comments': int(stats.get('commentCount', 0))
        }
    
    def get_video_analytics(self, video_id: str, start_date: str = None,
                           end_date: str = None) -> Optional[Dict]:
        """Get analytics data for a specific video."""
        if not self.access_token:
            logger.error("Not authenticated")
            return None
        
        # Default date range
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Get last 28 days of data
            start_date = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
        
        # Build metrics query
        metrics = "views,likes,comments,subscribersGained,subscribersLost,estimatedMinutesWatched,averageViewDuration"
        
        params = {
            'ids': 'channel==MINE',
            'startDate': start_date,
            'endDate': end_date,
            'metrics': metrics,
            'filters': f'video=={video_id}'
        }
        
        query_string = '&'.join(f"{k}={v}" for k, v in params.items())
        url = f"{self.ANALYTICS_API_BASE}/reports?{query_string}"
        
        data = self._make_api_request(url)
        if not data:
            return None
        
        # Parse column headers and rows
        headers = data.get('columnHeaders', [])
        rows = data.get('rows', [])
        
        if not rows:
            return {
                'views': 0, 'likes': 0, 'comments': 0,
                'subscribersGained': 0, 'subscribersLost': 0,
                'estimatedMinutesWatched': 0, 'averageViewDuration': 0
            }
        
        # Build column index map
        col_map = {h['name']: i for i, h in enumerate(headers)}
        
        # Sum up totals from all rows
        totals = {
            'views': sum(int(row[col_map.get('views', 0)] or 0) for row in rows),
            'likes': sum(int(row[col_map.get('likes', 1)] or 0) for row in rows),
            'comments': sum(int(row[col_map.get('comments', 2)] or 0) for row in rows),
            'subscribersGained': sum(int(row[col_map.get('subscribersGained', 3)] or 0) for row in rows),
            'subscribersLost': sum(int(row[col_map.get('subscribersLost', 4)] or 0) for row in rows),
            'estimatedMinutesWatched': sum(float(row[col_map.get('estimatedMinutesWatched', 5)] or 0) for row in rows),
            'averageViewDuration': sum(float(row[col_map.get('averageViewDuration', 6)] or 0) for row in rows) / len(rows)
        }
        
        return totals
    
    def get_retention_graph(self, video_id: str) -> List[Dict]:
        """Get audience retention graph data."""
        if not self.access_token:
            return []
        
        # Get retention data by elapsed video time
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")
        
        params = {
            'ids': 'channel==MINE',
            'startDate': start_date,
            'endDate': end_date,
            'metrics': 'audienceWatchRatio,relativeRetentionPerformance',
            'filters': f'video=={video_id}',
            'dimensions': 'elapsedVideoTimeRatio'
        }
        
        query_string = '&'.join(f"{k}={v}" for k, v in params.items())
        url = f"{self.ANALYTICS_API_BASE}/reports?{query_string}"
        
        data = self._make_api_request(url)
        if not data or 'rows' not in data:
            return []
        
        headers = data.get('columnHeaders', [])
        col_map = {h['name']: i for i, h in enumerate(headers)}
        
        graph_data = []
        for row in data['rows']:
            graph_data.append({
                'elapsed_ratio': float(row[col_map.get('elapsedVideoTimeRatio', 0)]),
                'watch_ratio': float(row[col_map.get('audienceWatchRatio', 1)] or 0),
                'retention_performance': row[col_map.get('relativeRetentionPerformance', 2)] if len(row) > 2 else None
            })
        
        return graph_data
    
    def identify_cliff_points(self, retention_graph: List[Dict]) -> List[Dict]:
        """Identify significant drop-off points in retention."""
        if len(retention_graph) < 2:
            return []
        
        cliff_points = []
        
        for i in range(1, len(retention_graph)):
            prev = retention_graph[i - 1]
            curr = retention_graph[i]
            
            prev_watch = prev.get('watch_ratio', 0)
            curr_watch = curr.get('watch_ratio', 0)
            
            # Calculate drop percentage
            if prev_watch > 0:
                drop_pct = ((prev_watch - curr_watch) / prev_watch) * 100
                
                # Significant cliff: > 15% drop
                if drop_pct > 15:
                    cliff_points.append({
                        'time_ratio': curr['elapsed_ratio'],
                        'drop_percentage': round(drop_pct, 1),
                        'from_watch_ratio': round(prev_watch, 3),
                        'to_watch_ratio': round(curr_watch, 3)
                    })
        
        return cliff_points
    
    def calculate_retention_percentage(self, avg_duration: float, video_length: float) -> float:
        """Calculate retention percentage from average view duration."""
        if video_length <= 0:
            return 0.0
        return round((avg_duration / video_length) * 100, 1)
    
    def determine_status(self, retention_pct: float) -> Tuple[str, List[str]]:
        """Determine video status and alerts based on retention."""
        alerts = []
        
        if retention_pct >= self.RETENTION_EXCELLENT:
            status = "excellent"
        elif retention_pct >= self.RETENTION_GOOD:
            status = "good"
        else:
            status = "warning"
            alerts.append(f"⚠️ Retention below {self.RETENTION_GOOD}% - Post-mortem recommended")
        
        return status, alerts
    
    def check_video(self, video_id: str) -> Optional[VideoMetrics]:
        """Perform full analysis on a single video."""
        logger.info(f"Analyzing video: {video_id}")
        
        # Get video info
        info = self.get_video_info(video_id)
        if not info:
            logger.error(f"Could not fetch video info for {video_id}")
            return None
        
        # Get analytics
        analytics = self.get_video_analytics(video_id)
        if not analytics:
            logger.warning(f"No analytics data for {video_id}")
            analytics = {}
        
        # Get retention graph
        retention_graph = self.get_retention_graph(video_id)
        cliff_points = self.identify_cliff_points(retention_graph)
        
        # Calculate retention percentage
        avg_duration = analytics.get('averageViewDuration', 0)
        video_length = info.get('duration_sec', 0)
        retention_pct = self.calculate_retention_percentage(avg_duration, video_length)
        
        # Determine status
        status, alerts = self.determine_status(retention_pct)
        
        # Build metrics object
        metrics = VideoMetrics(
            video_id=video_id,
            title=info.get('title', 'Unknown'),
            published_at=info.get('published_at', ''),
            views=analytics.get('views', info.get('views', 0)),
            likes=analytics.get('likes', info.get('likes', 0)),
            comments=analytics.get('comments', info.get('comments', 0)),
            subscribers_gained=analytics.get('subscribersGained', 0),
            subscribers_lost=analytics.get('subscribersLost', 0),
            avg_view_duration_sec=round(avg_duration, 1),
            video_length_sec=video_length,
            retention_percentage=retention_pct,
            retention_graph=retention_graph,
            cliff_points=cliff_points,
            status=status,
            alerts=alerts
        )
        
        # Save winning formula if excellent
        if status == "excellent":
            self.save_winning_formula(metrics)
        
        # Generate post-mortem if warning
        if status == "warning":
            self.generate_post_mortem(metrics)
        
        return metrics
    
    def load_uploads_for_date(self, date_str: str) -> List[Dict]:
        """Load upload records for a specific date."""
        uploads_file = self.uploads_dir / f"{date_str}.json"
        
        if not uploads_file.exists():
            logger.warning(f"No uploads found for {date_str}")
            return []
        
        try:
            with open(uploads_file) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load uploads: {e}")
            return []
    
    def check_date(self, date_str: str) -> Dict:
        """Check all videos from a specific date."""
        logger.info(f"Checking all videos for date: {date_str}")
        
        uploads = self.load_uploads_for_date(date_str)
        if not uploads:
            return {
                'date': date_str,
                'videos': [],
                'summary': {'total': 0, 'excellent': 0, 'good': 0, 'warning': 0}
            }
        
        results = []
        for upload in uploads:
            video_id = upload.get('video_id')
            if video_id:
                metrics = self.check_video(video_id)
                if metrics:
                    results.append(metrics)
        
        # Build summary
        summary = {
            'total': len(results),
            'excellent': sum(1 for r in results if r.status == 'excellent'),
            'good': sum(1 for r in results if r.status == 'good'),
            'warning': sum(1 for r in results if r.status == 'warning'),
            'avg_retention': round(sum(r.retention_percentage for r in results) / len(results), 1) if results else 0
        }
        
        # Save results
        output = {
            'date': date_str,
            'checked_at': datetime.now().isoformat(),
            'videos': [self._metrics_to_dict(m) for m in results],
            'summary': summary
        }
        
        output_file = self.analytics_dir / f"{date_str}.json"
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        logger.info(f"Saved analytics to {output_file}")
        return output
    
    def _metrics_to_dict(self, metrics: VideoMetrics) -> Dict:
        """Convert VideoMetrics to dictionary."""
        return {
            'video_id': metrics.video_id,
            'title': metrics.title,
            'published_at': metrics.published_at,
            'metrics': {
                'views': metrics.views,
                'likes': metrics.likes,
                'comments': metrics.comments,
                'avg_view_duration_sec': metrics.avg_view_duration_sec,
                'subscribers_gained': metrics.subscribers_gained,
                'subscribers_lost': metrics.subscribers_lost
            },
            'retention': {
                'video_length_sec': metrics.video_length_sec,
                'retention_percentage': metrics.retention_percentage,
                'cliff_points': metrics.cliff_points,
                'graph_data': metrics.retention_graph[:10]  # Limit graph data
            },
            'status': metrics.status,
            'alerts': metrics.alerts
        }
    
    def save_winning_formula(self, metrics: VideoMetrics) -> None:
        """Save a winning formula for high-performing videos."""
        formulas_file = self.templates_dir / "winning-formulas.json"
        
        # Load existing formulas
        formulas = []
        if formulas_file.exists():
            try:
                with open(formulas_file) as f:
                    formulas = json.load(f)
            except:
                formulas = []
        
        # Extract hook structure (first 30 chars of title as proxy)
        hook = metrics.title[:50] if metrics.title else "Unknown"
        
        # Determine topic category from title keywords
        topic = self._categorize_topic(metrics.title)
        
        formula = {
            'video_id': metrics.video_id,
            'title': metrics.title,
            'retention_percentage': metrics.retention_percentage,
            'video_length_sec': metrics.video_length_sec,
            'topic_category': topic,
            'hook_structure': hook,
            'saved_at': datetime.now().isoformat()
        }
        
        # Check if already saved
        existing_ids = {f['video_id'] for f in formulas}
        if metrics.video_id not in existing_ids:
            formulas.append(formula)
            
            with open(formulas_file, 'w') as f:
                json.dump(formulas, f, indent=2)
            
            logger.info(f"💎 Saved winning formula for {metrics.video_id} ({metrics.retention_percentage}% retention)")
    
    def _categorize_topic(self, title: str) -> str:
        """Categorize video topic from title."""
        title_lower = title.lower()
        
        categories = {
            'celebrity': ['celebrity', 'star', 'actor', 'singer', 'rapper', 'kardashian'],
            'drama': ['drama', 'beef', 'feud', 'fight', 'argument', 'controversy'],
            'breakup': ['breakup', 'divorce', 'split', 'cheating', 'affair'],
            'social_media': ['tweet', 'instagram', 'tiktok', 'viral', 'post'],
            'legal': ['lawsuit', 'court', 'arrested', 'charged', 'guilty'],
            'money': ['money', 'million', 'billion', 'rich', 'broke', 'debt']
        }
        
        for category, keywords in categories.items():
            if any(kw in title_lower for kw in keywords):
                return category
        
        return 'general'
    
    def generate_post_mortem(self, metrics: VideoMetrics) -> Path:
        """Generate post-mortem analysis for low retention videos."""
        post_mortems_dir = self.analytics_dir / "post-mortems"
        post_mortems_dir.mkdir(exist_ok=True)
        
        post_mortem = {
            'video_id': metrics.video_id,
            'title': metrics.title,
            'analysis_date': datetime.now().isoformat(),
            'issues': [],
            'recommendations': []
        }
        
        # Analyze issues
        if metrics.retention_percentage < 50:
            post_mortem['issues'].append("Severe retention drop - hook may be weak")
            post_mortem['recommendations'].append("Rewrite hook with stronger curiosity gap")
        elif metrics.retention_percentage < 70:
            post_mortem['issues'].append("Below average retention")
            post_mortem['recommendations'].append("Test different pacing in first 15 seconds")
        
        # Analyze cliff points
        if metrics.cliff_points:
            earliest_cliff = min(metrics.cliff_points, key=lambda x: x['time_ratio'])
            if earliest_cliff['time_ratio'] < 0.3:
                post_mortem['issues'].append(f"Early drop-off at {int(earliest_cliff['time_ratio'] * 100)}%")
                post_mortem['recommendations'].append("Front-load the most compelling information")
        
        # Length analysis
        if metrics.video_length_sec > 90:
            post_mortem['issues'].append("Video may be too long for Shorts format")
            post_mortem['recommendations'].append("Target 45-60 seconds for optimal retention")
        
        # Save post-mortem
        output_file = post_mortems_dir / f"{metrics.video_id}.json"
        with open(output_file, 'w') as f:
            json.dump(post_mortem, f, indent=2)
        
        logger.info(f"📝 Generated post-mortem for {metrics.video_id}")
        return output_file
    
    def generate_weekly_report(self) -> Path:
        """Generate weekly performance report."""
        logger.info("Generating weekly report...")
        
        # Get last 7 days of analytics files
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        all_videos = []
        for i in range(7):
            date_str = (end_date - timedelta(days=i)).strftime("%Y-%m-%d")
            analytics_file = self.analytics_dir / f"{date_str}.json"
            
            if analytics_file.exists():
                try:
                    with open(analytics_file) as f:
                        data = json.load(f)
                        all_videos.extend(data.get('videos', []))
                except:
                    pass
        
        # Also check uploads directory for any videos not yet analyzed
        for uploads_file in self.uploads_dir.glob("*.json"):
            try:
                with open(uploads_file) as f:
                    uploads = json.load(f)
                    for upload in uploads:
                        video_id = upload.get('video_id')
                        if video_id and not any(v['video_id'] == video_id for v in all_videos):
                            metrics = self.check_video(video_id)
                            if metrics:
                                all_videos.append(self._metrics_to_dict(metrics))
            except:
                pass
        
        if not all_videos:
            logger.warning("No videos found for weekly report")
            return None
        
        # Calculate summary
        total_videos = len(all_videos)
        avg_retention = sum(v['retention']['retention_percentage'] for v in all_videos) / total_videos
        excellent_count = sum(1 for v in all_videos if v['status'] == 'excellent')
        warning_count = sum(1 for v in all_videos if v['status'] == 'warning')
        total_views = sum(v['metrics']['views'] for v in all_videos)
        total_likes = sum(v['metrics']['likes'] for v in all_videos)
        
        # Sort by retention for top performers
        sorted_videos = sorted(all_videos, key=lambda x: x['retention']['retention_percentage'], reverse=True)
        top_performers = sorted_videos[:5]
        
        # Get warnings
        warnings = [v for v in all_videos if v['status'] == 'warning']
        
        # Generate markdown report
        report_date = datetime.now().strftime("%Y-%m-%d")
        report_lines = [
            f"# Weekly Performance Report",
            f"",
            f"Generated: {report_date}",
            f"Period: Last 7 days",
            f"",
            f"## Summary",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Videos Analyzed | {total_videos} |",
            f"| Average Retention | {avg_retention:.1f}% |",
            f"| Excellent (>95%) | {excellent_count} |",
            f"| Good (70-95%) | {total_videos - excellent_count - warning_count} |",
            f"| Warning (<70%) | {warning_count} |",
            f"| Total Views | {total_views:,} |",
            f"| Total Likes | {total_likes:,} |",
            f"",
            f"## Top Performers",
            f""
        ]
        
        for i, video in enumerate(top_performers, 1):
            report_lines.extend([
                f"### {i}. {video['title'][:60]}",
                f"- Video ID: {video['video_id']}",
                f"- Retention: {video['retention']['retention_percentage']}%",
                f"- Views: {video['metrics']['views']:,}",
                f"- Duration: {video['retention']['video_length_sec']}s",
                f""
            ])
        
        if warnings:
            report_lines.extend([
                f"## Post-Mortems Required",
                f""
            ])
            for video in warnings:
                report_lines.extend([
                    f"- **{video['title'][:50]}** ({video['video_id']})",
                    f"  - Retention: {video['retention']['retention_percentage']}%",
                    f"  - Alerts: {', '.join(video['alerts'])}",
                    f""
                ])
        
        # Load winning formulas
        formulas_file = self.templates_dir / "winning-formulas.json"
        if formulas_file.exists():
            try:
                with open(formulas_file) as f:
                    formulas = json.load(f)
                
                recent_formulas = [f for f in formulas 
                                  if (datetime.now() - datetime.fromisoformat(f['saved_at'])).days <= 7]
                
                if recent_formulas:
                    report_lines.extend([
                        f"## Winning Formulas Saved",
                        f""
                    ])
                    for formula in recent_formulas[-5:]:
                        report_lines.extend([
                            f"- **{formula['title'][:50]}**",
                            f"  - Retention: {formula['retention_percentage']}%",
                            f"  - Category: {formula['topic_category']}",
                            f""
                        ])
            except:
                pass
        
        # Save report
        report_path = self.analytics_dir / "weekly-insights.md"
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"📊 Weekly report saved to {report_path}")
        return report_path


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='RetentionWatcher - YouTube Analytics performance tracking',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --video-id ABC123              # Analyze single video
  %(prog)s --date 2026-02-14              # Check all videos from date
  %(prog)s --weekly-report                # Generate weekly report
  %(prog)s --video-id ABC123 --verbose    # Verbose output
        """
    )
    
    parser.add_argument('--video-id', help='Analyze specific video ID')
    parser.add_argument('--date', help='Check all videos from date (YYYY-MM-DD)')
    parser.add_argument('--weekly-report', action='store_true', help='Generate weekly report')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    watcher = RetentionWatcher()
    
    if args.video_id:
        metrics = watcher.check_video(args.video_id)
        if metrics:
            print(json.dumps(watcher._metrics_to_dict(metrics), indent=2))
            return 0
        else:
            print("Failed to analyze video", file=sys.stderr)
            return 1
    
    if args.date:
        result = watcher.check_date(args.date)
        print(json.dumps(result, indent=2))
        return 0
    
    if args.weekly_report:
        report_path = watcher.generate_weekly_report()
        if report_path:
            print(f"Weekly report generated: {report_path}")
            # Also print to stdout
            with open(report_path) as f:
                print("\n" + "="*60)
                print(f.read())
            return 0
        else:
            print("No data available for weekly report", file=sys.stderr)
            return 1
    
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
