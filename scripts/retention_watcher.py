#!/usr/bin/env python3
"""
RetentionWatcher - YouTube Analytics integration and performance tracking
Monitors video performance, retention metrics, and generates reports
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import urllib.request
import urllib.error

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


class RetentionWatcher:
    """YouTube Analytics monitoring and performance tracking."""
    
    # YouTube Analytics API endpoints
    ANALYTICS_API_BASE = "https://youtubeanalytics.googleapis.com/v2/reports"
    DATA_API_BASE = "https://www.googleapis.com/youtube/v3"
    
    # Key metrics to track
    KEY_METRICS = [
        "views",
        "estimatedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
        "subscribersGained",
        "subscribersLost",
        "likes",
        "dislikes",
        "comments",
        "shares"
    ]
    
    # Retention-focused metrics
    RETENTION_METRICS = [
        "audienceWatchRatio",  # Relative retention
        "relativeRetentionPerformance",  # Compared to similar videos
        "viewerPercentage"  # Demographics
    ]
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # Load YouTube credentials from uploader
        self.token_dir = self.pipeline_dir / "tokens"
        self.token_file = self.token_dir / "youtube_tokens.json"
        
        self.access_token = None
        self.refresh_token = None
        self._load_tokens()
        
        # Analytics storage
        self.analytics_dir = self.pipeline_dir / "analytics"
        self.analytics_dir.mkdir(exist_ok=True)
        
        # Uploads tracking
        self.uploads_dir = self.pipeline_dir / "uploads"
    
    def _load_tokens(self):
        """Load OAuth tokens from YouTube uploader."""
        if self.token_file.exists():
            with open(self.token_file) as f:
                data = json.load(f)
                self.access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')
    
    def ensure_valid_token(self) -> bool:
        """Ensure we have a valid access token."""
        import time
        
        if not self.access_token:
            print("[RetentionWatcher] Not authenticated. Run youtube_uploader.py --auth first")
            return False
        
        # Check token expiry and refresh if needed
        # (Would need to track expiry - simplified here)
        return True
    
    def get_video_analytics(self, video_id: str, start_date: str = None, 
                           end_date: str = None) -> Optional[Dict]:
        """Get analytics data for a specific video."""
        if not self.ensure_valid_token():
            return None
        
        # Default to last 7 days
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        # Build metrics string
        metrics = ",".join(self.KEY_METRICS)
        
        params = {
            'ids': 'channel==MINE',
            'startDate': start_date,
            'endDate': end_date,
            'metrics': metrics,
            'filters': f'video=={video_id}',
            'dimensions': 'day'
        }
        
        url = f"{self.ANALYTICS_API_BASE}?{self._encode_params(params)}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            req = urllib.request.Request(url, headers=headers)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                # Parse the report
                return self._parse_analytics_report(result, video_id)
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            print(f"[RetentionWatcher] Analytics error: {e.code} - {error_body}")
            return None
        except Exception as e:
            print(f"[RetentionWatcher] Analytics fetch error: {e}")
            return None
    
    def _encode_params(self, params: Dict) -> str:
        """URL encode parameters."""
        import urllib.parse
        return &quot;&&quot;.join(f&quot;{k}={urllib.parse.quote(str(v))}&quot; for k, v in params.items())
    
    def _parse_analytics_report(self, report: Dict, video_id: str) -> Dict:
        """Parse YouTube Analytics API response."""
        column_headers = report.get('columnHeaders', [])
        rows = report.get('rows', [])
        
        if not rows:
            return {
                'video_id': video_id,
                'data': [],
                'totals': {},
                'averages': {}
            }
        
        # Build column index mapping
        col_map = {h['name']: i for i, h in enumerate(column_headers)}
        
        # Parse daily data
        daily_data = []
        for row in rows:
            day_data = {
                'date': row[col_map.get('day', 0)],
                'views': int(row[col_map.get('views', 1)] or 0),
                'watch_time_minutes': float(row[col_map.get('estimatedMinutesWatched', 2)] or 0),
                'avg_view_duration_sec': float(row[col_map.get('averageViewDuration', 3)] or 0),
                'avg_view_percentage': float(row[col_map.get('averageViewPercentage', 4)] or 0),
                'subscribers_gained': int(row[col_map.get('subscribersGained', 5)] or 0),
                'subscribers_lost': int(row[col_map.get('subscribersLost', 6)] or 0),
                'likes': int(row[col_map.get('likes', 7)] or 0),
                'dislikes': int(row[col_map.get('dislikes', 8)] or 0),
                'comments': int(row[col_map.get('comments', 9)] or 0),
                'shares': int(row[col_map.get('shares', 10)] or 0)
            }
            daily_data.append(day_data)
        
        # Calculate totals
        totals = {
            'views': sum(d['views'] for d in daily_data),
            'watch_time_minutes': sum(d['watch_time_minutes'] for d in daily_data),
            'subscribers_gained': sum(d['subscribers_gained'] for d in daily_data),
            'subscribers_lost': sum(d['subscribers_lost'] for d in daily_data),
            'subscribers_net': sum(d['subscribers_gained'] for d in daily_data) - 
                              sum(d['subscribers_lost'] for d in daily_data),
            'likes': sum(d['likes'] for d in daily_data),
            'dislikes': sum(d['dislikes'] for d in daily_data),
            'comments': sum(d['comments'] for d in daily_data),
            'shares': sum(d['shares'] for d in daily_data)
        }
        
        # Calculate averages
        avg_view_duration = sum(d['avg_view_duration_sec'] for d in daily_data) / len(daily_data) if daily_data else 0
        avg_view_percentage = sum(d['avg_view_percentage'] for d in daily_data) / len(daily_data) if daily_data else 0
        
        averages = {
            'avg_view_duration_sec': round(avg_view_duration, 1),
            'avg_view_duration_formatted': self._format_duration(avg_view_duration),
            'avg_view_percentage': round(avg_view_percentage, 1),
            'engagement_rate': self._calculate_engagement_rate(totals)
        }
        
        return {
            'video_id': video_id,
            'data': daily_data,
            'totals': totals,
            'averages': averages,
            'fetched_at': datetime.utcnow().isoformat() + 'Z'
        }
    
    def _format_duration(self, seconds: float) -> str:
        """Format seconds as MM:SS."""
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}:{secs:02d}"
    
    def _calculate_engagement_rate(self, totals: Dict) -> float:
        """Calculate engagement rate (likes + comments + shares) / views."""
        views = totals.get('views', 0)
        if views == 0:
            return 0.0
        
        engagement = totals.get('likes', 0) + totals.get('comments', 0) + totals.get('shares', 0)
        return round((engagement / views) * 100, 2)
    
    def get_video_info(self, video_id: str) -> Optional[Dict]:
        """Get basic video info from Data API."""
        if not self.ensure_valid_token():
            return None
        
        url = f"{self.DATA_API_BASE}/videos?part=snippet,statistics,contentDetails&id={video_id}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        
        try:
            req = urllib.request.Request(url, headers=headers)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                
                items = result.get('items', [])
                if items:
                    video = items[0]
                    snippet = video.get('snippet', {})
                    stats = video.get('statistics', {})
                    content = video.get('contentDetails', {})
                    
                    return {
                        'id': video_id,
                        'title': snippet.get('title'),
                        'description': snippet.get('description'),
                        'published_at': snippet.get('publishedAt'),
                        'tags': snippet.get('tags', []),
                        'category_id': snippet.get('categoryId'),
                        'duration': content.get('duration'),
                        'views': int(stats.get('viewCount', 0)),
                        'likes': int(stats.get('likeCount', 0)),
                        'comments': int(stats.get('commentCount', 0)),
                        'privacy_status': snippet.get('privacyStatus', 'unknown')
                    }
                return None
                
        except Exception as e:
            print(f"[RetentionWatcher] Video info error: {e}")
            return None
    
    def track_uploaded_videos(self, days: int = 7) -> List[Dict]:
        """Track all videos uploaded in the last N days."""
        # Load upload records
        cutoff_date = datetime.now() - timedelta(days=days)
        videos_to_track = []
        
        for record_file in self.uploads_dir.glob("*.json"):
            with open(record_file) as f:
                records = json.load(f)
            
            for record in records:
                uploaded_at = record.get('uploaded_at', '')
                if uploaded_at:
                    upload_date = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
                    if upload_date >= cutoff_date:
                        videos_to_track.append(record)
        
        return videos_to_track
    
    def generate_performance_report(self, days: int = 7) -> Dict:
        """Generate comprehensive performance report."""
        print(f"[RetentionWatcher] Generating performance report for last {days} days...")
        
        # Get tracked videos
        videos = self.track_uploaded_videos(days)
        
        if not videos:
            print("[RetentionWatcher] No videos found to track")
            return {
                'period_days': days,
                'videos_tracked': 0,
                'summary': {}
            }
        
        # Fetch analytics for each video
        video_reports = []
        
        for video in videos:
            video_id = video.get('video_id')
            if not video_id:
                continue
            
            print(f"[RetentionWatcher] Fetching analytics for {video_id}...")
            
            # Get analytics
            analytics = self.get_video_analytics(video_id)
            
            # Get current info
            info = self.get_video_info(video_id)
            
            if analytics:
                video_reports.append({
                    'video_id': video_id,
                    'title': video.get('title', 'Unknown'),
                    'url': video.get('url'),
                    'uploaded_at': video.get('uploaded_at'),
                    'analytics': analytics,
                    'current_info': info
                })
        
        # Calculate summary stats
        total_views = sum(v['analytics']['totals']['views'] for v in video_reports if v.get('analytics'))
        total_watch_time = sum(v['analytics']['totals']['watch_time_minutes'] for v in video_reports if v.get('analytics'))
        total_subscribers = sum(v['analytics']['totals']['subscribers_net'] for v in video_reports if v.get('analytics'))
        
        avg_engagement = []
        avg_retention = []
        
        for v in video_reports:
            if v.get('analytics'):
                avg_engagement.append(v['analytics']['averages']['engagement_rate'])
                avg_retention.append(v['analytics']['averages']['avg_view_percentage'])
        
        summary = {
            'total_views': total_views,
            'total_watch_time_hours': round(total_watch_time / 60, 1),
            'total_subscribers_net': total_subscribers,
            'avg_engagement_rate': round(sum(avg_engagement) / len(avg_engagement), 2) if avg_engagement else 0,
            'avg_retention_percentage': round(sum(avg_retention) / len(avg_retention), 1) if avg_retention else 0
        }
        
        report = {
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'period_days': days,
            'videos_tracked': len(video_reports),
            'summary': summary,
            'videos': video_reports
        }
        
        # Save report
        self._save_report(report, days)
        
        return report
    
    def _save_report(self, report: Dict, days: int):
        """Save performance report to file."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        report_file = self.analytics_dir / f"report-{days}d-{date_str}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"[RetentionWatcher] Report saved to: {report_file}")
    
    def format_telegram_report(self, report: Dict) -> str:
        """Format report for Telegram."""
        summary = report.get('summary', {})
        videos = report.get('videos', [])
        
        lines = [
            f"📊 <b>PERFORMANCE REPORT — Last {report['period_days']} Days</b>",
            f"Generated: {report['generated_at'][:19]}Z",
            "",
            "<b>📈 SUMMARY</b>",
            f"  Videos Tracked: {report['videos_tracked']}",
            f"  Total Views: {summary.get('total_views', 0):,}",
            f"  Watch Time: {summary.get('total_watch_time_hours', 0)} hours",
            f"  Subscribers: {'+' if summary.get('total_subscribers_net', 0) >= 0 else ''}{summary.get('total_subscribers_net', 0)}",
            f"  Avg Engagement: {summary.get('avg_engagement_rate', 0)}%",
            f"  Avg Retention: {summary.get('avg_retention_percentage', 0)}%",
            ""
        ]
        
        # Top performers
        if videos:
            # Sort by views
            sorted_videos = sorted(
                [v for v in videos if v.get('analytics')],
                key=lambda x: x['analytics']['totals']['views'],
                reverse=True
            )
            
            lines.append("<b>🏆 TOP PERFORMERS</b>")
            
            for i, v in enumerate(sorted_videos[:3], 1):
                analytics = v.get('analytics', {})
                totals = analytics.get('totals', {})
                averages = analytics.get('averages', {})
                
                lines.extend([
                    f"",
                    f"{i}. {v['title'][:50]}...",
                    f"   👁 {totals.get('views', 0):,} views | ⏱ {averages.get('avg_view_duration_formatted', '0:00')}",
                    f"   👍 {totals.get('likes', 0)} | 💬 {totals.get('comments', 0)} | 📤 {totals.get('shares', 0)}"
                ])
        
        return '\n'.join(lines)
    
    def check_video_health(self, video_id: str) -> Dict:
        """Check health metrics for a specific video."""
        analytics = self.get_video_analytics(video_id, days=1)
        info = self.get_video_info(video_id)
        
        if not analytics:
            return {'status': 'error', 'message': 'Could not fetch analytics'}
        
        totals = analytics.get('totals', {})
        averages = analytics.get('averages', {})
        
        # Health checks
        health = {
            'video_id': video_id,
            'title': info.get('title', 'Unknown') if info else 'Unknown',
            'status': 'healthy',
            'alerts': [],
            'metrics': {
                'views_24h': totals.get('views', 0),
                'avg_retention': averages.get('avg_view_percentage', 0),
                'engagement_rate': averages.get('engagement_rate', 0)
            }
        }
        
        # Retention alert
        if averages.get('avg_view_percentage', 0) < 30:
            health['alerts'].append("⚠️ Low retention (< 30%)")
            health['status'] = 'warning'
        
        # Engagement alert
        if averages.get('engagement_rate', 0) < 1:
            health['alerts'].append("⚠️ Low engagement (< 1%)")
            health['status'] = 'warning'
        
        # Viral indicator
        if totals.get('views', 0) > 10000:
            health['alerts'].append("🚀 High views! Potential viral")
            health['status'] = 'viral'
        
        return health


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='RetentionWatcher - YouTube Analytics tracking')
    parser.add_argument('--video-id', help='Get analytics for specific video')
    parser.add_argument('--report', action='store_true', help='Generate performance report')
    parser.add_argument('--days', type=int, default=7, help='Days to include in report')
    parser.add_argument('--health', help='Check health for video ID')
    parser.add_argument('--telegram', action='store_true', help='Format report for Telegram')
    parser.add_argument('--track-all', action='store_true', help='Track all recent uploads')
    
    args = parser.parse_args()
    
    watcher = RetentionWatcher()
    
    if args.video_id:
        analytics = watcher.get_video_analytics(args.video_id)
        if analytics:
            print(json.dumps(analytics, indent=2))
            return 0
        else:
            print("Failed to fetch analytics")
            return 1
    
    if args.health:
        health = watcher.check_video_health(args.health)
        print(json.dumps(health, indent=2))
        return 0
    
    if args.report or args.track_all:
        report = watcher.generate_performance_report(args.days)
        
        if args.telegram:
            message = watcher.format_telegram_report(report)
            print(message)
        else:
            print(json.dumps(report, indent=2))
        
        return 0 if report.get('videos_tracked', 0) > 0 else 0
    
    print("Use --video-id, --report, --health, or --track-all")
    return 0


if __name__ == "__main__":
    sys.exit(main())
