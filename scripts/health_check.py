#!/usr/bin/env python3
"""
HealthCheck - Pipeline health monitoring endpoint
Checks API credentials, directory structure, disk space, and service status
"""

import json
import os
import sys
import shutil
import subprocess
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, get_anthropic_api_key


@dataclass
class HealthCheck:
    """Health check result."""
    name: str
    status: str  # ok, warning, error
    message: str
    details: Optional[Dict] = None
    duration_ms: Optional[float] = None


class HealthChecker:
    """Pipeline health checker."""
    
    # Disk space thresholds (GB)
    DISK_WARNING_GB = 10
    DISK_CRITICAL_GB = 5
    
    # Required directories
    REQUIRED_DIRS = [
        'seeds',
        'drafts', 
        'approved',
        'audio',
        'assets',
        'handoffs',
        'logs',
        'logs/errors',
        'state'
    ]
    
    # Required environment variables
    REQUIRED_ENV_VARS = [
        'ANTHROPIC_API_KEY',
        'X_BEARER_TOKEN',
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID'
    ]
    
    # Optional but recommended
    OPTIONAL_ENV_VARS = [
        'YOUTUBE_CLIENT_SECRETS',
        'CRAYO_API_KEY',
        'PEXELS_API_KEY',
        'PIXABAY_API_KEY'
    ]
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        self.results: List[HealthCheck] = []
        self.start_time = None
    
    def run_all_checks(self, quick: bool = False) -> Dict:
        """Run all health checks."""
        self.start_time = datetime.now(timezone.utc)
        self.results = []
        
        # Core checks (always run)
        self._check_disk_space()
        self._check_directory_structure()
        self._check_environment_variables()
        
        if not quick:
            # API checks (can be slow)
            self._check_anthropic_api()
            self._check_x_api()
            self._check_telegram_api()
            self._check_youtube_api()
            self._check_reddit_api()
        
        # Pipeline state checks
        self._check_pipeline_state()
        
        return self._compile_report()
    
    def _check_disk_space(self):
        """Check available disk space."""
        try:
            stat = shutil.disk_usage(self.pipeline_dir)
            free_gb = stat.free / (1024**3)
            total_gb = stat.total / (1024**3)
            used_percent = (stat.used / stat.total) * 100
            
            if free_gb < self.DISK_CRITICAL_GB:
                status = 'error'
                message = f"Critical: Only {free_gb:.1f}GB free"
            elif free_gb < self.DISK_WARNING_GB:
                status = 'warning'
                message = f"Low space: {free_gb:.1f}GB free"
            else:
                status = 'ok'
                message = f"Healthy: {free_gb:.1f}GB free ({used_percent:.1f}% used)"
            
            self.results.append(HealthCheck(
                name='disk_space',
                status=status,
                message=message,
                details={
                    'free_gb': round(free_gb, 2),
                    'total_gb': round(total_gb, 2),
                    'used_percent': round(used_percent, 1)
                }
            ))
        except Exception as e:
            self.results.append(HealthCheck(
                name='disk_space',
                status='error',
                message=f"Failed to check disk: {e}"
            ))
    
    def _check_directory_structure(self):
        """Verify required directories exist."""
        missing = []
        for dir_name in self.REQUIRED_DIRS:
            dir_path = self.pipeline_dir / dir_name
            if not dir_path.exists():
                missing.append(dir_name)
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    missing.append(f"{dir_name} (create failed: {e})")
        
        if missing:
            self.results.append(HealthCheck(
                name='directory_structure',
                status='warning',
                message=f"Created missing directories: {', '.join(missing)}",
                details={'missing': missing}
            ))
        else:
            self.results.append(HealthCheck(
                name='directory_structure',
                status='ok',
                message=f"All {len(self.REQUIRED_DIRS)} required directories present"
            ))
    
    def _check_environment_variables(self):
        """Check required environment variables."""
        missing = []
        present = []
        
        for var in self.REQUIRED_ENV_VARS:
            if os.environ.get(var):
                present.append(var)
            else:
                missing.append(var)
        
        optional_present = []
        for var in self.OPTIONAL_ENV_VARS:
            if os.environ.get(var):
                optional_present.append(var)
        
        if missing:
            self.results.append(HealthCheck(
                name='environment_variables',
                status='error',
                message=f"Missing required: {', '.join(missing)}",
                details={
                    'missing': missing,
                    'present': present,
                    'optional_present': optional_present
                }
            ))
        else:
            self.results.append(HealthCheck(
                name='environment_variables',
                status='ok',
                message=f"All required vars set ({len(optional_present)}/{len(self.OPTIONAL_ENV_VARS)} optional)",
                details={
                    'present': present,
                    'optional_present': optional_present
                }
            ))
    
    def _check_anthropic_api(self):
        """Test Anthropic API connectivity."""
        import time
        start = time.time()
        
        try:
            api_key = get_anthropic_api_key()
            if not api_key:
                self.results.append(HealthCheck(
                    name='anthropic_api',
                    status='error',
                    message='API key not found',
                    duration_ms=(time.time() - start) * 1000
                ))
                return
            
            # Make a minimal API call
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            }
            data = {
                "model": "claude-3-haiku-20240307",
                "max_tokens": 10,
                "messages": [{"role": "user", "content": "Hi"}]
            }
            
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers=headers,
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                duration_ms = (time.time() - start) * 1000
                
                if response.status == 200:
                    self.results.append(HealthCheck(
                        name='anthropic_api',
                        status='ok',
                        message=f'API responsive ({duration_ms:.0f}ms)',
                        duration_ms=duration_ms
                    ))
                else:
                    self.results.append(HealthCheck(
                        name='anthropic_api',
                        status='warning',
                        message=f'Unexpected status: {response.status}',
                        duration_ms=duration_ms
                    ))
                    
        except urllib.error.HTTPError as e:
            duration_ms = (time.time() - start) * 1000
            if e.code == 401:
                self.results.append(HealthCheck(
                    name='anthropic_api',
                    status='error',
                    message='Invalid API key (401)',
                    duration_ms=duration_ms
                ))
            elif e.code == 429:
                self.results.append(HealthCheck(
                    name='anthropic_api',
                    status='warning',
                    message='Rate limited (429)',
                    duration_ms=duration_ms
                ))
            else:
                self.results.append(HealthCheck(
                    name='anthropic_api',
                    status='error',
                    message=f'HTTP error: {e.code}',
                    duration_ms=duration_ms
                ))
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            self.results.append(HealthCheck(
                name='anthropic_api',
                status='error',
                message=f'Connection failed: {str(e)[:100]}',
                duration_ms=duration_ms
            ))
    
    def _check_x_api(self):
        """Test X/Twitter API connectivity."""
        import time
        start = time.time()
        
        try:
            bearer_token = os.environ.get('X_BEARER_TOKEN')
            if not bearer_token:
                self.results.append(HealthCheck(
                    name='x_api',
                    status='error',
                    message='X_BEARER_TOKEN not set',
                    duration_ms=(time.time() - start) * 1000
                ))
                return
            
            # Try to get my user info
            url = "https://api.twitter.com/2/users/me"
            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": "DramaPipeline/1.0"
            }
            
            req = urllib.request.Request(url, headers=headers)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                duration_ms = (time.time() - start) * 1000
                
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    username = data.get('data', {}).get('username', 'unknown')
                    self.results.append(HealthCheck(
                        name='x_api',
                        status='ok',
                        message=f'Connected as @{username} ({duration_ms:.0f}ms)',
                        duration_ms=duration_ms
                    ))
                else:
                    self.results.append(HealthCheck(
                        name='x_api',
                        status='warning',
                        message=f'Unexpected status: {response.status}',
                        duration_ms=duration_ms
                    ))
                    
        except urllib.error.HTTPError as e:
            duration_ms = (time.time() - start) * 1000
            if e.code == 401:
                self.results.append(HealthCheck(
                    name='x_api',
                    status='error',
                    message='Invalid bearer token (401)',
                    duration_ms=duration_ms
                ))
            elif e.code == 403:
                self.results.append(HealthCheck(
                    name='x_api',
                    status='error',
                    message='Insufficient permissions (403)',
                    duration_ms=duration_ms
                ))
            elif e.code == 429:
                self.results.append(HealthCheck(
                    name='x_api',
                    status='warning',
                    message='Rate limited (429)',
                    duration_ms=duration_ms
                ))
            else:
                self.results.append(HealthCheck(
                    name='x_api',
                    status='error',
                    message=f'HTTP error: {e.code}',
                    duration_ms=duration_ms
                ))
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            self.results.append(HealthCheck(
                name='x_api',
                status='error',
                message=f'Connection failed: {str(e)[:100]}',
                duration_ms=duration_ms
            ))
    
    def _check_telegram_api(self):
        """Test Telegram Bot API connectivity."""
        import time
        start = time.time()
        
        try:
            token = os.environ.get('TELEGRAM_BOT_TOKEN')
            if not token:
                self.results.append(HealthCheck(
                    name='telegram_api',
                    status='error',
                    message='TELEGRAM_BOT_TOKEN not set',
                    duration_ms=(time.time() - start) * 1000
                ))
                return
            
            url = f"https://api.telegram.org/bot{token}/getMe"
            
            with urllib.request.urlopen(url, timeout=30) as response:
                duration_ms = (time.time() - start) * 1000
                
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    if data.get('ok'):
                        bot_name = data.get('result', {}).get('username', 'unknown')
                        self.results.append(HealthCheck(
                            name='telegram_api',
                            status='ok',
                            message=f'Bot @{bot_name} connected ({duration_ms:.0f}ms)',
                            duration_ms=duration_ms
                        ))
                    else:
                        self.results.append(HealthCheck(
                            name='telegram_api',
                            status='error',
                            message=f'API error: {data.get("description")}',
                            duration_ms=duration_ms
                        ))
                else:
                    self.results.append(HealthCheck(
                        name='telegram_api',
                        status='warning',
                        message=f'Unexpected status: {response.status}',
                        duration_ms=duration_ms
                    ))
                    
        except urllib.error.HTTPError as e:
            duration_ms = (time.time() - start) * 1000
            if e.code == 401:
                self.results.append(HealthCheck(
                    name='telegram_api',
                    status='error',
                    message='Invalid bot token (401)',
                    duration_ms=duration_ms
                ))
            else:
                self.results.append(HealthCheck(
                    name='telegram_api',
                    status='error',
                    message=f'HTTP error: {e.code}',
                    duration_ms=duration_ms
                ))
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            self.results.append(HealthCheck(
                name='telegram_api',
                status='error',
                message=f'Connection failed: {str(e)[:100]}',
                duration_ms=duration_ms
            ))
    
    def _check_youtube_api(self):
        """Check YouTube API credentials."""
        import time
        start = time.time()
        
        try:
            # Check for client secrets file or env var
            client_secrets = os.environ.get('YOUTUBE_CLIENT_SECRETS')
            secrets_path = self.pipeline_dir / 'client_secrets.json'
            
            if not client_secrets and not secrets_path.exists():
                self.results.append(HealthCheck(
                    name='youtube_api',
                    status='warning',
                    message='YouTube credentials not configured (optional)',
                    duration_ms=(time.time() - start) * 1000
                ))
                return
            
            # Try to validate JSON if file exists
            if secrets_path.exists():
                with open(secrets_path) as f:
                    secrets = json.load(f)
                    if 'installed' in secrets or 'web' in secrets:
                        self.results.append(HealthCheck(
                            name='youtube_api',
                            status='ok',
                            message='Client secrets file valid',
                            duration_ms=(time.time() - start) * 1000
                        ))
                    else:
                        self.results.append(HealthCheck(
                            name='youtube_api',
                            status='warning',
                            message='Client secrets file may be invalid',
                            duration_ms=(time.time() - start) * 1000
                        ))
            else:
                self.results.append(HealthCheck(
                    name='youtube_api',
                    status='ok',
                    message='Credentials configured via env var',
                    duration_ms=(time.time() - start) * 1000
                ))
                
        except json.JSONDecodeError:
            duration_ms = (time.time() - start) * 1000
            self.results.append(HealthCheck(
                name='youtube_api',
                status='error',
                message='Invalid client secrets JSON',
                duration_ms=duration_ms
            ))
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            self.results.append(HealthCheck(
                name='youtube_api',
                status='error',
                message=f'Check failed: {str(e)[:100]}',
                duration_ms=duration_ms
            ))
    
    def _check_reddit_api(self):
        """Check Reddit API accessibility (no auth required for read)."""
        import time
        start = time.time()
        
        try:
            url = "https://www.reddit.com/r/all/hot.json?limit=1"
            headers = {"User-Agent": "DramaPipeline/1.0"}
            
            req = urllib.request.Request(url, headers=headers)
            
            with urllib.request.urlopen(req, timeout=30) as response:
                duration_ms = (time.time() - start) * 1000
                
                if response.status == 200:
                    self.results.append(HealthCheck(
                        name='reddit_api',
                        status='ok',
                        message=f'Reddit accessible ({duration_ms:.0f}ms)',
                        duration_ms=duration_ms
                    ))
                else:
                    self.results.append(HealthCheck(
                        name='reddit_api',
                        status='warning',
                        message=f'Unexpected status: {response.status}',
                        duration_ms=duration_ms
                    ))
                    
        except urllib.error.HTTPError as e:
            duration_ms = (time.time() - start) * 1000
            if e.code == 429:
                self.results.append(HealthCheck(
                    name='reddit_api',
                    status='warning',
                    message='Rate limited (429)',
                    duration_ms=duration_ms
                ))
            else:
                self.results.append(HealthCheck(
                    name='reddit_api',
                    status='error',
                    message=f'HTTP error: {e.code}',
                    duration_ms=duration_ms
                ))
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            self.results.append(HealthCheck(
                name='reddit_api',
                status='error',
                message=f'Connection failed: {str(e)[:100]}',
                duration_ms=duration_ms
            ))
    
    def _check_pipeline_state(self):
        """Check pipeline state files."""
        try:
            state_dir = self.pipeline_dir / 'state'
            
            # Check for error recovery state
            circuit_file = state_dir / 'circuit_breakers.json'
            dlq_file = state_dir / 'dead_letter_queue.json'
            
            open_circuits = 0
            pending_dlq = 0
            
            if circuit_file.exists():
                try:
                    with open(circuit_file) as f:
                        circuits = json.load(f)
                        for service, state in circuits.items():
                            if state.get('state') == 'open':
                                open_circuits += 1
                except:
                    pass
            
            if dlq_file.exists():
                try:
                    with open(dlq_file) as f:
                        dlq = json.load(f)
                        pending_dlq = len([j for j in dlq if j.get('status') == 'pending'])
                except:
                    pass
            
            if open_circuits > 0 or pending_dlq > 0:
                status = 'warning' if open_circuits == 0 else 'error'
                self.results.append(HealthCheck(
                    name='pipeline_state',
                    status=status,
                    message=f'{open_circuits} open circuits, {pending_dlq} pending DLQ items',
                    details={'open_circuits': open_circuits, 'pending_dlq': pending_dlq}
                ))
            else:
                self.results.append(HealthCheck(
                    name='pipeline_state',
                    status='ok',
                    message='No open circuits or pending DLQ items'
                ))
                
        except Exception as e:
            self.results.append(HealthCheck(
                name='pipeline_state',
                status='warning',
                message=f'State check failed: {e}'
            ))
    
    def _compile_report(self) -> Dict:
        """Compile health check results into report."""
        # Count by status
        ok_count = sum(1 for r in self.results if r.status == 'ok')
        warning_count = sum(1 for r in self.results if r.status == 'warning')
        error_count = sum(1 for r in self.results if r.status == 'error')
        
        # Determine overall status
        if error_count > 0:
            overall_status = 'error'
        elif warning_count > 0:
            overall_status = 'warning'
        else:
            overall_status = 'healthy'
        
        # Calculate duration
        duration_ms = None
        if self.start_time:
            duration_ms = (datetime.now(timezone.utc) - self.start_time).total_seconds() * 1000
        
        return {
            'status': overall_status,
            'timestamp': datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            'duration_ms': round(duration_ms, 2) if duration_ms else None,
            'summary': {
                'total': len(self.results),
                'ok': ok_count,
                'warning': warning_count,
                'error': error_count
            },
            'checks': [asdict(r) for r in self.results]
        }
    
    def format_report(self, report: Dict) -> str:
        """Format report for human reading."""
        status_emoji = {
            'healthy': '✅',
            'warning': '⚠️',
            'error': '🚨'
        }
        
        lines = [
            f"{status_emoji.get(report['status'], '❓')} Pipeline Health: {report['status'].upper()}",
            f"Checked at: {report['timestamp'][:19]}Z",
            f"Duration: {report['duration_ms']:.0f}ms" if report.get('duration_ms') else "",
            "",
            f"Summary: {report['summary']['ok']} OK, {report['summary']['warning']} Warning, {report['summary']['error']} Error",
            "",
            "Checks:"
        ]
        
        for check in report['checks']:
            emoji = {'ok': '✅', 'warning': '⚠️', 'error': '❌'}.get(check['status'], '❓')
            lines.append(f"  {emoji} {check['name']}: {check['message']}")
        
        return '\n'.join(filter(None, lines))


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='HealthCheck - Pipeline health monitoring'
    )
    parser.add_argument('--quick', action='store_true', 
                       help='Skip API connectivity checks (faster)')
    parser.add_argument('--json', action='store_true',
                       help='Output as JSON')
    parser.add_argument('--check', choices=[
        'disk', 'dirs', 'env', 'anthropic', 'x', 'telegram', 'youtube', 'reddit', 'state'
    ], help='Run single check')
    
    args = parser.parse_args()
    
    checker = HealthChecker()
    
    if args.check:
        # Single check mode
        check_map = {
            'disk': checker._check_disk_space,
            'dirs': checker._check_directory_structure,
            'env': checker._check_environment_variables,
            'anthropic': checker._check_anthropic_api,
            'x': checker._check_x_api,
            'telegram': checker._check_telegram_api,
            'youtube': checker._check_youtube_api,
            'reddit': checker._check_reddit_api,
            'state': checker._check_pipeline_state
        }
        
        check_map[args.check]()
        result = checker.results[0]
        
        if args.json:
            print(json.dumps(asdict(result), indent=2))
        else:
            emoji = {'ok': '✅', 'warning': '⚠️', 'error': '❌'}.get(result.status, '❓')
            print(f"{emoji} {result.name}: {result.message}")
            if result.details:
                print(f"   Details: {json.dumps(result.details, indent=2)}")
        
        return 0 if result.status == 'ok' else 1
    
    # Full check
    report = checker.run_all_checks(quick=args.quick)
    
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(checker.format_report(report))
    
    # Exit code based on status
    return 0 if report['status'] == 'healthy' else 1


if __name__ == "__main__":
    sys.exit(main())
