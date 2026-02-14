#!/usr/bin/env python3
"""
CronRunner - Wrapper for cron job execution with logging and error handling
Handles retries, notifications, and error recovery for scheduled tasks

This script is called by cron jobs, not run directly by users.
"""

import json
import logging
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Setup paths
PIPELINE_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = PIPELINE_DIR / "scripts"
LOGS_DIR = PIPELINE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
STATE_DIR = PIPELINE_DIR / "state"
STATE_DIR.mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "cron-runner.log")
    ]
)
logger = logging.getLogger('cron_runner')


class CronRunner:
    """Wrapper for cron job execution with error handling."""
    
    MAX_RETRIES = 3
    RETRY_DELAY = 60  # seconds
    
    # Job configurations
    JOBS = {
        'pipeline': {
            'script': 'drama_maestro.py',
            'args': ['--daily'],
            'timeout': 600,
            'critical': True
        },
        'scout': {
            'script': 'scout_drama.py',
            'args': [],
            'timeout': 120,
            'critical': False
        },
        'summary': {
            'script': 'daily_summary.py',
            'args': [],
            'timeout': 60,
            'critical': False
        },
        'retention': {
            'script': 'retention_watcher.py',
            'args': [],
            'timeout': 300,
            'critical': False
        }
    }
    
    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.state_file = STATE_DIR / "cron-state.json"
    
    def _load_state(self) -> Dict:
        """Load cron state."""
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    return json.load(f)
            except:
                pass
        return {'runs': [], 'failures': []}
    
    def _save_state(self, state: Dict) -> None:
        """Save cron state."""
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def _log_run(self, job_type: str, success: bool, output: str = "", 
                 error: str = "", duration: float = 0) -> None:
        """Log a job run."""
        state = self._load_state()
        
        run_record = {
            'run_id': self.run_id,
            'job_type': job_type,
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'duration_sec': duration,
            'output': output[-1000:] if output else "",  # Last 1000 chars
            'error': error[-500:] if error else ""  # Last 500 chars
        }
        
        state['runs'].append(run_record)
        
        # Keep only last 100 runs
        state['runs'] = state['runs'][-100:]
        
        if not success:
            state['failures'].append(run_record)
            state['failures'] = state['failures'][-50:]
        
        self._save_state(state)
    
    def _send_notification(self, job_type: str, success: bool, error: str = "") -> None:
        """Send notification on failure (if Telegram configured)."""
        import os
        
        token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        if not token or not chat_id:
            return
        
        status = "✅ SUCCESS" if success else "❌ FAILED"
        message = f"🎭 Cron Job: {job_type}\n"
        message += f"Status: {status}\n"
        message += f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        if error:
            message += f"Error: {error[:200]}"
        
        # Try to send via telegram_bot.py if available
        try:
            script_path = SCRIPTS_DIR / "telegram_bot.py"
            if script_path.exists():
                subprocess.run(
                    ['python', str(script_path), '--send', message],
                    capture_output=True,
                    timeout=30
                )
        except Exception as e:
            logger.warning(f"Failed to send notification: {e}")
    
    def run_job(self, job_type: str, extra_args: List[str] = None) -> Tuple[bool, str]:
        """Run a job with retries and error handling."""
        if job_type not in self.JOBS:
            logger.error(f"Unknown job type: {job_type}")
            return False, f"Unknown job type: {job_type}"
        
        config = self.JOBS[job_type]
        script_path = SCRIPTS_DIR / config['script']
        
        if not script_path.exists():
            logger.error(f"Script not found: {script_path}")
            return False, f"Script not found: {script_path}"
        
        args = ['python', str(script_path)] + config['args'] + (extra_args or [])
        timeout = config['timeout']
        
        logger.info(f"Running {job_type} job: {' '.join(args)}")
        
        start_time = datetime.now()
        last_error = ""
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                result = subprocess.run(
                    args,
                    cwd=PIPELINE_DIR,
                    capture_output=True,
                    text=True,
                    timeout=timeout
                )
                
                duration = (datetime.now() - start_time).total_seconds()
                
                if result.returncode == 0:
                    logger.info(f"✅ {job_type} job completed successfully")
                    self._log_run(job_type, True, result.stdout, "", duration)
                    return True, result.stdout
                else:
                    last_error = result.stderr or "Unknown error"
                    logger.warning(f"⚠️ {job_type} job failed (attempt {attempt}): {last_error[:200]}")
                    
                    if attempt < self.MAX_RETRIES:
                        import time
                        time.sleep(self.RETRY_DELAY)
                    
            except subprocess.TimeoutExpired:
                duration = (datetime.now() - start_time).total_seconds()
                last_error = f"Timeout after {timeout}s"
                logger.error(f"⏱️ {job_type} job timed out")
                
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                last_error = str(e)
                logger.error(f"💥 {job_type} job crashed: {e}")
                logger.error(traceback.format_exc())
        
        # All retries failed
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"❌ {job_type} job failed after {self.MAX_RETRIES} attempts")
        
        self._log_run(job_type, False, "", last_error, duration)
        
        # Send notification for critical jobs
        if config.get('critical'):
            self._send_notification(job_type, False, last_error)
        
        return False, last_error
    
    def get_status(self) -> Dict:
        """Get status of recent runs."""
        state = self._load_state()
        
        # Calculate stats
        runs = state.get('runs', [])
        recent_runs = [r for r in runs 
                      if (datetime.now() - datetime.fromisoformat(r['timestamp'])).days <= 7]
        
        success_count = sum(1 for r in recent_runs if r['success'])
        failure_count = len(recent_runs) - success_count
        
        return {
            'total_runs_7d': len(recent_runs),
            'success_count': success_count,
            'failure_count': failure_count,
            'success_rate': round(success_count / len(recent_runs) * 100, 1) if recent_runs else 0,
            'recent_failures': [r for r in recent_runs if not r['success']][-5:],
            'last_run': runs[-1] if runs else None
        }
    
    def cleanup_old_logs(self, days: int = 30) -> int:
        """Clean up log files older than N days."""
        import time
        
        cutoff = time.time() - (days * 24 * 60 * 60)
        removed = 0
        
        for log_file in LOGS_DIR.glob("*.log"):
            if log_file.stat().st_mtime < cutoff:
                log_file.unlink()
                removed += 1
        
        logger.info(f"Cleaned up {removed} old log files")
        return removed


def main():
    """CLI entry point - primarily called by cron."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='CronRunner - Job execution wrapper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script is typically called by cron, not run manually.
For manual job execution, use setup_cron.py --run

Cron usage:
  python scripts/cron_runner.py --job pipeline
  python scripts/cron_runner.py --job scout --date 2026-02-14
        """
    )
    
    parser.add_argument('--job', choices=['pipeline', 'scout', 'summary', 'retention'],
                       help='Job type to run')
    parser.add_argument('--date', help='Date argument for retention job (YYYY-MM-DD)')
    parser.add_argument('--status', action='store_true', help='Show cron status')
    parser.add_argument('--cleanup', type=int, metavar='DAYS',
                       help='Clean up logs older than N days')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    runner = CronRunner()
    
    if args.status:
        status = runner.get_status()
        print(json.dumps(status, indent=2))
        return 0
    
    if args.cleanup:
        removed = runner.cleanup_old_logs(args.cleanup)
        print(f"Removed {removed} old log files")
        return 0
    
    if args.job:
        extra_args = []
        if args.job == 'retention' and args.date:
            extra_args = ['--date', args.date]
        
        success, output = runner.run_job(args.job, extra_args)
        
        if success:
            print(output)
            return 0
        else:
            print(f"Job failed: {output}", file=sys.stderr)
            return 1
    
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
