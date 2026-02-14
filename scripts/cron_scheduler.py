#!/usr/bin/env python3
"""
CronScheduler - Daily 08:00 EST pipeline run scheduling
Manages cron jobs for the Drama Pipeline
"""

import json
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


class CronScheduler:
    """Manage cron jobs for Drama Pipeline."""
    
    # Default schedule
    DEFAULT_PIPELINE_TIME = "0 8"  # 8:00 AM
    DEFAULT_SUMMARY_TIME = "0 22"  # 10:00 PM
    TIMEZONE = "EST"
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # Scripts directory
        self.scripts_dir = self.pipeline_dir / "scripts"
        
        # Log directory
        self.logs_dir = self.pipeline_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        
        # Python executable
        self.python = sys.executable
    
    def _get_cron_jobs(self) -> List[str]:
        """Get current cron jobs for user."""
        try:
            result = subprocess.run(
                ["crontab", "-l"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                return [line for line in result.stdout.split('\n') if line.strip()]
            else:
                return []
        except Exception as e:
            print(f"[CronScheduler] Error reading crontab: {e}")
            return []
    
    def _set_cron_jobs(self, jobs: List[str]) -> bool:
        """Set cron jobs for user."""
        try:
            # Build crontab content
            cron_content = '\n'.join(jobs) + '\n'
            
            # Write to crontab
            result = subprocess.run(
                ["crontab", "-"],
                input=cron_content,
                capture_output=True,
                text=True
            )
            
            return result.returncode == 0
        except Exception as e:
            print(f"[CronScheduler] Error setting crontab: {e}")
            return False
    
    def _build_pipeline_job(self, hour: int = 8, minute: int = 0) -> str:
        """Build cron job for daily pipeline run."""
        log_file = self.logs_dir / "cron-pipeline.log"
        
        job = f"{minute} {hour} * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/drama_maestro.py "
        job += f">> {log_file} 2>&1"
        
        return job
    
    def _build_summary_job(self, hour: int = 22, minute: int = 0) -> str:
        """Build cron job for daily summary."""
        log_file = self.logs_dir / "cron-summary.log"
        
        job = f"{minute} {hour} * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/daily_summary.py "
        job += f">> {log_file} 2>&1"
        
        return job
    
    def _build_retention_job(self, hour: int = 12, minute: int = 0) -> str:
        """Build cron job for retention tracking."""
        log_file = self.logs_dir / "cron-retention.log"
        
        job = f"{minute} {hour} * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/retention_watcher.py --report --telegram "
        job += f">> {log_file} 2>&1"
        
        return job
    
    def install_schedule(self, pipeline_time: str = "8:00", 
                        summary_time: str = "22:00",
                        retention_time: str = "12:00") -> bool:
        """Install default cron schedule."""
        print("[CronScheduler] Installing cron schedule...")
        
        # Parse times
        pipeline_hour, pipeline_min = map(int, pipeline_time.split(':'))
        summary_hour, summary_min = map(int, summary_time.split(':'))
        retention_hour, retention_min = map(int, retention_time.split(':'))
        
        # Get existing jobs (filter out old drama pipeline jobs)
        existing_jobs = self._get_cron_jobs()
        filtered_jobs = [
            job for job in existing_jobs 
            if 'drama_maestro.py' not in job 
            and 'daily_summary.py' not in job
            and 'retention_watcher.py' not in job
        ]
        
        # Build new jobs
        new_jobs = [
            f"# Drama Pipeline Schedule - Installed {datetime.now().isoformat()}",
            f"# Daily pipeline run at {pipeline_time}",
            self._build_pipeline_job(pipeline_hour, pipeline_min),
            f"# Daily summary at {summary_time}",
            self._build_summary_job(summary_hour, summary_min),
            f"# Retention tracking at {retention_time}",
            self._build_retention_job(retention_hour, retention_min)
        ]
        
        # Combine jobs
        all_jobs = filtered_jobs + [''] + new_jobs
        
        # Install
        success = self._set_cron_jobs(all_jobs)
        
        if success:
            print(f"[CronScheduler] ✅ Schedule installed:")
            print(f"  - Pipeline: Daily at {pipeline_time}")
            print(f"  - Summary: Daily at {summary_time}")
            print(f"  - Retention: Daily at {retention_time}")
            
            log_operation('CronScheduler', 'install', 'success', {
                'pipeline_time': pipeline_time,
                'summary_time': summary_time,
                'retention_time': retention_time
            })
        else:
            print("[CronScheduler] ❌ Failed to install schedule")
        
        return success
    
    def remove_schedule(self) -> bool:
        """Remove all Drama Pipeline cron jobs."""
        print("[CronScheduler] Removing cron schedule...")
        
        existing_jobs = self._get_cron_jobs()
        filtered_jobs = [
            job for job in existing_jobs 
            if 'drama_maestro.py' not in job 
            and 'daily_summary.py' not in job
            and 'retention_watcher.py' not in job
            and 'Drama Pipeline Schedule' not in job
            and 'Drama Pipeline' not in job
        ]
        
        success = self._set_cron_jobs(filtered_jobs)
        
        if success:
            print("[CronScheduler] ✅ Schedule removed")
            log_operation('CronScheduler', 'remove', 'success')
        else:
            print("[CronScheduler] ❌ Failed to remove schedule")
        
        return success
    
    def show_schedule(self):
        """Show current Drama Pipeline cron jobs."""
        jobs = self._get_cron_jobs()
        
        drama_jobs = [
            job for job in jobs 
            if 'drama_maestro.py' in job 
            or 'daily_summary.py' in job
            or 'retention_watcher.py' in job
            or 'Drama Pipeline' in job
        ]
        
        if drama_jobs:
            print("[CronScheduler] Current Drama Pipeline schedule:")
            print("=" * 60)
            for job in drama_jobs:
                print(job)
            print("=" * 60)
        else:
            print("[CronScheduler] No Drama Pipeline jobs found")
            print("Run: python scripts/cron_scheduler.py --install")
    
    def run_pipeline_now(self) -> bool:
        """Run pipeline immediately (for testing)."""
        print("[CronScheduler] Running pipeline now...")
        
        try:
            result = subprocess.run(
                [self.python, str(self.scripts_dir / "drama_maestro.py")],
                cwd=self.pipeline_dir,
                capture_output=True,
                text=True,
                timeout=600
            )
            
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            
            return result.returncode == 0
        except Exception as e:
            print(f"[CronScheduler] Pipeline run failed: {e}")
            return False
    
    def run_summary_now(self) -> bool:
        """Run daily summary now (for testing)."""
        print("[CronScheduler] Running daily summary now...")
        
        try:
            result = subprocess.run(
                [self.python, str(self.scripts_dir / "daily_summary.py")],
                cwd=self.pipeline_dir,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            
            return result.returncode == 0
        except Exception as e:
            print(f"[CronScheduler] Summary run failed: {e}")
            return False
    
    def export_crontab(self, output_file: Optional[Path] = None) -> Path:
        """Export current crontab to file."""
        if output_file is None:
            output_file = self.pipeline_dir / "config" / "crontab.backup"
        
        output_file.parent.mkdir(exist_ok=True)
        
        jobs = self._get_cron_jobs()
        
        with open(output_file, 'w') as f:
            f.write('\n'.join(jobs) + '\n')
        
        print(f"[CronScheduler] Crontab exported to: {output_file}")
        return output_file
    
    def import_crontab(self, input_file: Path) -> bool:
        """Import crontab from file."""
        if not input_file.exists():
            print(f"[CronScheduler] File not found: {input_file}")
            return False
        
        with open(input_file) as f:
            content = f.read()
        
        jobs = [line for line in content.split('\n') if line.strip()]
        
        success = self._set_cron_jobs(jobs)
        
        if success:
            print(f"[CronScheduler] Crontab imported from: {input_file}")
        
        return success


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CronScheduler - Pipeline scheduling')
    parser.add_argument('--install', action='store_true', help='Install cron schedule')
    parser.add_argument('--remove', action='store_true', help='Remove cron schedule')
    parser.add_argument('--show', action='store_true', help='Show current schedule')
    parser.add_argument('--run-pipeline', action='store_true', help='Run pipeline now')
    parser.add_argument('--run-summary', action='store_true', help='Run summary now')
    parser.add_argument('--pipeline-time', default='8:00', help='Pipeline time (HH:MM)')
    parser.add_argument('--summary-time', default='22:00', help='Summary time (HH:MM)')
    parser.add_argument('--retention-time', default='12:00', help='Retention time (HH:MM)')
    parser.add_argument('--export', help='Export crontab to file')
    parser.add_argument('--import-file', help='Import crontab from file')
    
    args = parser.parse_args()
    
    scheduler = CronScheduler()
    
    if args.install:
        success = scheduler.install_schedule(
            pipeline_time=args.pipeline_time,
            summary_time=args.summary_time,
            retention_time=args.retention_time
        )
        return 0 if success else 1
    
    if args.remove:
        success = scheduler.remove_schedule()
        return 0 if success else 1
    
    if args.show:
        scheduler.show_schedule()
        return 0
    
    if args.run_pipeline:
        success = scheduler.run_pipeline_now()
        return 0 if success else 1
    
    if args.run_summary:
        success = scheduler.run_summary_now()
        return 0 if success else 1
    
    if args.export:
        scheduler.export_crontab(Path(args.export))
        return 0
    
    if args.import_file:
        success = scheduler.import_crontab(Path(args.import_file))
        return 0 if success else 1
    
    # Default: show schedule
    scheduler.show_schedule()
    return 0


if __name__ == "__main__":
    sys.exit(main())
