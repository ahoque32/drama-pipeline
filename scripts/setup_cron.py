#!/usr/bin/env python3
"""
CronScheduler - Automated pipeline scheduling
Manages cron jobs for Drama Pipeline with proper logging and error handling

Usage:
    python scripts/setup_cron.py --install      # Install all cron jobs
    python scripts/setup_cron.py --remove       # Remove all jobs
    python scripts/setup_cron.py --show         # Show current schedule
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger('setup_cron')


class CronScheduler:
    """Manage cron jobs for Drama Pipeline."""
    
    # Schedule configuration
    SCHEDULE = {
        'pipeline': {'hour': 8, 'minute': 0},      # 08:00 EST daily
        'scout': {'interval': 2, 'start': 8, 'end': 23},  # Every 2 hours 08:00-23:00
        'summary': {'hour': 22, 'minute': 0},      # 22:00 EST daily
        'retention': {'hour': 12, 'minute': 0}     # T+24h after publish (noon check)
    }
    
    TIMEZONE = "EST"
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.config = load_config()
        
        # Directories
        self.scripts_dir = self.pipeline_dir / "scripts"
        self.logs_dir = self.pipeline_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self.config_dir = self.pipeline_dir / "config"
        self.config_dir.mkdir(exist_ok=True)
        
        # Python executable
        self.python = sys.executable
        
        # Marker for our cron jobs
        self.MARKER_BEGIN = "# === DRAMA PIPELINE SCHEDULE BEGIN ==="
        self.MARKER_END = "# === DRAMA PIPELINE SCHEDULE END ==="
    
    def _get_current_crontab(self) -> str:
        """Get current user's crontab."""
        try:
            result = subprocess.run(
                ["crontab", "-l"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return result.stdout
            return ""
        except Exception as e:
            logger.error(f"Failed to read crontab: {e}")
            return ""
    
    def _set_crontab(self, content: str) -> bool:
        """Set user's crontab."""
        try:
            result = subprocess.run(
                ["crontab", "-"],
                input=content,
                capture_output=True,
                text=True
            )
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Failed to set crontab: {e}")
            return False
    
    def _remove_existing_schedule(self, crontab: str) -> str:
        """Remove existing Drama Pipeline schedule from crontab."""
        lines = crontab.split('\n')
        result = []
        in_our_block = False
        
        for line in lines:
            if self.MARKER_BEGIN in line:
                in_our_block = True
                continue
            if self.MARKER_END in line:
                in_our_block = False
                continue
            if not in_our_block:
                result.append(line)
        
        return '\n'.join(result)
    
    def _build_pipeline_job(self) -> str:
        """Build daily pipeline run job (08:00 EST)."""
        log_file = self.logs_dir / "cron-pipeline.log"
        
        job = f"0 8 * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/drama_maestro.py --daily"
        job += f" >> {log_file} 2>&1"
        
        return job
    
    def _build_scout_jobs(self) -> List[str]:
        """Build ScoutDrama scan jobs (every 2 hours, 08:00-23:00)."""
        jobs = []
        log_file = self.logs_dir / "cron-scout.log"
        
        # Run at 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00, 22:00
        hours = list(range(8, 24, 2))  # [8, 10, 12, 14, 16, 18, 20, 22]
        hours_str = ",".join(str(h) for h in hours)
        
        job = f"0 {hours_str} * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/scout_drama.py"
        job += f" >> {log_file} 2>&1"
        
        return [job]
    
    def _build_summary_job(self) -> str:
        """Build daily summary report job (22:00 EST)."""
        log_file = self.logs_dir / "cron-summary.log"
        
        job = f"0 22 * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/daily_summary.py"
        job += f" >> {log_file} 2>&1"
        
        return job
    
    def _build_retention_job(self) -> str:
        """Build retention check job (12:00 EST daily - checks T+24h videos)."""
        log_file = self.logs_dir / "cron-retention.log"
        
        # Use yesterday's date to check videos published 24h ago
        job = f"0 12 * * * "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/retention_watcher.py --date $(date -v-1d +%Y-%m-%d)"
        job += f" >> {log_file} 2>&1"
        
        return job
    
    def _build_weekly_report_job(self) -> str:
        """Build weekly report job (Sundays at 09:00)."""
        log_file = self.logs_dir / "cron-weekly.log"
        
        job = f"0 9 * * 0 "
        job += f"cd {self.pipeline_dir} && "
        job += f"{self.python} {self.scripts_dir}/retention_watcher.py --weekly-report"
        job += f" >> {log_file} 2>&1"
        
        return job
    
    def _generate_schedule_content(self) -> str:
        """Generate the full schedule block."""
        lines = [
            self.MARKER_BEGIN,
            f"# Drama Pipeline Automated Schedule",
            f"# Installed: {datetime.now().isoformat()}",
            f"# Timezone: {self.TIMEZONE}",
            "",
            "# 08:00 EST - Full pipeline run (Scout → Script → Approval)",
            self._build_pipeline_job(),
            "",
            "# Every 2 hours (08:00-22:00) - ScoutDrama scan for breaking news",
        ]
        
        for job in self._build_scout_jobs():
            lines.append(job)
        
        lines.extend([
            "",
            "# 22:00 EST - Daily summary report",
            self._build_summary_job(),
            "",
            "# 12:00 EST - Retention check (T+24h after publish)",
            self._build_retention_job(),
            "",
            "# 09:00 Sundays - Weekly performance report",
            self._build_weekly_report_job(),
            "",
            self.MARKER_END
        ])
        
        return '\n'.join(lines)
    
    def install(self) -> bool:
        """Install Drama Pipeline cron schedule."""
        logger.info("Installing Drama Pipeline cron schedule...")
        
        # Get current crontab and remove existing schedule
        current = self._get_current_crontab()
        cleaned = self._remove_existing_schedule(current)
        
        # Generate new schedule
        schedule = self._generate_schedule_content()
        
        # Combine
        new_crontab = cleaned.rstrip() + "\n\n" + schedule + "\n"
        
        # Install
        if self._set_crontab(new_crontab):
            logger.info("✅ Cron schedule installed successfully")
            logger.info("Schedule:")
            logger.info("  - Pipeline: Daily at 08:00 EST")
            logger.info("  - Scout: Every 2 hours (08:00-22:00)")
            logger.info("  - Summary: Daily at 22:00 EST")
            logger.info("  - Retention: Daily at 12:00 EST (T+24h check)")
            logger.info("  - Weekly Report: Sundays at 09:00 EST")
            
            # Save config
            self._save_crontab_config()
            
            log_operation('CronScheduler', 'install', 'success')
            return True
        else:
            logger.error("❌ Failed to install cron schedule")
            return False
    
    def remove(self) -> bool:
        """Remove Drama Pipeline cron schedule."""
        logger.info("Removing Drama Pipeline cron schedule...")
        
        current = self._get_current_crontab()
        cleaned = self._remove_existing_schedule(current)
        
        if self._set_crontab(cleaned):
            logger.info("✅ Cron schedule removed")
            log_operation('CronScheduler', 'remove', 'success')
            return True
        else:
            logger.error("❌ Failed to remove cron schedule")
            return False
    
    def show(self) -> None:
        """Show current Drama Pipeline schedule."""
        crontab = self._get_current_crontab()
        
        if self.MARKER_BEGIN in crontab:
            logger.info("Current Drama Pipeline schedule:")
            print("=" * 60)
            
            lines = crontab.split('\n')
            in_block = False
            for line in lines:
                if self.MARKER_BEGIN in line:
                    in_block = True
                if in_block:
                    print(line)
                if self.MARKER_END in line:
                    in_block = False
            
            print("=" * 60)
        else:
            logger.info("No Drama Pipeline schedule found")
            print("Run: python scripts/setup_cron.py --install")
    
    def _save_crontab_config(self) -> None:
        """Save crontab config to file."""
        config_file = self.config_dir / "crontab"
        
        content = self._generate_schedule_content()
        with open(config_file, 'w') as f:
            f.write(content)
        
        logger.info(f"Schedule config saved to {config_file}")
    
    def export_config(self, output_path: Optional[Path] = None) -> Path:
        """Export current crontab to file."""
        if output_path is None:
            output_path = self.config_dir / "crontab.backup"
        
        crontab = self._get_current_crontab()
        with open(output_path, 'w') as f:
            f.write(crontab)
        
        logger.info(f"Crontab exported to {output_path}")
        return output_path
    
    def import_config(self, input_path: Path) -> bool:
        """Import crontab from file."""
        if not input_path.exists():
            logger.error(f"File not found: {input_path}")
            return False
        
        with open(input_path) as f:
            content = f.read()
        
        if self._set_crontab(content):
            logger.info(f"Crontab imported from {input_path}")
            return True
        return False
    
    def run_job(self, job_type: str) -> bool:
        """Run a specific job manually (for testing)."""
        import subprocess
        
        jobs = {
            'pipeline': ['python', str(self.scripts_dir / 'drama_maestro.py'), '--daily'],
            'scout': ['python', str(self.scripts_dir / 'scout_drama.py')],
            'summary': ['python', str(self.scripts_dir / 'daily_summary.py')],
            'retention': ['python', str(self.scripts_dir / 'retention_watcher.py'), '--weekly-report']
        }
        
        if job_type not in jobs:
            logger.error(f"Unknown job type: {job_type}")
            return False
        
        logger.info(f"Running {job_type} job...")
        
        try:
            result = subprocess.run(
                jobs[job_type],
                cwd=self.pipeline_dir,
                capture_output=True,
                text=True,
                timeout=600
            )
            
            print(result.stdout)
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Job failed: {e}")
            return False


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='SetupCron - Drama Pipeline cron scheduling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --install              # Install all cron jobs
  %(prog)s --remove               # Remove all jobs
  %(prog)s --show                 # Show current schedule
  %(prog)s --run pipeline         # Run pipeline job now
  %(prog)s --export backup.txt    # Export crontab
        """
    )
    
    parser.add_argument('--install', action='store_true', help='Install cron schedule')
    parser.add_argument('--remove', action='store_true', help='Remove cron schedule')
    parser.add_argument('--show', action='store_true', help='Show current schedule')
    parser.add_argument('--run', choices=['pipeline', 'scout', 'summary', 'retention'],
                       help='Run a job manually for testing')
    parser.add_argument('--export', metavar='FILE', help='Export crontab to file')
    parser.add_argument('--import-file', metavar='FILE', help='Import crontab from file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    scheduler = CronScheduler()
    
    if args.install:
        success = scheduler.install()
        return 0 if success else 1
    
    if args.remove:
        success = scheduler.remove()
        return 0 if success else 1
    
    if args.show:
        scheduler.show()
        return 0
    
    if args.run:
        success = scheduler.run_job(args.run)
        return 0 if success else 1
    
    if args.export:
        scheduler.export_config(Path(args.export))
        return 0
    
    if args.import_file:
        success = scheduler.import_config(Path(args.import_file))
        return 0 if success else 1
    
    # Default: show schedule
    scheduler.show()
    return 0


if __name__ == "__main__":
    sys.exit(main())
