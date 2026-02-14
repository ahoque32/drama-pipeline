#!/usr/bin/env python3
"""
DailySummary - 22:00 EST automated daily report
Seeds scanned, scripts generated, approved, published, cost tracking, performance metrics
"""

import json
import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import urllib.request

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation
from cost_tracker import CostTracker


class DailySummary:
    """Generate and send daily summary reports."""
    
    def __init__(self):
        self.config = load_config()
        self.pipeline_dir = get_pipeline_dir()
        self.seeds_dir = self.pipeline_dir / "seeds"
        self.drafts_dir = self.pipeline_dir / "drafts"
        self.approved_dir = self.pipeline_dir / "approved"
        self.handoffs_dir = self.pipeline_dir / "handoffs"
        self.logs_dir = self.pipeline_dir / "logs"
        
        # Telegram config
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        # Cost tracker
        self.cost_tracker = CostTracker()
    
    def load_seeds_data(self, date_str: str) -> Dict:
        """Load seeds data for date."""
        seeds_file = self.seeds_dir / f"{date_str}.json"
        
        if not seeds_file.exists():
            return {"count": 0, "sources": {}, "top_priority": 0}
        
        with open(seeds_file) as f:
            data = json.load(f)
        
        seeds = data.get('seeds', [])
        
        # Count by source
        sources = {}
        for seed in seeds:
            source = seed.get('source', 'unknown')
            sources[source] = sources.get(source, 0) + 1
        
        # Get top priority
        top_priority = max([s.get('priority_score', 0) for s in seeds], default=0)
        
        return {
            "count": len(seeds),
            "sources": sources,
            "top_priority": top_priority,
            "scan_duration": data.get('scan_duration_sec', 0)
        }
    
    def load_scripts_data(self, date_str: str) -> Dict:
        """Load scripts data for date."""
        drafts_file = self.drafts_dir / f"{date_str}.json"
        
        if not drafts_file.exists():
            return {
                "generated": 0,
                "passing": 0,
                "failing": 0,
                "avg_grade": 0,
                "avg_duration": 0,
                "rewrites": 0
            }
        
        with open(drafts_file) as f:
            data = json.load(f)
        
        stats = data.get('generation_stats', {})
        
        return {
            "generated": data.get('script_count', 0),
            "passing": stats.get('passing_count', 0),
            "failing": stats.get('failing_count', 0),
            "avg_grade": stats.get('avg_grade_level', 0),
            "avg_duration": stats.get('avg_duration_sec', 0),
            "rewrites": stats.get('total_rewrites', 0),
            "claude_calls": stats.get('total_claude_calls', 0)
        }
    
    def load_approved_data(self, date_str: str) -> Dict:
        """Load approved scripts data for date."""
        approved_date_dir = self.approved_dir / date_str
        
        if not approved_date_dir.exists():
            return {"count": 0, "breaking": 0, "manual": 0}
        
        approved_count = 0
        breaking_count = 0
        manual_count = 0
        
        for script_dir in approved_date_dir.iterdir():
            if script_dir.is_dir():
                script_file = script_dir / "script.json"
                if script_file.exists():
                    with open(script_file) as f:
                        data = json.load(f)
                    
                    approved_count += 1
                    approved_by = data.get('approved_by', 'manual')
                    
                    if 'breaking' in approved_by or data.get('breaking'):
                        breaking_count += 1
                    elif approved_by == 'manual':
                        manual_count += 1
        
        return {
            "count": approved_count,
            "breaking": breaking_count,
            "manual": manual_count
        }
    
    def load_handoffs_data(self, date_str: str) -> Dict:
        """Load handoff data for date."""
        handoffs_date_dir = self.handoffs_dir / date_str
        
        if not handoffs_date_dir.exists():
            return {"count": 0}
        
        handoff_count = len(list(handoffs_date_dir.glob("*.json")))
        
        return {
            "count": handoff_count
        }
    
    def load_pipeline_logs(self, date_str: str) -> List[Dict]:
        """Load pipeline logs for date."""
        log_file = self.logs_dir / f"{date_str}.json"
        
        if not log_file.exists():
            return []
        
        with open(log_file) as f:
            return json.load(f)
    
    def generate_summary(self, date_str: Optional[str] = None) -> Dict:
        """Generate complete daily summary."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        print(f"[DailySummary] Generating summary for {date_str}...")
        
        # Gather all data
        seeds_data = self.load_seeds_data(date_str)
        scripts_data = self.load_scripts_data(date_str)
        approved_data = self.load_approved_data(date_str)
        handoffs_data = self.load_handoffs_data(date_str)
        cost_data = self.cost_tracker.get_daily_report(date_str)
        logs = self.load_pipeline_logs(date_str)
        
        # Calculate pipeline success rate
        pipeline_runs = [l for l in logs if l.get('action') == 'pipeline']
        successful_runs = [l for l in pipeline_runs if l.get('status') == 'success']
        
        summary = {
            "date": date_str,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "seeds": seeds_data,
            "scripts": scripts_data,
            "approved": approved_data,
            "handoffs": handoffs_data,
            "cost": cost_data,
            "pipeline": {
                "total_runs": len(pipeline_runs),
                "successful_runs": len(successful_runs),
                "success_rate": round(len(successful_runs) / len(pipeline_runs) * 100, 1) if pipeline_runs else 0
            }
        }
        
        return summary
    
    def format_telegram_message(self, summary: Dict) -> str:
        """Format summary for Telegram."""
        date = summary['date']
        seeds = summary['seeds']
        scripts = summary['scripts']
        approved = summary['approved']
        handoffs = summary['handoffs']
        cost = summary['cost']
        
        lines = [
            f"📊 <b>DAILY SUMMARY — {date}</b>",
            f"Generated: {summary['generated_at'][:19]}Z",
            "",
            "<b>🌱 SEEDS</b>",
            f"  Scanned: {seeds['count']}",
        ]
        
        if seeds['sources']:
            for source, count in seeds['sources'].items():
                lines.append(f"    • {source}: {count}")
        
        lines.extend([
            f"  Top Priority: {seeds['top_priority']}/10",
            "",
            "<b>📝 SCRIPTS</b>",
            f"  Generated: {scripts['generated']}",
            f"  Passing Quality: {scripts['passing']} ✅",
            f"  Failing: {scripts['failing']} ❌",
            f"  Rewrites: {scripts['rewrites']}",
            f"  Claude Calls: {scripts.get('claude_calls', 'N/A')}",
        ])
        
        if scripts['avg_grade']:
            lines.append(f"  Avg Grade Level: {scripts['avg_grade']}")
        
        lines.extend([
            "",
            "<b>✅ APPROVED</b>",
            f"  Total: {approved['count']}",
        ])
        
        if approved['breaking'] > 0:
            lines.append(f"    ⚡ Breaking: {approved['breaking']}")
        if approved['manual'] > 0:
            lines.append(f"    Manual: {approved['manual']}")
        
        lines.extend([
            "",
            "<b>📦 HANDOFFS</b>",
            f"  Complete Packages: {handoffs['count']}",
            "",
            "<b>💰 COSTS</b>",
            f"  Total: ${cost['total_cost']:.3f}",
            f"  Budget: {'✅ Under' if cost['budget_status'] == 'under' else '⚠️ Over'} limit"
        ])
        
        if cost['operations']:
            lines.append("")
            for op, stats in cost['operations'].items():
                lines.append(f"    • {op}: ${stats['cost']:.3f}")
        
        lines.extend([
            "",
            "<b>⚙️ PIPELINE</b>",
            f"  Runs: {summary['pipeline']['total_runs']}",
            f"  Success Rate: {summary['pipeline']['success_rate']:.0f}%"
        ])
        
        lines.extend([
            "",
            "─" * 30,
            "Ready for tomorrow's run at 08:00 EST 🎬"
        ])
        
        return '\n'.join(lines)
    
    def send_telegram_summary(self, summary: Dict) -> bool:
        """Send summary via Telegram."""
        if not self.telegram_token or not self.telegram_chat_id:
            print("[DailySummary] Warning: Telegram not configured")
            # Save to file instead
            summary_file = self.pipeline_dir / "logs" / f"{summary['date']}-summary.txt"
            with open(summary_file, 'w') as f:
                f.write(self.format_telegram_message(summary))
            print(f"[DailySummary] Summary saved to: {summary_file}")
            return False
        
        message = self.format_telegram_message(summary)
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        data = {
            "chat_id": self.telegram_chat_id,
            "text": message[:4000],
            "parse_mode": "HTML"
        }
        
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers={"Content-Type": "application/json"},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                if result.get('ok'):
                    print(f"[DailySummary] Summary sent to Telegram")
                    return True
                else:
                    print(f"[DailySummary] Telegram error: {result}")
                    return False
        except Exception as e:
            print(f"[DailySummary] Telegram send error: {e}")
            return False
    
    def save_summary(self, summary: Dict):
        """Save summary to JSON file."""
        summary_file = self.pipeline_dir / "logs" / f"{summary['date']}-summary.json"
        
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"[DailySummary] Summary saved to: {summary_file}")
    
    def run(self, date_str: Optional[str] = None, send_telegram: bool = True) -> Dict:
        """Generate and send daily summary."""
        print(f"\n{'='*60}")
        print(f"📊 DAILY SUMMARY - {datetime.utcnow().isoformat()}Z")
        print(f"{'='*60}\n")
        
        # Generate summary
        summary = self.generate_summary(date_str)
        
        # Save to file
        self.save_summary(summary)
        
        # Send via Telegram
        if send_telegram:
            self.send_telegram_summary(summary)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"📊 SUMMARY COMPLETE")
        print(f"{'='*60}")
        print(f"Seeds: {summary['seeds']['count']}")
        print(f"Scripts: {summary['scripts']['passing']} passing / {summary['scripts']['failing']} failing")
        print(f"Approved: {summary['approved']['count']} (⚡ {summary['approved']['breaking']} breaking)")
        print(f"Handoffs: {summary['handoffs']['count']}")
        print(f"Cost: ${summary['cost']['total_cost']:.3f}")
        
        log_operation('DailySummary', 'generate', 'success', {
            'date': summary['date'],
            'seeds': summary['seeds']['count'],
            'scripts_passing': summary['scripts']['passing'],
            'approved': summary['approved']['count'],
            'cost': summary['cost']['total_cost']
        })
        
        return summary


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DailySummary - Daily pipeline summary report')
    parser.add_argument('--date', help='Date to summarize (YYYY-MM-DD)')
    parser.add_argument('--no-telegram', action='store_true', help='Skip sending Telegram message')
    parser.add_argument('--json-only', action='store_true', help='Only output JSON to stdout')
    
    args = parser.parse_args()
    
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    summary_bot = DailySummary()
    
    if args.json_only:
        summary = summary_bot.generate_summary(date_str)
        print(json.dumps(summary, indent=2))
        return 0
    
    summary = summary_bot.run(date_str, send_telegram=not args.no_telegram)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
