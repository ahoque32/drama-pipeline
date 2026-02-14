#!/usr/bin/env python3
"""
CostTracker - Per-video API cost logging and reporting
Tracks Claude calls, token usage, daily/weekly reports, alerts
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import urllib.request

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, log_operation


class CostTracker:
    """Track and report API costs for the Drama Pipeline."""
    
    # Claude API pricing (as of 2025-02)
    CLAUDE_PRICING = {
        "claude-sonnet-4-20250514": {
            "input_per_1m": 3.00,   # $3 per 1M input tokens
            "output_per_1m": 15.00  # $15 per 1M output tokens
        }
    }
    
    # Daily budget alert threshold
    DAILY_BUDGET_ALERT = 2.00  # $2/day
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.costs_dir = self.pipeline_dir / "costs"
        self.costs_dir.mkdir(exist_ok=True)
        self.state_dir = self.pipeline_dir / "state"
        self.state_dir.mkdir(exist_ok=True)
        
        # Telegram config for alerts
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    
    def get_cost_file(self, date_str: Optional[str] = None) -> Path:
        """Get cost file for a specific date."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        return self.costs_dir / f"{date_str}.json"
    
    def load_costs(self, date_str: Optional[str] = None) -> Dict:
        """Load cost data for a date."""
        cost_file = self.get_cost_file(date_str)
        
        if cost_file.exists():
            with open(cost_file) as f:
                return json.load(f)
        
        return {
            "date": date_str or datetime.now().strftime("%Y-%m-%d"),
            "entries": [],
            "totals": {
                "claude_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_cost": 0.0
            }
        }
    
    def save_costs(self, data: Dict, date_str: Optional[str] = None):
        """Save cost data for a date."""
        cost_file = self.get_cost_file(date_str)
        with open(cost_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def log_claude_call(self, model: str, input_tokens: int, output_tokens: int, 
                        operation: str = "script_generation", video_id: Optional[str] = None,
                        date_str: Optional[str] = None) -> Dict:
        """Log a Claude API call with cost calculation."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Get pricing
        pricing = self.CLAUDE_PRICING.get(model, self.CLAUDE_PRICING["claude-sonnet-4-20250514"])
        
        # Calculate cost
        input_cost = (input_tokens / 1_000_000) * pricing["input_per_1m"]
        output_cost = (output_tokens / 1_000_000) * pricing["output_per_1m"]
        total_cost = input_cost + output_cost
        
        # Build entry
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "model": model,
            "operation": operation,
            "video_id": video_id,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": {
                "input": round(input_cost, 6),
                "output": round(output_cost, 6),
                "total": round(total_cost, 6)
            }
        }
        
        # Load and update
        data = self.load_costs(date_str)
        data["entries"].append(entry)
        
        # Update totals
        data["totals"]["claude_calls"] += 1
        data["totals"]["input_tokens"] += input_tokens
        data["totals"]["output_tokens"] += output_tokens
        data["totals"]["total_cost"] += total_cost
        data["totals"]["total_cost"] = round(data["totals"]["total_cost"], 6)
        
        self.save_costs(data, date_str)
        
        # Check budget alert
        self._check_budget_alert(data["totals"]["total_cost"], date_str)
        
        return entry
    
    def log_script_generation(self, seed_id: str, variations: int, 
                              rewrites: int = 0, date_str: Optional[str] = None) -> Dict:
        """Log estimated cost for script generation (when actual tokens unknown)."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Estimate tokens based on operation type
        # Script generation: ~2000 input, ~400 output per call
        # Rewrites: ~2500 input, ~400 output
        
        estimated_input = (variations * 2000) + (rewrites * 2500)
        estimated_output = (variations + rewrites) * 400
        
        entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "model": "claude-sonnet-4-20250514",
            "operation": "script_generation",
            "video_id": seed_id,
            "variations": variations,
            "rewrites": rewrites,
            "estimated_input_tokens": estimated_input,
            "estimated_output_tokens": estimated_output,
            "note": "Estimated - actual tokens not tracked"
        }
        
        # Calculate estimated cost
        pricing = self.CLAUDE_PRICING["claude-sonnet-4-20250514"]
        input_cost = (estimated_input / 1_000_000) * pricing["input_per_1m"]
        output_cost = (estimated_output / 1_000_000) * pricing["output_per_1m"]
        entry["estimated_cost"] = round(input_cost + output_cost, 6)
        
        # Load and update
        data = self.load_costs(date_str)
        data["entries"].append(entry)
        
        # Update totals (using estimates)
        data["totals"]["claude_calls"] += variations + rewrites
        data["totals"]["total_cost"] += entry["estimated_cost"]
        data["totals"]["total_cost"] = round(data["totals"]["total_cost"], 6)
        
        self.save_costs(data, date_str)
        
        # Check budget alert
        self._check_budget_alert(data["totals"]["total_cost"], date_str)
        
        return entry
    
    def _check_budget_alert(self, daily_total: float, date_str: str):
        """Send alert if daily budget exceeded."""
        if daily_total < self.DAILY_BUDGET_ALERT:
            return
        
        # Only alert once per day (check if already alerted)
        alert_file = self.state_dir / f"{date_str}-alert-sent"
        if alert_file.exists():
            return
        
        message = f"""🚨 <b>COST ALERT - Daily Budget Exceeded</b>

Date: {date_str}
Daily Total: ${daily_total:.2f}
Budget Limit: ${self.DAILY_BUDGET_ALERT:.2f}
Overage: ${daily_total - self.DAILY_BUDGET_ALERT:.2f}

Consider reducing variations or seeds for today."""
        
        self._send_telegram_alert(message)
        alert_file.touch()  # Mark alert as sent
    
    def _send_telegram_alert(self, message: str):
        """Send alert via Telegram."""
        if not self.telegram_token or not self.telegram_chat_id:
            print(f"[CostTracker] Would send alert: {message[:100]}...")
            return
        
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
                    print(f"[CostTracker] Alert sent to Telegram")
                else:
                    print(f"[CostTracker] Telegram error: {result}")
        except Exception as e:
            print(f"[CostTracker] Telegram send error: {e}")
    
    def get_daily_report(self, date_str: Optional[str] = None) -> Dict:
        """Generate daily cost report."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        data = self.load_costs(date_str)
        
        # Calculate breakdown by operation
        operations = {}
        for entry in data["entries"]:
            op = entry.get("operation", "unknown")
            if op not in operations:
                operations[op] = {"calls": 0, "cost": 0.0}
            
            operations[op]["calls"] += 1
            cost = entry.get("cost", {}).get("total", entry.get("estimated_cost", 0))
            operations[op]["cost"] += cost
        
        # Round costs
        for op in operations:
            operations[op]["cost"] = round(operations[op]["cost"], 4)
        
        return {
            "date": date_str,
            "total_cost": data["totals"]["total_cost"],
            "claude_calls": data["totals"]["claude_calls"],
            "operations": operations,
            "budget_status": "under" if data["totals"]["total_cost"] < self.DAILY_BUDGET_ALERT else "over"
        }
    
    def get_weekly_report(self, end_date: Optional[str] = None) -> Dict:
        """Generate weekly cost report."""
        if end_date is None:
            end = datetime.now()
        else:
            end = datetime.strptime(end_date, "%Y-%m-%d")
        
        start = end - timedelta(days=6)
        
        weekly_total = 0.0
        daily_breakdown = []
        
        for i in range(7):
            day = start + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            data = self.load_costs(day_str)
            
            daily_cost = data["totals"]["total_cost"]
            weekly_total += daily_cost
            
            daily_breakdown.append({
                "date": day_str,
                "cost": daily_cost,
                "calls": data["totals"]["claude_calls"]
            })
        
        return {
            "period": f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}",
            "total_cost": round(weekly_total, 4),
            "daily_average": round(weekly_total / 7, 4),
            "daily_breakdown": daily_breakdown
        }
    
    def format_daily_report(self, date_str: Optional[str] = None) -> str:
        """Format daily report for Telegram."""
        report = self.get_daily_report(date_str)
        
        lines = [
            f"💰 <b>DAILY COST REPORT — {report['date']}</b>",
            "",
            f"Total Cost: ${report['total_cost']:.3f}",
            f"Claude Calls: {report['claude_calls']}",
            f"Budget Status: {'✅ Under' if report['budget_status'] == 'under' else '⚠️ Over'} budget"
        ]
        
        if report['operations']:
            lines.extend(["", "By Operation:"])
            for op, stats in report['operations'].items():
                lines.append(f"  • {op}: {stats['calls']} calls, ${stats['cost']:.3f}")
        
        return '\n'.join(lines)
    
    def format_weekly_report(self, end_date: Optional[str] = None) -> str:
        """Format weekly report for Telegram."""
        report = self.get_weekly_report(end_date)
        
        lines = [
            f"📊 <b>WEEKLY COST REPORT</b>",
            f"{report['period']}",
            "",
            f"Total Cost: ${report['total_cost']:.3f}",
            f"Daily Average: ${report['daily_average']:.3f}",
            "",
            "Daily Breakdown:"
        ]
        
        for day in report['daily_breakdown']:
            status = "✅" if day['cost'] < self.DAILY_BUDGET_ALERT else "⚠️"
            lines.append(f"  {status} {day['date']}: ${day['cost']:.3f} ({day['calls']} calls)")
        
        return '\n'.join(lines)
    
    def send_daily_report(self, date_str: Optional[str] = None):
        """Send daily cost report via Telegram."""
        message = self.format_daily_report(date_str)
        self._send_telegram_alert(message)
    
    def send_weekly_report(self, end_date: Optional[str] = None):
        """Send weekly cost report via Telegram."""
        message = self.format_weekly_report(end_date)
        self._send_telegram_alert(message)


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='CostTracker - API cost tracking and reporting')
    parser.add_argument('--log-claude', action='store_true', help='Log a Claude API call')
    parser.add_argument('--model', default='claude-sonnet-4-20250514', help='Claude model used')
    parser.add_argument('--input-tokens', type=int, help='Input tokens used')
    parser.add_argument('--output-tokens', type=int, help='Output tokens used')
    parser.add_argument('--operation', default='script_generation', help='Operation type')
    parser.add_argument('--video-id', help='Associated video ID')
    parser.add_argument('--daily-report', action='store_true', help='Generate daily report')
    parser.add_argument('--weekly-report', action='store_true', help='Generate weekly report')
    parser.add_argument('--date', help='Date for report (YYYY-MM-DD)')
    parser.add_argument('--send-telegram', action='store_true', help='Send report via Telegram')
    
    args = parser.parse_args()
    
    tracker = CostTracker()
    
    if args.log_claude:
        if args.input_tokens is None or args.output_tokens is None:
            print("Error: --input-tokens and --output-tokens required for --log-claude")
            return 1
        
        entry = tracker.log_claude_call(
            model=args.model,
            input_tokens=args.input_tokens,
            output_tokens=args.output_tokens,
            operation=args.operation,
            video_id=args.video_id,
            date_str=args.date
        )
        print(f"Logged: {entry['operation']} - ${entry['cost']['total']:.6f}")
        return 0
    
    if args.daily_report:
        report = tracker.get_daily_report(args.date)
        print(json.dumps(report, indent=2))
        
        if args.send_telegram:
            tracker.send_daily_report(args.date)
        return 0
    
    if args.weekly_report:
        report = tracker.get_weekly_report(args.date)
        print(json.dumps(report, indent=2))
        
        if args.send_telegram:
            tracker.send_weekly_report(args.date)
        return 0
    
    # Default: show today's costs
    report = tracker.get_daily_report(args.date)
    print(tracker.format_daily_report(args.date))
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
