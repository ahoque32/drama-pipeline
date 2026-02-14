#!/usr/bin/env python3
"""
DramaMaestro - Master Orchestrator with Error Recovery
Chains ScoutDrama → ScriptSmith → Telegram Approval → VoiceForge → AssetHunter → Handoff
Includes circuit breakers, retry logic, and fallback chains
"""

import json
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Callable
import urllib.request
import urllib.error

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation
from error_recovery import ErrorRecovery, retry, CircuitState


class DramaMaestro:
    """Master pipeline orchestrator with error recovery."""
    
    def __init__(self):
        self.config = load_config()
        self.pipeline_dir = get_pipeline_dir()
        self.seeds_dir = self.pipeline_dir / "seeds"
        self.drafts_dir = self.pipeline_dir / "drafts"
        self.approved_dir = self.pipeline_dir / "approved"
        self.audio_dir = self.pipeline_dir / "audio"
        self.assets_dir = self.pipeline_dir / "assets"
        self.handoffs_dir = self.pipeline_dir / "handoffs"
        
        # Create directories
        for d in [self.approved_dir, self.audio_dir, self.assets_dir, self.handoffs_dir]:
            d.mkdir(exist_ok=True)
        
        # Error recovery
        self.error_recovery = ErrorRecovery()
        
        # Telegram config
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        # Stage tracking for DLQ
        self.current_stage = None
        self.current_job = None
    
    def _run_subprocess_with_recovery(self, command: List[str], module: str,
                                       max_retries: int = 2, timeout: int = 300) -> Tuple[bool, str]:
        """Run subprocess with error recovery."""
        return self.error_recovery.run_with_recovery(
            command, module, max_retries=max_retries, timeout=timeout
        )
    
    def _send_telegram_alert(self, message: str):
        """Send alert via Telegram."""
        self.error_recovery._send_alert(message)
    
    def run_scout(self, date_str: str) -> bool:
        """Run ScoutDrama module with error recovery."""
        print(f"[Maestro] Running ScoutDrama for {date_str}...")
        self.current_stage = 'scout'
        
        seeds_file = self.seeds_dir / f"{date_str}.json"
        if seeds_file.exists():
            print(f"[Maestro] Seeds already exist for {date_str}, skipping ScoutDrama")
            return True
        
        # Check circuit breaker
        allowed, reason = self.error_recovery.check_circuit_breaker('x_api')
        if not allowed:
            print(f"[Maestro] Warning: {reason}")
            self._send_telegram_alert(f"⚠️ ScoutDrama: {reason}")
            # Continue anyway - ScoutDrama can work with Reddit only
        
        allowed, reason = self.error_recovery.check_circuit_breaker('reddit_api')
        if not allowed:
            print(f"[Maestro] Warning: {reason}")
        
        command = [sys.executable, str(self.pipeline_dir / "scripts" / "scout_drama.py")]
        
        success, output = self._run_subprocess_with_recovery(
            command, 'scout_drama', max_retries=2, timeout=120
        )
        
        if success:
            print(output)
            self.error_recovery.record_success('x_api')
            self.error_recovery.record_success('reddit_api')
            return True
        else:
            print(f"[Maestro] ScoutDrama failed: {output}")
            
            # Add to dead letter queue for manual retry
            self.error_recovery.add_to_dead_letter(
                job={'date': date_str, 'stage': 'scout'},
                reason=f"ScoutDrama failed: {output[:200]}",
                stage='scout'
            )
            
            # Don't fail the entire pipeline - we might have cached seeds
            if seeds_file.exists():
                print("[Maestro] Using cached seeds from previous run")
                return True
            
            return False
    
    def run_scriptsmith(self, date_str: str) -> bool:
        """Run ScriptSmith module with error recovery."""
        print(f"[Maestro] Running ScriptSmith for {date_str}...")
        self.current_stage = 'scriptsmith'
        
        drafts_file = self.drafts_dir / f"{date_str}.json"
        if drafts_file.exists():
            print(f"[Maestro] Drafts already exist for {date_str}, skipping ScriptSmith")
            return True
        
        # Check circuit breaker for Claude API
        allowed, reason = self.error_recovery.check_circuit_breaker('claude_api')
        if not allowed:
            print(f"[Maestro] {reason}")
            self._send_telegram_alert(f"🔴 ScriptSmith blocked: {reason}")
            
            # Add to DLQ
            self.error_recovery.add_to_dead_letter(
                job={'date': date_str, 'stage': 'scriptsmith'},
                reason=reason,
                stage='scriptsmith'
            )
            return False
        
        command = [sys.executable, str(self.pipeline_dir / "scripts" / "scriptsmith.py"), date_str]
        
        success, output = self._run_subprocess_with_recovery(
            command, 'scriptsmith', max_retries=3, timeout=300
        )
        
        if success:
            print(output)
            self.error_recovery.record_success('claude_api')
            return True
        else:
            print(f"[Maestro] ScriptSmith failed: {output}")
            
            # Add to DLQ
            self.error_recovery.add_to_dead_letter(
                job={'date': date_str, 'stage': 'scriptsmith'},
                reason=f"ScriptSmith failed: {output[:200]}",
                stage='scriptsmith'
            )
            
            return False
    
    def load_passing_scripts(self, date_str: str) -> List[Dict]:
        """Load scripts that passed quality gates."""
        drafts_file = self.drafts_dir / f"{date_str}.json"
        if not drafts_file.exists():
            return []
        
        try:
            with open(drafts_file) as f:
                data = json.load(f)
            
            scripts = data.get('scripts', [])
            passing = [s for s in scripts if s.get('quality_passed')]
            
            # Sort by hook strength
            passing.sort(key=lambda x: x.get('hook_strength', 0), reverse=True)
            
            return passing
        except Exception as e:
            print(f"[Maestro] Error loading scripts: {e}")
            return []
    
    def send_telegram_scripts(self, scripts: List[Dict], date_str: str) -> bool:
        """Send scripts to Ahawk via Telegram for approval with inline buttons."""
        self.current_stage = 'approval'
        
        if not self.telegram_token or not self.telegram_chat_id:
            print("[Maestro] Warning: Telegram not configured, skipping approval flow")
            self._save_manual_review(scripts, date_str)
            return True
        
        # Check circuit breaker
        allowed, reason = self.error_recovery.check_circuit_breaker('telegram_api')
        if not allowed:
            print(f"[Maestro] {reason}")
            print("[Maestro] Falling back to manual review file")
            self._save_manual_review(scripts, date_str)
            return True  # Non-critical, continue pipeline
        
        print(f"[Maestro] Sending {len(scripts)} scripts to Telegram with inline buttons...")
        
        # Send header message
        header = f"🎬 <b>DRAMA SCRIPTS — {date_str}</b>\n"
        header += f"{'='*40}\n"
        header += f"{len(scripts)} scripts ready for review:\n\n"
        header += "Use the buttons below each script to:\n"
        header += "✅ Approve | ❌ Kill | ✏️ Edit | 🔄 Rewrite\n"
        
        if not self._send_telegram_message(header):
            print("[Maestro] Failed to send header, falling back to manual review")
            self._save_manual_review(scripts, date_str)
            return True  # Non-critical
        
        # Use TelegramBot to send scripts with inline buttons
        try:
            from telegram_bot import TelegramBot
            bot = TelegramBot()
            
            for i, script in enumerate(scripts[:5], 1):  # Top 5
                try:
                    bot.send_script_with_buttons(script, date_str, i)
                except Exception as e:
                    print(f"[Maestro] Error sending script {i}: {e}")
                    # Send plain text fallback
                    msg = self._format_script_message(script, i)
                    self._send_telegram_message(msg)
            
            self.error_recovery.record_success('telegram_api')
            return True
            
        except Exception as e:
            print(f"[Maestro] Error using TelegramBot: {e}")
            print("[Maestro] Falling back to plain text messages...")
            
            # Fallback: send plain text
            for i, script in enumerate(scripts[:5], 1):
                msg = self._format_script_message(script, i)
                if not self._send_telegram_message(msg):
                    print(f"[Maestro] Failed to send script {i}")
            
            return True  # Non-critical failure
    
    def _format_script_message(self, script: Dict, index: int) -> str:
        """Format a single script for Telegram."""
        lines = [
            f"📝 <b>SCRIPT {index}</b> [{script['variation']}]",
            f"<i>{script['headline'][:60]}...</i>",
            f"",
            f"📊 {script['word_count']}w | ~{script['duration_sec']}s | Grade {script['grade_level']}",
            f"🎭 Tone: {script['tone']} | Hook: {script['hook_strength']}/10",
            f""
        ]
        
        for j, line in enumerate(script['lines'], 1):
            lines.append(f"{j}. {line}")
        
        lines.append("")
        lines.append(f"<code>approve {index}</code> | <code>reject {index}</code> | <code>rewrite {index}: notes</code>")
        
        return '\n'.join(lines)
    
    def _send_telegram_message(self, text: str) -> bool:
        """Send message via Telegram Bot API with retry."""
        if not self.telegram_token:
            print(f"[Telegram] Would send:\n{text[:200]}...")
            return True
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        data = {
            "chat_id": self.telegram_chat_id,
            "text": text[:4000],  # Telegram limit
            "parse_mode": "HTML"
        }
        
        def _send():
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers={"Content-Type": "application/json"},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                if not result.get('ok'):
                    raise Exception(f"Telegram API error: {result.get('description')}")
                return result
        
        try:
            # Use error recovery for retry
            self.error_recovery.retry_with_backoff(
                _send,
                max_retries=2,
                backoff_base=2,
                service_name='telegram_api'
            )
            return True
        except Exception as e:
            print(f"[Telegram] Error sending message: {e}")
            self.error_recovery.record_failure('telegram_api', str(e))
            return False
    
    def _save_manual_review(self, scripts: List[Dict], date_str: str):
        """Save scripts for manual review when Telegram not configured."""
        review_file = self.drafts_dir / f"{date_str}-for-review.json"
        
        with open(review_file, 'w') as f:
            json.dump({
                "date": date_str,
                "scripts": scripts,
                "instructions": "Review and run: python scripts/drama_maestro.py --approve-script ID"
            }, f, indent=2)
        
        print(f"[Maestro] Scripts saved for manual review: {review_file}")
    
    def approve_script(self, script_id: str, date_str: str) -> bool:
        """Mark a script as approved and trigger downstream stages."""
        drafts_file = self.drafts_dir / f"{date_str}.json"
        
        try:
            with open(drafts_file) as f:
                data = json.load(f)
        except Exception as e:
            print(f"[Maestro] Error loading drafts: {e}")
            return False
        
        # Find script by ID or index
        script = None
        for s in data.get('scripts', []):
            if s.get('seed_id') == script_id or s.get('id') == script_id:
                script = s
                break
        
        if not script:
            print(f"[Maestro] Script not found: {script_id}")
            return False
        
        # Save to approved
        approved_date_dir = self.approved_dir / date_str
        approved_date_dir.mkdir(exist_ok=True)
        
        approved_record = {
            "script": script,
            "approved_at": datetime.utcnow().isoformat() + "Z",
            "approved_by": "manual"
        }
        
        approved_file = approved_date_dir / "script.json"
        with open(approved_file, 'w') as f:
            json.dump(approved_record, f, indent=2)
        
        # Save markdown version
        md_content = f"# Approved Script - {date_str}\n\n"
        md_content += f"**Headline:** {script['headline']}\n"
        md_content += f"**Variation:** {script['variation']}\n"
        md_content += f"**Tone:** {script['tone']}\n\n"
        md_content += "## Script\n\n"
        
        for i, line in enumerate(script['lines'], 1):
            md_content += f"{i}. {line}\n"
        
        md_content += f"\n## Stats\n\n"
        md_content += f"- Words: {script['word_count']}\n"
        md_content += f"- Duration: {script['duration_sec']}s\n"
        md_content += f"- Grade Level: {script['grade_level']}\n"
        md_content += f"- Hook Strength: {script['hook_strength']}/10\n"
        
        (approved_date_dir / "script.md").write_text(md_content)
        
        print(f"[Maestro] Script approved and saved to: {approved_date_dir}")
        
        # Trigger next stages with error recovery
        voiceforge_ok = self._trigger_voiceforge(approved_date_dir, date_str)
        assethunter_ok = self._trigger_assethunter(approved_date_dir, date_str)
        
        # Only trigger handoff if both stages succeeded (or if failures are non-critical)
        if voiceforge_ok or assethunter_ok:
            self._trigger_handoff(date_str)
        
        return True
    
    def _trigger_voiceforge(self, approved_dir: Path, date_str: str) -> bool:
        """Trigger VoiceForge for approved script with error recovery."""
        print(f"[Maestro] Triggering VoiceForge for {approved_dir}...")
        
        # Check circuit breaker
        allowed, reason = self.error_recovery.check_circuit_breaker('crayo_api')
        if not allowed:
            print(f"[Maestro] Warning: {reason}")
            # Try fallback or add to DLQ
            self.error_recovery.add_to_dead_letter(
                job={'date': date_str, 'stage': 'voiceforge'},
                reason=reason,
                stage='voiceforge'
            )
            return False  # Non-critical, can continue
        
        command = [sys.executable, str(self.pipeline_dir / "scripts" / "voiceforge.py"), "--date", date_str]
        
        success, output = self._run_subprocess_with_recovery(
            command, 'voiceforge', max_retries=2, timeout=180
        )
        
        if success:
            print(output)
            self.error_recovery.record_success('crayo_api')
            return True
        else:
            print(f"[Maestro] VoiceForge failed: {output}")
            self.error_recovery.record_failure('crayo_api', output[:200])
            
            # Add to DLQ for retry
            self.error_recovery.add_to_dead_letter(
                job={'date': date_str, 'stage': 'voiceforge'},
                reason=f"VoiceForge failed: {output[:200]}",
                stage='voiceforge'
            )
            
            return False  # Non-critical
    
    def _trigger_assethunter(self, approved_dir: Path, date_str: str) -> bool:
        """Trigger AssetHunter for approved script with error recovery."""
        print(f"[Maestro] Triggering AssetHunter for {approved_dir}...")
        
        command = [sys.executable, str(self.pipeline_dir / "scripts" / "assethunter.py"), "--date", date_str]
        
        success, output = self._run_subprocess_with_recovery(
            command, 'assethunter', max_retries=2, timeout=120
        )
        
        if success:
            print(output)
            return True
        else:
            print(f"[Maestro] AssetHunter failed: {output}")
            
            # Add to DLQ
            self.error_recovery.add_to_dead_letter(
                job={'date': date_str, 'stage': 'assethunter'},
                reason=f"AssetHunter failed: {output[:200]}",
                stage='assethunter'
            )
            
            return False  # Non-critical
    
    def _trigger_handoff(self, date_str: str) -> bool:
        """Trigger HandoffAssembler for complete package."""
        print(f"[Maestro] Triggering HandoffAssembler for {date_str}...")
        
        command = [sys.executable, str(self.pipeline_dir / "scripts" / "handoff_assembler.py"), "--date", date_str]
        
        success, output = self._run_subprocess_with_recovery(
            command, 'handoff_assembler', max_retries=1, timeout=60
        )
        
        if success:
            print(output)
            return True
        else:
            print(f"[Maestro] HandoffAssembler failed: {output}")
            return False  # Non-critical
    
    def check_breaking_news(self, date_str: str) -> bool:
        """Check for and process breaking news before normal pipeline."""
        print(f"[Maestro] Checking for breaking news...")
        
        command = [sys.executable, str(self.pipeline_dir / "scripts" / "breaking_news.py"), "--date", date_str]
        
        success, output = self._run_subprocess_with_recovery(
            command, 'breaking_news', max_retries=1, timeout=300
        )
        
        if success:
            print(output)
            # Check if any breaking news was processed
            if "⚡ BREAKING NEWS COMPLETE" in output and "✅ PROCESSED" in output:
                return True
            return False
        else:
            print(f"[Maestro] Breaking news check failed: {output}")
            return False  # Non-critical
    
    def run_pipeline(self, date_str: Optional[str] = None, skip_scout: bool = False, 
                     skip_breaking: bool = False) -> Dict:
        """Run full pipeline with error recovery."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        print(f"\n{'='*60}")
        print(f"DRAMA MAESTRO - Pipeline Run: {date_str}")
        print(f"{'='*60}\n")
        
        results = {
            "date": date_str,
            "started_at": datetime.utcnow().isoformat() + "Z",
            "stages": {}
        }
        
        # Stage 0: Check for breaking news (before normal pipeline)
        if not skip_breaking:
            try:
                breaking_processed = self.check_breaking_news(date_str)
                results['stages']['breaking_news'] = 'processed' if breaking_processed else 'none'
                if breaking_processed:
                    print("[Maestro] ⚡ Breaking news processed - continuing with normal pipeline")
            except Exception as e:
                print(f"[Maestro] Breaking news check error: {e}")
                results['stages']['breaking_news'] = f'error: {str(e)[:50]}'
        else:
            print("[Maestro] Skipping breaking news check ( --skip-breaking )")
            results['stages']['breaking_news'] = 'skipped'
        
        # Stage 1: ScoutDrama
        if not skip_scout:
            try:
                scout_ok = self.run_scout(date_str)
                results['stages']['scout'] = 'success' if scout_ok else 'failed'
                if not scout_ok:
                    print("[Maestro] Pipeline halted at ScoutDrama")
                    results['completed_at'] = datetime.utcnow().isoformat() + "Z"
                    return results
            except Exception as e:
                print(f"[Maestro] ScoutDrama error: {e}")
                results['stages']['scout'] = f'error: {str(e)[:50]}'
                results['completed_at'] = datetime.utcnow().isoformat() + "Z"
                return results
        else:
            print("[Maestro] Skipping ScoutDrama ( --skip-scout )")
            results['stages']['scout'] = 'skipped'
        
        # Stage 2: ScriptSmith
        try:
            smith_ok = self.run_scriptsmith(date_str)
            results['stages']['scriptsmith'] = 'success' if smith_ok else 'failed'
            if not smith_ok:
                print("[Maestro] Pipeline halted at ScriptSmith")
                results['completed_at'] = datetime.utcnow().isoformat() + "Z"
                return results
        except Exception as e:
            print(f"[Maestro] ScriptSmith error: {e}")
            results['stages']['scriptsmith'] = f'error: {str(e)[:50]}'
            results['completed_at'] = datetime.utcnow().isoformat() + "Z"
            return results
        
        # Stage 3: Load passing scripts
        try:
            passing_scripts = self.load_passing_scripts(date_str)
            print(f"[Maestro] {len(passing_scripts)} scripts passed quality gates")
            
            if not passing_scripts:
                print("[Maestro] No passing scripts, pipeline complete")
                results['stages']['approval'] = 'no_scripts'
                results['completed_at'] = datetime.utcnow().isoformat() + "Z"
                return results
        except Exception as e:
            print(f"[Maestro] Error loading scripts: {e}")
            results['stages']['approval'] = f'error: {str(e)[:50]}'
            results['completed_at'] = datetime.utcnow().isoformat() + "Z"
            return results
        
        # Stage 4: Send for approval
        try:
            approval_ok = self.send_telegram_scripts(passing_scripts, date_str)
            results['stages']['approval'] = 'sent' if approval_ok else 'failed'
        except Exception as e:
            print(f"[Maestro] Approval stage error: {e}")
            results['stages']['approval'] = f'error: {str(e)[:50]}'
        
        results['completed_at'] = datetime.utcnow().isoformat() + "Z"
        
        print(f"\n{'='*60}")
        print(f"Pipeline Complete - Waiting for Approval")
        print(f"{'='*60}")
        
        log_operation('DramaMaestro', 'pipeline', 'success', {
            'date': date_str,
            'scripts_generated': len(passing_scripts),
            'stages_completed': list(results['stages'].keys())
        })
        
        return results
    
    def send_daily_summary(self, date_str: Optional[str] = None):
        """Send end-of-day summary to Ahawk with health status."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Load data
        seeds_file = self.seeds_dir / f"{date_str}.json"
        drafts_file = self.drafts_dir / f"{date_str}.json"
        
        seeds_count = 0
        if seeds_file.exists():
            try:
                with open(seeds_file) as f:
                    seeds_count = len(json.load(f).get('seeds', []))
            except:
                pass
        
        scripts_generated = 0
        scripts_passing = 0
        if drafts_file.exists():
            try:
                with open(drafts_file) as f:
                    data = json.load(f)
                    scripts_generated = data.get('script_count', 0)
                    scripts_passing = data.get('generation_stats', {}).get('passing_count', 0)
            except:
                pass
        
        # Count approved
        approved_count = 0
        approved_dir = self.approved_dir / date_str
        if approved_dir.exists():
            approved_count = len(list(approved_dir.glob("*/script.json")))
        
        # Get health status
        health = self.error_recovery.check_pipeline_health()
        health_emoji = {
            'healthy': '✅',
            'warning': '⚠️',
            'degraded': '🔶',
            'critical': '🚨'
        }.get(health['status'], '❓')
        
        summary = f"""📊 <b>DAILY SUMMARY — {date_str}</b>

Seeds scanned: {seeds_count}
Scripts generated: {scripts_generated}
Scripts passing quality: {scripts_passing}
Scripts approved: {approved_count}

{health_emoji} Pipeline Health: {health['status'].upper()}
Errors (24h): {health['total_errors_24h']}
Open circuits: {len(health['open_circuits'])}
Pending DLQ: {health['dead_letter_queue']['pending']}

💰 Cost today: ~${scripts_generated * 0.05:.2f} (est.)

Ready for tomorrow's run at 08:00 EST."""
        
        self._send_telegram_message(summary)
        print(f"[Maestro] Daily summary sent")
    
    def check_health(self) -> Dict:
        """Check pipeline health status."""
        return self.error_recovery.check_pipeline_health()


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DramaMaestro - Pipeline Orchestrator')
    parser.add_argument('--date', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--skip-scout', action='store_true', help='Skip ScoutDrama stage')
    parser.add_argument('--skip-breaking', action='store_true', help='Skip breaking news check')
    parser.add_argument('--approve-script', help='Approve a script by ID')
    parser.add_argument('--daily-summary', action='store_true', help='Send daily summary')
    parser.add_argument('--health', action='store_true', help='Show pipeline health')
    parser.add_argument('--retry-failed', action='store_true', help='Retry dead letter queue')
    
    args = parser.parse_args()
    
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    maestro = DramaMaestro()
    
    if args.health:
        health = maestro.check_health()
        print(maestro.error_recovery.generate_health_report())
        return 0 if health['status'] in ['healthy', 'warning'] else 1
    
    if args.retry_failed:
        results = maestro.error_recovery.retry_dead_letter()
        print(f"Retry complete: {results['succeeded']}/{results['attempted']} succeeded")
        return 0 if results['failed'] == 0 else 1
    
    if args.daily_summary:
        maestro.send_daily_summary(date_str)
        return 0
    
    if args.approve_script:
        success = maestro.approve_script(args.approve_script, date_str)
        return 0 if success else 1
    
    # Run full pipeline
    results = maestro.run_pipeline(
        date_str, 
        skip_scout=args.skip_scout, 
        skip_breaking=args.skip_breaking
    )
    
    # Check if all stages succeeded
    all_success = all(
        s in ['success', 'skipped', 'sent', 'none', 'no_scripts'] or 
        isinstance(s, str) and s.startswith('processed')
        for s in results['stages'].values()
    )
    
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main())
