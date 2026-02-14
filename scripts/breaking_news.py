#!/usr/bin/env python3
"""
BreakingNews - Fast-track protocol for high-priority seeds
Detects time_sensitivity: "high" and expedites to handoff in < 1 hour
"""

import json
import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import urllib.request

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation, get_anthropic_api_key


class BreakingNews:
    """Fast-track breaking news protocol."""
    
    # Single focused prompt for breaking news (no variations)
    BREAKING_PROMPT = '''You are ScriptSmith BREAKING MODE. Write a URGENT drama commentary script.

## BREAKING NEWS RULES
1. Speed > polish - get the story out FAST
2. Lead with the most shocking fact immediately
3. 8-line formula but punchier - aim for 40-45 seconds
4. Use simple words (Grade 5-6 max)
5. NO absolute dates - use "just broke", "minutes ago", "happening now"
6. Tone: urgent, live-update energy

## THE 8-LINE FORMULA (Accelerated)
Line 1 - HOOK (8-12 words): Breaking news hook. "Just broke" energy.
Line 2 - SUPPORTING HOOK (10-18 words): Why this matters RIGHT NOW.
Line 3 - DEVELOPING IDEA #1a (10-18 words): What happened - fast facts.
Line 4 - DEVELOPING IDEA #1b (10-18 words): Key detail that raises stakes.
Line 5 - DEVELOPING IDEA #2 + SETUP (10-18 words): The question everyone has.
Line 6 - PAYOFF (8-15 words): The reveal/answer.
Line 7 - PAYOFF EXTENSION (8-15 words): Implication or reaction.
Line 8 - SUB INCENTIVE (6-12 words): Breaking-style CTA. "More updates coming."

## OUTPUT FORMAT
Return ONLY JSON:
{
  "lines": ["line 1", "line 2", "line 3", "line 4", "line 5", "line 6", "line 7", "line 8"],
  "tone": "urgent|breaking|shocked",
  "hook_strength": 1-10,
  "visual_cues": [{"line": 1, "visual": "description"}, ...]
}'''

    def __init__(self):
        self.config = load_config()
        self.pipeline_dir = get_pipeline_dir()
        self.seeds_dir = self.pipeline_dir / "seeds"
        self.drafts_dir = self.pipeline_dir / "drafts"
        self.approved_dir = self.pipeline_dir / "approved"
        self.audio_dir = self.pipeline_dir / "audio"
        self.assets_dir = self.pipeline_dir / "assets"
        self.handoffs_dir = self.pipeline_dir / "handoffs"
        self.breaking_dir = self.pipeline_dir / "breaking"
        
        # Create directories
        for d in [self.breaking_dir, self.approved_dir, self.audio_dir, self.assets_dir, self.handoffs_dir]:
            d.mkdir(exist_ok=True)
        
        # Telegram config
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        # API key
        self.api_key = get_anthropic_api_key()
        
        # Tracking
        self.cost_tracker = {
            'claude_calls': 0,
            'input_tokens': 0,
            'output_tokens': 0,
            'estimated_cost': 0.0
        }
    
    def load_seeds(self, date_str: Optional[str] = None) -> List[Dict]:
        """Load seeds and filter for breaking news (time_sensitivity: high)."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        seeds_file = self.seeds_dir / f"{date_str}.json"
        if not seeds_file.exists():
            print(f"[BreakingNews] No seeds file found: {seeds_file}")
            return []
        
        with open(seeds_file) as f:
            data = json.load(f)
        
        all_seeds = data.get('seeds', [])
        
        # Filter for high time sensitivity
        breaking_seeds = [
            s for s in all_seeds 
            if s.get('time_sensitivity') == 'high' and s.get('priority_score', 0) >= 7.0
        ]
        
        # Sort by priority score
        breaking_seeds.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        
        print(f"[BreakingNews] Found {len(breaking_seeds)} breaking news seeds (of {len(all_seeds)} total)")
        
        return breaking_seeds
    
    def build_breaking_prompt(self, seed: Dict) -> str:
        """Build urgent prompt for breaking news."""
        prompt = f"""BREAKING NEWS - Write URGENT script NOW.

## STORY CONTEXT

Headline: {seed['headline']}
Source: {seed['source']}
Key Figures: {', '.join(seed.get('key_figures', []))}
Emotional Trigger: {seed.get('emotional_trigger', 'surprise')}
Risk Level: {seed.get('risk_level', 'low')}
Priority Score: {seed.get('priority_score', 0)}/10

## NARRATIVE ANGLE

{seed.get('narrative_angle', '')}

## FULL CONTEXT

{seed.get('context', '')[:1200]}

## URGENCY INSTRUCTIONS

This is BREAKING. Write with "just happened" energy.
Aim for 120-135 words (40-45 seconds).
Use words like "just broke", "happening now", "minutes ago".
Return ONLY the JSON object."""
        
        return prompt
    
    def call_claude(self, prompt: str, max_tokens: int = 1500) -> Optional[Dict]:
        """Call Claude API for breaking news script."""
        if not self.api_key:
            print("[BreakingNews] Error: ANTHROPIC_API_KEY not set")
            return None
        
        url = "https://api.anthropic.com/v1/messages"
        
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "anthropic-version": "2023-06-01"
        }
        
        data = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "system": self.BREAKING_PROMPT,
            "messages": [{"role": "user", "content": prompt}]
        }
        
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers=headers,
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode('utf-8'))
                content = result['content'][0]['text']
                
                # Track cost
                self.cost_tracker['claude_calls'] += 1
                self.cost_tracker['input_tokens'] += result.get('usage', {}).get('input_tokens', 0)
                self.cost_tracker['output_tokens'] += result.get('usage', {}).get('output_tokens', 0)
                self.cost_tracker['estimated_cost'] += 0.008  # Approximate per-call cost
                
                # Extract JSON
                import re
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
                
                json_match = re.search(r'(\{[\s\S]*"lines"[\s\S]*\})', content)
                if json_match:
                    return json.loads(json_match.group(1))
                
                print(f"[BreakingNews] Warning: Could not parse JSON from response")
                return None
                
        except Exception as e:
            print(f"[BreakingNews] Claude API error: {e}")
            return None
    
    def parse_script(self, raw: Dict, seed: Dict) -> Dict:
        """Parse breaking news script."""
        lines = raw.get('lines', [])
        
        # Ensure exactly 8 lines
        while len(lines) < 8:
            lines.append("[Missing line]")
        lines = lines[:8]
        
        full_text = ' '.join(lines)
        word_count = len(full_text.split())
        
        # Simple grade calculation
        from utils import flesch_kincaid_grade
        
        script = {
            "variation": "BREAKING",
            "lines": lines,
            "full_text": full_text,
            "word_count": word_count,
            "duration_sec": round(word_count / 2.9, 1),  # Slightly faster for breaking
            "grade_level": flesch_kincaid_grade(full_text),
            "seed_id": seed['id'],
            "headline": seed['headline'][:100],
            "breaking": True,
            "hook_strength": raw.get('hook_strength', 8),
            "tone": raw.get('tone', 'urgent'),
            "risk_flags": [seed.get('risk_level', 'low')] if seed.get('risk_level') != 'low' else [],
            "visual_cues": raw.get('visual_cues', []),
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }
        
        return script
    
    def send_urgent_telegram(self, script: Dict, seed: Dict) -> bool:
        """Send ⚡ URGENT Telegram alert."""
        if not self.telegram_token or not self.telegram_chat_id:
            print("[BreakingNews] Warning: Telegram not configured")
            return False
        
        # Build urgent message
        lines = [
            "⚡ <b>BREAKING NEWS - URGENT</b> ⚡",
            "",
            f"🚨 <b>{seed['headline'][:80]}...</b>",
            "",
            f"📊 Priority: {seed.get('priority_score', 0)}/10 | Source: {seed['source']}",
            f"🎭 Emotion: {seed.get('emotional_trigger', 'surprise')} | Risk: {seed.get('risk_level', 'low')}",
            f"⏱️ Duration: {script['duration_sec']}s | Words: {script['word_count']}",
            "",
            "<b>SCRIPT:</b>",
            ""
        ]
        
        for i, line in enumerate(script['lines'], 1):
            lines.append(f"{i}. {line}")
        
        lines.extend([
            "",
            f"<b>Source:</b> {seed['source_urls'][0]}",
            "",
            "⚡ <b>AUTO-APPROVED</b> - Pipeline triggered",
            f"Generated: {script['generated_at'][:19]}Z"
        ])
        
        message = '\n'.join(lines)
        
        # Send message
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
                    print(f"[BreakingNews] ⚡ URGENT alert sent to Telegram")
                    return True
                else:
                    print(f"[BreakingNews] Telegram error: {result}")
                    return False
        except Exception as e:
            print(f"[BreakingNews] Telegram send error: {e}")
            return False
    
    def save_breaking_record(self, script: Dict, seed: Dict) -> Path:
        """Save breaking news record."""
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        record_dir = self.breaking_dir / timestamp
        record_dir.mkdir(exist_ok=True)
        
        record = {
            "breaking_id": f"breaking-{timestamp}",
            "seed": seed,
            "script": script,
            "detected_at": datetime.utcnow().isoformat() + "Z",
            "cost": self.cost_tracker,
            "auto_approved": True
        }
        
        # Save JSON
        with open(record_dir / "record.json", 'w') as f:
            json.dump(record, f, indent=2)
        
        # Save markdown
        md_lines = [
            f"# ⚡ BREAKING NEWS - {timestamp}",
            "",
            f"**Headline:** {seed['headline']}",
            f"**Source:** {seed['source']}",
            f"**Priority Score:** {seed.get('priority_score', 0)}/10",
            f"**Detected:** {record['detected_at']}",
            "",
            "## Script",
            ""
        ]
        
        for i, line in enumerate(script['lines'], 1):
            md_lines.append(f"{i}. {line}")
        
        md_lines.extend([
            "",
            "## Stats",
            f"- Words: {script['word_count']}",
            f"- Duration: {script['duration_sec']}s",
            f"- Grade Level: {script['grade_level']}",
            f"- Hook Strength: {script['hook_strength']}/10",
            "",
            "## Source",
            f"- {seed['source_urls'][0]}"
        ])
        
        with open(record_dir / "script.md", 'w') as f:
            f.write('\n'.join(md_lines))
        
        print(f"[BreakingNews] Record saved to: {record_dir}")
        return record_dir
    
    def auto_approve_and_trigger(self, script: Dict, seed: Dict, record_dir: Path) -> bool:
        """Auto-approve and trigger VoiceForge, AssetHunter, Handoff."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Save to approved directory
        approved_date_dir = self.approved_dir / date_str / f"breaking-{record_dir.name}"
        approved_date_dir.mkdir(parents=True, exist_ok=True)
        
        approved_record = {
            "script": script,
            "seed": seed,
            "approved_at": datetime.utcnow().isoformat() + "Z",
            "approved_by": "auto-breaking-news",
            "breaking": True
        }
        
        with open(approved_date_dir / "script.json", 'w') as f:
            json.dump(approved_record, f, indent=2)
        
        # Save markdown
        md_content = f"# ⚡ Auto-Approved Breaking News\n\n"
        md_content += f"**Headline:** {script['headline']}\n"
        md_content += f"**Breaking ID:** {record_dir.name}\n\n"
        md_content += "## Script\n\n"
        
        for i, line in enumerate(script['lines'], 1):
            md_content += f"{i}. {line}\n"
        
        with open(approved_date_dir / "script.md", 'w') as f:
            f.write(md_content)
        
        print(f"[BreakingNews] Auto-approved to: {approved_date_dir}")
        
        # Trigger downstream modules
        self._trigger_voiceforge(approved_date_dir, date_str)
        self._trigger_assethunter(approved_date_dir, date_str)
        self._trigger_handoff(date_str, record_dir.name)
        
        return True
    
    def _trigger_voiceforge(self, approved_dir: Path, date_str: str):
        """Trigger VoiceForge for breaking script."""
        print(f"[BreakingNews] Triggering VoiceForge...")
        
        try:
            result = subprocess.run(
                [sys.executable, str(self.pipeline_dir / "scripts" / "voiceforge.py"), 
                 "--date", date_str, "--breaking", approved_dir.name],
                capture_output=True, text=True, timeout=180
            )
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            return result.returncode == 0
        except Exception as e:
            print(f"[BreakingNews] VoiceForge failed: {e}")
            return False
    
    def _trigger_assethunter(self, approved_dir: Path, date_str: str):
        """Trigger AssetHunter for breaking script."""
        print(f"[BreakingNews] Triggering AssetHunter...")
        
        try:
            result = subprocess.run(
                [sys.executable, str(self.pipeline_dir / "scripts" / "assethunter.py"), 
                 "--date", date_str, "--breaking", approved_dir.name],
                capture_output=True, text=True, timeout=120
            )
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            return result.returncode == 0
        except Exception as e:
            print(f"[BreakingNews] AssetHunter failed: {e}")
            return False
    
    def _trigger_handoff(self, date_str: str, breaking_id: str):
        """Trigger HandoffAssembler for breaking package."""
        print(f"[BreakingNews] Triggering HandoffAssembler...")
        
        try:
            result = subprocess.run(
                [sys.executable, str(self.pipeline_dir / "scripts" / "handoff_assembler.py"), 
                 "--date", date_str, "--breaking", breaking_id],
                capture_output=True, text=True, timeout=60
            )
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            return result.returncode == 0
        except Exception as e:
            print(f"[BreakingNews] HandoffAssembler failed: {e}")
            return False
    
    def process_breaking_news(self, date_str: Optional[str] = None) -> Dict:
        """Main entry: detect and process breaking news."""
        start_time = datetime.utcnow()
        print(f"\n{'='*60}")
        print(f"⚡ BREAKING NEWS PROTOCOL - {start_time.isoformat()}Z")
        print(f"{'='*60}\n")
        
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        results = {
            "date": date_str,
            "started_at": start_time.isoformat() + "Z",
            "breaking_seeds_found": 0,
            "scripts_generated": 0,
            "scripts_processed": 0,
            "telegram_alerts_sent": 0,
            "cost": self.cost_tracker
        }
        
        # Load breaking seeds
        breaking_seeds = self.load_seeds(date_str)
        results['breaking_seeds_found'] = len(breaking_seeds)
        
        if not breaking_seeds:
            print("[BreakingNews] No breaking news seeds found")
            results['completed_at'] = datetime.utcnow().isoformat() + "Z"
            return results
        
        # Process top breaking seed (only process 1 for speed)
        seed = breaking_seeds[0]
        print(f"[BreakingNews] Processing top seed: {seed['headline'][:60]}...")
        print(f"[BreakingNews] Priority: {seed.get('priority_score', 0)}/10")
        
        # Generate script
        prompt = self.build_breaking_prompt(seed)
        raw = self.call_claude(prompt)
        
        if not raw:
            print("[BreakingNews] Failed to generate script")
            results['completed_at'] = datetime.utcnow().isoformat() + "Z"
            return results
        
        script = self.parse_script(raw, seed)
        results['scripts_generated'] = 1
        
        print(f"[BreakingNews] Script generated: {script['word_count']}w | {script['duration_sec']}s")
        
        # Save record
        record_dir = self.save_breaking_record(script, seed)
        
        # Send urgent Telegram alert
        telegram_ok = self.send_urgent_telegram(script, seed)
        if telegram_ok:
            results['telegram_alerts_sent'] += 1
        
        # Auto-approve and trigger pipeline
        auto_ok = self.auto_approve_and_trigger(script, seed, record_dir)
        if auto_ok:
            results['scripts_processed'] = 1
        
        # Final stats
        duration = (datetime.utcnow() - start_time).total_seconds()
        results['duration_sec'] = round(duration, 1)
        results['completed_at'] = datetime.utcnow().isoformat() + "Z"
        results['cost'] = self.cost_tracker
        
        print(f"\n{'='*60}")
        print(f"⚡ BREAKING NEWS COMPLETE")
        print(f"{'='*60}")
        print(f"Duration: {duration:.1f}s")
        print(f"Cost: ${self.cost_tracker['estimated_cost']:.3f}")
        print(f"Status: {'✅ PROCESSED' if auto_ok else '❌ FAILED'}")
        
        log_operation('BreakingNews', 'process', 'success' if auto_ok else 'failed', results)
        
        return results


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='BreakingNews - Fast-track breaking news protocol')
    parser.add_argument('--date', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--check-only', action='store_true', help='Only check for breaking seeds, do not process')
    
    args = parser.parse_args()
    
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    breaking = BreakingNews()
    
    if args.check_only:
        # Just check and report
        seeds = breaking.load_seeds(date_str)
        print(f"\n[BreakingNews] Check complete: {len(seeds)} breaking seeds found")
        for i, seed in enumerate(seeds[:3], 1):
            print(f"  {i}. [{seed.get('priority_score', 0)}/10] {seed['headline'][:60]}...")
        return 0 if seeds else 0  # Not an error if no breaking news
    
    # Full processing
    results = breaking.process_breaking_news(date_str)
    
    return 0 if results['scripts_processed'] > 0 or results['breaking_seeds_found'] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
