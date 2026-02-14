#!/usr/bin/env python3
"""
ScriptSmith - Retention-Optimized Script Generation
Generates 8-line drama scripts using Claude API with Flesch-Kincaid validation.
"""

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import urllib.request
import urllib.error

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, flesch_kincaid_grade, estimate_duration, log_operation, get_anthropic_api_key


class ScriptSmith:
    """Script generation agent with 8-line formula and quality gates."""
    
    # The 8-Line Formula Structure
    LINE_NAMES = [
        "HOOK",
        "SUPPORTING HOOK", 
        "DEVELOPING IDEA #1a",
        "DEVELOPING IDEA #1b",
        "DEVELOPING IDEA #2 + SETUP",
        "PAYOFF",
        "PAYOFF EXTENSION",
        "SUB INCENTIVE"
    ]
    
    # System prompt for Claude
    SYSTEM_PROMPT = '''You are ScriptSmith, an expert YouTube Shorts scriptwriter specializing in drama commentary.

Your task: Write a 45-50 second script following the EXACT 8-line formula below.

## THE 8-LINE FORMULA

Line 1 - HOOK (8-15 words): Stop the scroll. Bold, shocking claim.
Line 2 - SUPPORTING HOOK (10-20 words): Develop interest. Shut down the obvious conclusion.
Line 3 - DEVELOPING IDEA #1a (12-22 words): Fast date/timeline. Build story without revealing payoff.
Line 4 - DEVELOPING IDEA #1b (12-22 words): Continue building. Add detail that raises stakes.
Line 5 - DEVELOPING IDEA #2 + SETUP (12-22 words): Edge viewer more. Ask the question they should be thinking.
Line 6 - PAYOFF (10-18 words): The reveal. "damn wtf" moment.
Line 7 - PAYOFF EXTENSION (10-18 words): One more beat of reaction/context/implication.
Line 8 - SUB INCENTIVE (6-14 words): CTA tied directly to story content. Fast, punchy.

## CRITICAL RULES

1. Total: 130-150 words (45-50 seconds at ~2.8 words/sec)
2. Reading level: Grade 5-6 (simple words, short sentences)
3. Tension: Build continuously from Line 1 to Line 6
4. "Edge them but don't make them cum": Viewer must NEED the next line
5. Line 2 MUST shut down what viewer assumes will happen
6. After first name mention, use pronouns ("he", "she", "they") or synonyms
7. NO absolute dates: "last week", "three days ago" — never "February 14, 2026"
8. Commentary tone: Friend telling gossip, not news anchor
9. Story-tied CTA: Reference the specific story, never generic "subscribe for more"
10. NO hashtags, NO emojis
11. Every sentence pushes story forward

## OUTPUT FORMAT

Return ONLY a JSON object:
{
  "lines": ["line 1", "line 2", "line 3", "line 4", "line 5", "line 6", "line 7", "line 8"],
  "tone": "serious|ironic|shocked|hype",
  "hook_strength": 1-10,
  "visual_cues": [
    {"line": 1, "visual": "description"},
    ...
  ]
}'''

    def __init__(self):
        self.config = load_config()
        self.pipeline_dir = get_pipeline_dir()
        self.seeds_dir = self.pipeline_dir / "seeds"
        self.drafts_dir = self.pipeline_dir / "drafts"
        self.drafts_dir.mkdir(exist_ok=True)
        
        # Script settings
        script_config = self.config.get('script', {})
        self.target_word_count = [130, 150]
        self.target_duration = [45, 50]
        self.target_grade = [5.0, 6.0]
        self.max_rewrites = 2
        self.variations_per_seed = 3
        self.top_seeds = 3
        
        # API key
        self.api_key = get_anthropic_api_key()
    
    def load_seeds(self, date_str: Optional[str] = None) -> List[Dict]:
        """Load validated seeds from file."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        seeds_file = self.seeds_dir / f"{date_str}.json"
        if not seeds_file.exists():
            print(f"[ScriptSmith] Error: Seeds file not found: {seeds_file}")
            return []
        
        with open(seeds_file) as f:
            data = json.load(f)
        
        seeds = data.get('seeds', [])
        # Sort by priority and take top N
        seeds.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        return seeds[:self.top_seeds]
    
    def build_prompt(self, seed: Dict, variation: str) -> str:
        """Build generation prompt for a seed."""
        variation_prompts = {
            'A': 'Lead with the most shocking fact. Hit them hard immediately.',
            'B': 'Lead with irony or contrast. Make them do a double-take.',
            'C': 'Lead with a question or "imagine this" framing. Draw them in.'
        }
        
        prompt = f"""Write a drama commentary script for YouTube Shorts.

## STORY CONTEXT

Headline: {seed['headline']}
Source: {seed['source']}
Key Figures: {', '.join(seed.get('key_figures', []))}
Emotional Trigger: {seed.get('emotional_trigger', 'surprise')}
Risk Level: {seed.get('risk_level', 'low')}

## NARRATIVE ANGLE

{seed.get('narrative_angle', '')}

## FULL CONTEXT

{seed.get('context', '')[:1500]}

## VARIATION INSTRUCTION

{variation_prompts.get(variation, variation_prompts['A'])}

Tone: {seed.get('emotional_trigger', 'serious')}

Generate the 8-line script following the formula exactly."""
        
        return prompt
    
    def call_claude(self, prompt: str, max_tokens: int = 2000) -> Optional[Dict]:
        """Call Claude API to generate script."""
        if not self.api_key:
            print("[ScriptSmith] Error: ANTHROPIC_API_KEY not set")
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
            "system": self.SYSTEM_PROMPT,
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
                
                # Extract JSON from response
                # Look for JSON block
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
                
                # Try to find raw JSON object
                json_match = re.search(r'(\{[\s\S]*"lines"[\s\S]*\})', content)
                if json_match:
                    return json.loads(json_match.group(1))
                
                print(f"[ScriptSmith] Warning: Could not parse JSON from response")
                return None
                
        except urllib.error.HTTPError as e:
            print(f"[ScriptSmith] Claude API error: {e.code} - {e.reason}")
            try:
                error_body = e.read().decode('utf-8')
                print(f"[ScriptSmith] Error details: {error_body}")
            except:
                pass
            return None
        except Exception as e:
            print(f"[ScriptSmith] Error calling Claude: {e}")
            return None
    
    def parse_script(self, raw: Dict, seed: Dict, variation: str) -> Dict:
        """Parse and validate script structure."""
        lines = raw.get('lines', [])
        
        # Ensure exactly 8 lines
        if len(lines) != 8:
            print(f"[ScriptSmith] Warning: Script has {len(lines)} lines, expected 8")
            # Pad or trim
            while len(lines) < 8:
                lines.append("[Missing line]")
            lines = lines[:8]
        
        full_text = ' '.join(lines)
        word_count = len(full_text.split())
        
        script = {
            "variation": variation,
            "lines": lines,
            "full_text": full_text,
            "word_count": word_count,
            "duration_sec": round(word_count / 2.8, 1),
            "grade_level": flesch_kincaid_grade(full_text),
            "grade_pass": False,  # Set in quality check
            "duration_pass": False,  # Set in quality check
            "seed_id": seed['id'],
            "headline": seed['headline'][:100],
            "rewritten": False,
            "rewrite_count": 0,
            "hook_strength": raw.get('hook_strength', 5),
            "tone": raw.get('tone', 'serious'),
            "risk_flags": [seed.get('risk_level', 'low')] if seed.get('risk_level') != 'low' else [],
            "visual_cues": raw.get('visual_cues', [])
        }
        
        return script
    
    def check_quality(self, script: Dict) -> Tuple[bool, List[str]]:
        """Check script against quality gates."""
        issues = []
        
        # Word count check
        min_words, max_words = self.target_word_count
        if not (min_words <= script['word_count'] <= max_words):
            issues.append(f"Word count {script['word_count']} outside range {min_words}-{max_words}")
        
        # Duration check
        min_dur, max_dur = self.target_duration
        if not (min_dur <= script['duration_sec'] <= max_dur):
            issues.append(f"Duration {script['duration_sec']}s outside range {min_dur}-{max_dur}s")
        
        # Readability check
        min_grade, max_grade = self.target_grade
        grade = script['grade_level']
        if not (min_grade <= grade <= max_grade):
            issues.append(f"Grade level {grade} outside range {min_grade}-{max_grade}")
        
        # Line count check
        if len(script['lines']) != 8:
            issues.append(f"Line count {len(script['lines'])} != 8")
        
        # Set pass flags
        script['grade_pass'] = min_grade <= grade <= max_grade
        script['duration_pass'] = min_dur <= script['duration_sec'] <= max_dur
        
        passed = len(issues) == 0
        return passed, issues
    
    def rewrite_script(self, script: Dict, seed: Dict, issues: List[str]) -> Optional[Dict]:
        """Request Claude to rewrite script fixing issues."""
        issues_text = '\n'.join(f"- {issue}" for issue in issues)
        
        rewrite_prompt = f"""Rewrite this script to fix the following issues:

## ISSUES TO FIX
{issues_text}

## CURRENT SCRIPT

{chr(10).join(f"{i+1}. {line}" for i, line in enumerate(script['lines']))}

## STORY CONTEXT

{seed.get('context', '')[:800]}

## REWRITE INSTRUCTIONS

Fix all issues while maintaining the 8-line formula structure.
Use simpler words for readability.
Adjust length to hit 130-150 words.

Return ONLY the JSON object with the corrected script."""

        raw = self.call_claude(rewrite_prompt)
        if raw:
            new_script = self.parse_script(raw, seed, script['variation'])
            new_script['rewritten'] = True
            new_script['rewrite_count'] = script.get('rewrite_count', 0) + 1
            return new_script
        return None
    
    def generate_for_seed(self, seed: Dict) -> List[Dict]:
        """Generate all variations for a seed."""
        scripts = []
        variations = ['A', 'B', 'C'][:self.variations_per_seed]
        
        for variation in variations:
            print(f"[ScriptSmith] Generating variation {variation} for seed {seed['id']}")
            
            prompt = self.build_prompt(seed, variation)
            raw = self.call_claude(prompt)
            
            if not raw:
                print(f"[ScriptSmith] Failed to generate variation {variation}")
                continue
            
            script = self.parse_script(raw, seed, variation)
            
            # Quality check with auto-rewrite loop
            rewrite_count = 0
            while rewrite_count < self.max_rewrites:
                passed, issues = self.check_quality(script)
                if passed:
                    break
                
                print(f"[ScriptSmith] Quality issues: {issues}. Rewriting ({rewrite_count + 1}/{self.max_rewrites})...")
                new_script = self.rewrite_script(script, seed, issues)
                if new_script:
                    script = new_script
                    rewrite_count += 1
                else:
                    break
            
            # Final quality check
            passed, issues = self.check_quality(script)
            script['quality_passed'] = passed
            script['quality_issues'] = issues if not passed else []
            
            scripts.append(script)
            print(f"[ScriptSmith] Variation {variation}: {script['word_count']}w | {script['duration_sec']}s | Grade {script['grade_level']} | {'PASS' if passed else 'FAIL'}")
        
        return scripts
    
    def generate_markdown(self, scripts: List[Dict], date_str: str):
        """Generate human-readable markdown of scripts."""
        lines = [f"# Script Drafts - {date_str}", ""]
        
        for script in scripts:
            status = "✅ PASS" if script.get('quality_passed') else "❌ FAIL"
            lines.extend([
                f"## {script['headline'][:60]}... [Var {script['variation']}]",
                f"**Status:** {status} | **Words:** {script['word_count']} | **Duration:** {script['duration_sec']}s | **Grade:** {script['grade_level']}",
                f"**Seed:** {script['seed_id']} | **Tone:** {script['tone']}",
                "",
                "### Script",
                ""
            ])
            
            for i, line in enumerate(script['lines'], 1):
                line_name = self.LINE_NAMES[i-1]
                lines.append(f"{i}. **{line_name}:** {line}")
            
            if script.get('quality_issues'):
                lines.extend([
                    "",
                    "### Quality Issues",
                    ""
                ])
                for issue in script['quality_issues']:
                    lines.append(f"- {issue}")
            
            lines.append("")
            lines.append("---")
            lines.append("")
        
        md_file = self.drafts_dir / f"{date_str}.md"
        md_file.write_text('\n'.join(lines))
        print(f"[ScriptSmith] Markdown saved to: {md_file}")
    
    def generate_telegram_format(self, scripts: List[Dict], date_str: str):
        """Generate Telegram-formatted output."""
        lines = [f"🎬 DRAMA SCRIPTS — {date_str}", "=" * 40, f"{len([s for s in scripts if s.get('quality_passed')])} scripts ready for review:", ""]
        
        passing_scripts = [s for s in scripts if s.get('quality_passed')]
        
        for i, script in enumerate(passing_scripts[:5], 1):  # Top 5 only
            lines.extend([
                f"",
                f"📝 SCRIPT {i} [{script['variation']}] — {script['headline'][:50]}...",
                f"   {script['word_count']}w | ~{script['duration_sec']}s | Grade {script['grade_level']}",
                f""
            ])
            
            for j, line in enumerate(script['lines'], 1):
                lines.append(f"   {j}. {line}")
            
            lines.append("")
            lines.append("   —" * 10)
        
        txt_file = self.drafts_dir / f"{date_str}-telegram.txt"
        txt_file.write_text('\n'.join(lines))
        print(f"[ScriptSmith] Telegram format saved to: {txt_file}")
    
    def run(self, date_str: Optional[str] = None) -> Dict:
        """Run full ScriptSmith generation."""
        start_time = datetime.utcnow()
        print(f"[ScriptSmith] Starting generation at {start_time.isoformat()}Z")
        
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        # Load seeds
        seeds = self.load_seeds(date_str)
        if not seeds:
            print("[ScriptSmith] No seeds found. Exiting.")
            return {"error": "No seeds found"}
        
        print(f"[ScriptSmith] Processing {len(seeds)} seeds...")
        
        # Generate scripts for each seed
        all_scripts = []
        total_claude_calls = 0
        total_rewrites = 0
        
        for seed in seeds:
            scripts = self.generate_for_seed(seed)
            all_scripts.extend(scripts)
            total_claude_calls += len(scripts)
            total_rewrites += sum(s.get('rewrite_count', 0) for s in scripts)
        
        # Sort by quality (passing first), then hook strength
        all_scripts.sort(key=lambda x: (not x.get('quality_passed'), -x.get('hook_strength', 0)))
        
        # Build output
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        passing = [s for s in all_scripts if s.get('quality_passed')]
        failing = [s for s in all_scripts if not s.get('quality_passed')]
        
        output = {
            "date": date_str,
            "generated_at": start_time.isoformat() + "Z",
            "seed_count": len(seeds),
            "script_count": len(all_scripts),
            "scripts": all_scripts,
            "generation_stats": {
                "total_claude_calls": total_claude_calls,
                "total_rewrites": total_rewrites,
                "passing_count": len(passing),
                "failing_count": len(failing),
                "avg_grade_level": round(sum(s['grade_level'] for s in all_scripts) / len(all_scripts), 1) if all_scripts else 0,
                "avg_duration_sec": round(sum(s['duration_sec'] for s in all_scripts) / len(all_scripts), 1) if all_scripts else 0
            }
        }
        
        # Save JSON output
        json_file = self.drafts_dir / f"{date_str}.json"
        with open(json_file, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"[ScriptSmith] JSON saved to: {json_file}")
        
        # Generate markdown
        self.generate_markdown(all_scripts, date_str)
        
        # Generate telegram format
        self.generate_telegram_format(all_scripts, date_str)
        
        print(f"[ScriptSmith] Generation complete. {len(passing)} passing, {len(failing)} failing.")
        
        log_operation('ScriptSmith', 'generate', 'success', {
            'scripts_generated': len(all_scripts),
            'scripts_passing': len(passing),
            'total_rewrites': total_rewrites,
            'duration_sec': duration
        })
        
        return output


def main():
    """CLI entry point."""
    date_str = sys.argv[1] if len(sys.argv) > 1 else None
    
    smith = ScriptSmith()
    result = smith.run(date_str)
    
    if 'error' in result:
        print(f"\n[ScriptSmith] Error: {result['error']}")
        return 1
    
    print(f"\n{'='*50}")
    print(f"ScriptSmith Complete")
    print(f"{'='*50}")
    stats = result['generation_stats']
    print(f"Scripts: {stats['passing_count']} passing / {stats['failing_count']} failing")
    print(f"Claude calls: {stats['total_claude_calls']} (+ {stats['total_rewrites']} rewrites)")
    print(f"Avg grade: {stats['avg_grade_level']} | Avg duration: {stats['avg_duration_sec']}s")
    
    return 0 if stats['passing_count'] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
