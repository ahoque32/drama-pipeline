#!/usr/bin/env python3
"""
CodexBrain - Codex-powered QA & intervention engine for Maestro.
Routes through OpenClaw gateway (handles OAuth token refresh).
Zero cost on Plus/Pro plan.
"""

import json
import os
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any


class CodexBrain:
    """Codex-powered quality assessment and intervention for pipeline stages."""
    
    def __init__(self):
        # Direct OpenAI Responses API (uses your plan — $0 cost)
        self.api_key = os.environ.get('OPENAI_API_KEY', '')
        self.base_url = 'https://api.openai.com/v1'
        self.model = os.environ.get('MAESTRO_MODEL', 'openai-codex-anton/gpt-5.3-codex')
        
        if not self.api_key:
            print("[CodexBrain] WARNING: OPENAI_API_KEY not set!")
    
    def _call_codex(self, system_prompt: str, user_prompt: str, 
                     max_tokens: int = 2000, temperature: float = 0.3) -> Optional[str]:
        """Call Codex via OpenAI Responses API."""
        url = f"{self.base_url}/responses"
        
        # Strip provider prefix for API call (openai-codex-anton/gpt-5.3-codex → gpt-5.3-codex)
        api_model = self.model.split('/')[-1] if '/' in self.model else self.model
        
        payload = {
            "model": api_model,
            "instructions": system_prompt,
            "input": user_prompt,
            "max_output_tokens": max_tokens,
            "temperature": temperature
        }
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode('utf-8'),
                headers=headers,
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=90) as response:
                data = json.loads(response.read().decode('utf-8'))
                
                # Responses API returns output differently than Chat Completions
                output = data.get('output', [])
                if output:
                    # Extract text from output items
                    for item in output:
                        if item.get('type') == 'message':
                            for content in item.get('content', []):
                                if content.get('type') == 'output_text':
                                    return content.get('text', '')
                
                # Fallback: try common response formats
                if 'choices' in data:
                    return data['choices'][0]['message']['content']
                
                print(f"[CodexBrain] Unexpected response format: {json.dumps(data)[:200]}")
                return None
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else ''
            print(f"[CodexBrain] API error {e.code}: {error_body[:300]}")
            return None
        except Exception as e:
            print(f"[CodexBrain] Error calling Codex: {e}")
            return None
    
    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """Extract JSON from Codex response (handles markdown code blocks)."""
        if not response:
            return None
        
        # Try direct parse
        try:
            return json.loads(response)
        except:
            pass
        
        # Try extracting from code block
        import re
        match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', response, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except:
                pass
        
        # Try finding JSON object in response
        for start in range(len(response)):
            if response[start] == '{':
                for end in range(len(response), start, -1):
                    if response[end-1] == '}':
                        try:
                            return json.loads(response[start:end])
                        except:
                            continue
        
        return None
    
    # ========================================
    # STAGE 1: Scout QA
    # ========================================
    
    def evaluate_seeds(self, seeds: List[Dict], date_str: str) -> Dict:
        """
        Evaluate seed quality after Scout runs.
        Returns: {passed: bool, score: int, issues: [...], suggestions: [...], 
                  rerun_queries: [...], intervention_needed: bool}
        """
        system = """You are the QA brain for a YouTube Shorts drama content pipeline. 
Your job: evaluate whether today's drama seeds are good enough to produce viral Shorts.

You understand what makes drama content go viral on YouTube Shorts:
- Strong emotional triggers (outrage, surprise, betrayal, humor)
- Celebrity/influencer involvement (recognizable names)
- Trending topics with high engagement
- Conflict-driven narratives
- Visual storytelling potential

Be STRICT. Low-quality seeds waste the whole pipeline."""
        
        seed_summary = []
        for s in seeds[:10]:  # Top 10
            seed_summary.append({
                "headline": s.get("headline", "")[:100],
                "source": s.get("source", ""),
                "engagement": s.get("engagement", 0),
                "emotion": s.get("emotional_trigger", ""),
                "priority": s.get("priority_score", 0),
                "key_figures": s.get("key_figures", []),
                "risk": s.get("risk_level", "")
            })
        
        user = f"""Evaluate these {len(seeds)} drama seeds from {date_str}:

{json.dumps(seed_summary, indent=2)}

Respond in JSON:
{{
  "passed": true/false,       // Are there enough quality seeds to proceed?
  "score": 1-10,              // Overall seed batch quality
  "top_seeds": [0, 2],        // Indices of best seeds (0-indexed)
  "issues": ["..."],          // What's wrong with the batch
  "suggestions": ["..."],     // How to improve
  "rerun_queries": ["..."],   // If score < 5: plain keyword search queries for X API (NO site: prefix, just keywords like "drake beef diss response")
  "intervention_needed": true/false  // Should Maestro step in?
}}"""
        
        response = self._call_codex(system, user)
        result = self._parse_json_response(response)
        
        if not result:
            # Default: pass if we have seeds
            return {
                "passed": len(seeds) >= 1,
                "score": 5,
                "issues": ["Codex evaluation unavailable"],
                "suggestions": [],
                "rerun_queries": [],
                "intervention_needed": False
            }
        
        return result
    
    # ========================================
    # STAGE 2: Script QA
    # ========================================
    
    def evaluate_scripts(self, scripts: List[Dict], seeds: List[Dict]) -> Dict:
        """
        Evaluate script quality after ScriptSmith runs.
        Returns: {passed: bool, score: int, rewrites: [{index, issues, rewritten_lines}]}
        """
        system = """You are the script QA brain for a YouTube Shorts drama pipeline.
You evaluate 30-35 second drama scripts (7-8 lines) for viral potential.

Required structure:
1) Hook (line 1)
2) Supporting Hook (line 2)
3) Developing Idea #1 (lines 3-4)
4) Developing Idea #2 + Setup Payoff (lines 5-6)
5) Payoff + contextual Subscribe Incentive (lines 7-8)

Score each script on:
- Hook strength (0-10): would you stop scrolling?
- Threading quality (0-10): does the middle maintain tension?
- Payoff clarity (0-10): short, clear, satisfying reveal?
- Flow/naturalness (0-10): sounds natural when read aloud?
- CTA quality (0-10): contextual and compelling subscribe incentive?

Reject scripts missing any required section or using generic CTA.
You can REWRITE weak scripts directly."""
        
        script_data = []
        for i, s in enumerate(scripts[:5]):
            script_data.append({
                "index": i,
                "headline": s.get("headline", "")[:80],
                "lines": s.get("lines", []),
                "word_count": s.get("word_count", 0),
                "grade_level": s.get("grade_level", 0),
                "hook_strength": s.get("hook_strength", 0),
                "tone": s.get("tone", "")
            })
        
        user = f"""Evaluate these {len(script_data)} drama scripts:

{json.dumps(script_data, indent=2)}

For each script, either APPROVE or REWRITE. Respond in JSON:
{{
  "passed": true/false,
  "score": 1-10,
  "evaluations": [
    {{
      "index": 0,
      "verdict": "approve" or "rewrite",
      "hook_strength": 0-10,
      "threading_quality": 0-10,
      "payoff_clarity": 0-10,
      "flow_naturalness": 0-10,
      "cta_quality": 0-10,
      "structure_ok": true/false,
      "issues": ["..."],
      "rewritten_lines": ["line1", "line2", ...] // if rewrite: exactly 7 or 8 lines
    }}
  ],
  "intervention_needed": true/false
}}"""
        
        response = self._call_codex(system, user, max_tokens=4000)
        result = self._parse_json_response(response)
        
        if not result:
            return {
                "passed": len(scripts) >= 1,
                "score": 5,
                "evaluations": [],
                "intervention_needed": False
            }
        
        return result
    
    # ========================================
    # STAGE 3: Asset QA
    # ========================================
    
    def evaluate_assets(self, manifest: Dict, script: Dict) -> Dict:
        """
        Evaluate asset quality after AssetHunter runs.
        Returns: {passed: bool, score: int, gaps: [{line, issue, search_query}]}
        """
        system = """You are the asset QA brain for a YouTube Shorts drama pipeline.
You evaluate whether collected visual assets (videos, screenshots, images) match 
the script lines they're meant to illustrate.

Good assets:
- Directly relevant to the script line's content
- Video preferred over static images
- Real source material (tweets, clips) over generic stock
- High enough quality for YouTube Shorts (720p+)
- No watermarks, no stolen content

You suggest specific search queries to fill gaps."""
        
        # Build asset summary
        assets_summary = []
        lines = script.get("lines", [])
        for asset in manifest.get("assets", []):
            line_num = asset.get("line_number", 0)
            line_text = lines[line_num - 1] if line_num <= len(lines) else "?"
            assets_summary.append({
                "line": line_num,
                "line_text": line_text[:60],
                "type": asset.get("type", "unknown"),
                "has_file": bool(asset.get("local_path")),
                "auto_downloaded": asset.get("auto_downloaded", False),
                "notes": asset.get("sourcing_notes", "")[:80]
            })
        
        stats = manifest.get("stats", {})
        
        user = f"""Evaluate this asset collection for a drama Short:

Script headline: {script.get('headline', 'unknown')[:80]}
Stats: {stats.get('auto_collected', 0)}/{stats.get('total_lines', 0)} auto-collected, {stats.get('success_rate', 0)}% success

Assets:
{json.dumps(assets_summary, indent=2)}

Respond in JSON:
{{
  "passed": true/false,       // Enough assets to proceed?
  "score": 1-10,
  "gaps": [
    {{
      "line": 3,
      "issue": "No video for key scene",
      "search_query": "specific X or YouTube search query",
      "search_platform": "x" or "youtube" or "instagram"
    }}
  ],
  "intervention_needed": true/false
}}"""
        
        response = self._call_codex(system, user)
        result = self._parse_json_response(response)
        
        if not result:
            success_rate = manifest.get("stats", {}).get("success_rate", 0)
            return {
                "passed": success_rate >= 50,
                "score": 5,
                "gaps": [],
                "intervention_needed": False
            }
        
        return result
    
    # ========================================
    # INTERVENTION: Maestro takes over
    # ========================================
    
    def generate_search_queries(self, topic: str, platform: str = "x") -> List[str]:
        """Generate targeted search queries when a stage needs better content."""
        platform_notes = {
            "x": "For X/Twitter search API. Use plain keywords, NO site: prefix. Include drama-related terms. Example: 'drake responds beef diss track'",
            "youtube": "For YouTube search. Use descriptive keywords. Example: 'celebrity callout video drama'",
            "instagram": "For Instagram. Use hashtags or account names. Example: '#celebritydrama exposed'"
        }
        system = f"""Generate 5 highly specific search queries to find drama/viral content about this topic.
{platform_notes.get(platform, '')}
Return as JSON array of strings. NO site: prefixes. Plain search keywords only."""
        
        response = self._call_codex(system, f"Topic: {topic}")
        result = self._parse_json_response(response)
        
        if isinstance(result, list):
            return result
        return [topic]  # Fallback
    
    def rewrite_script(self, original_lines: List[str], headline: str, 
                        issues: List[str]) -> Optional[List[str]]:
        """Codex rewrites a weak script directly."""
        system = """You are a viral YouTube Shorts scriptwriter. Rewrite this drama script 
to be more engaging. Format: exactly 7 or 8 lines, 30-35 seconds (~85-100 words), grade 5-6.

Required structure:
- Line 1: Hook
- Line 2: Supporting hook
- Lines 3-4: Developing Idea #1
- Lines 5-6: Developing Idea #2 + setup question
- Final line: payoff + contextual subscribe incentive

The CTA must be story-specific (never generic "subscribe for more drama").
Write conversationally and naturally."""
        
        user = f"""Topic: {headline}

Original script (needs improvement):
{chr(10).join(f'{i+1}. {line}' for i, line in enumerate(original_lines))}

Issues identified:
{chr(10).join(f'- {issue}' for issue in issues)}

Rewrite all 8 lines. Return as JSON array of 8 strings."""
        
        response = self._call_codex(system, user, max_tokens=1500, temperature=0.7)
        result = self._parse_json_response(response)
        
        if isinstance(result, list) and len(result) in (7, 8):
            return result
        return None
    
    def diagnose_failure(self, stage: str, error: str, context: Dict) -> Dict:
        """Codex diagnoses why a stage failed and suggests fixes."""
        system = """You are debugging a YouTube Shorts drama pipeline.
A stage failed. Diagnose WHY and suggest the exact fix.
Be specific — generic advice is useless."""
        
        user = f"""Stage: {stage}
Error: {error}

Context:
{json.dumps(context, indent=2, default=str)[:2000]}

Respond in JSON:
{{
  "root_cause": "...",
  "fix": "...",
  "can_auto_fix": true/false,
  "retry_with_changes": {{...}},  // Modified params for retry
  "skip_safe": true/false          // Is it safe to skip this stage?
}}"""
        
        response = self._call_codex(system, user)
        return self._parse_json_response(response) or {
            "root_cause": "Unknown",
            "fix": "Manual investigation needed",
            "can_auto_fix": False,
            "skip_safe": False
        }


# Quick test
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    brain = CodexBrain()
    print(f"API Key: {'set' if brain.api_key else 'NOT SET'}")
    print(f"Model: {brain.model}")
    
    # Test connection
    result = brain._call_codex(
        "You are a test assistant. Respond in exactly 3 words.",
        "Say 'Codex brain online'."
    )
    print(f"Test response: {result}")
