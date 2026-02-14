#!/usr/bin/env python3
"""
AssetHunter - Visual Asset Sourcing Module
Finds and downloads screenshots, clips, and b-roll for scripts
"""

import json
import os
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, log_operation


class AssetHunter:
    """Visual asset sourcing agent."""
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.approved_dir = self.pipeline_dir / "approved"
        self.seeds_dir = self.pipeline_dir / "seeds"
        self.assets_dir = self.pipeline_dir / "assets"
        self.assets_dir.mkdir(exist_ok=True)
    
    def load_approved_script(self, date_str: str, script_id: Optional[str] = None) -> Optional[Dict]:
        """Load approved script with visual cues."""
        approved_date_dir = self.approved_dir / date_str
        
        if not approved_date_dir.exists():
            return None
        
        # Find script - check root first, then subdirs
        script_file = approved_date_dir / "script.json"
        if script_file.exists():
            with open(script_file) as f:
                return json.load(f)
        
        # Find in subdirs
        for subdir in approved_date_dir.iterdir():
            if subdir.is_dir():
                script_file = subdir / "script.json"
                if script_file.exists():
                    with open(script_file) as f:
                        return json.load(f)
        
        return None
    
    def load_seed_data(self, seed_id: str, date_str: str) -> Optional[Dict]:
        """Load original seed data for source URLs."""
        seeds_file = self.seeds_dir / f"{date_str}.json"
        
        if not seeds_file.exists():
            return None
        
        with open(seeds_file) as f:
            data = json.load(f)
        
        for seed in data.get('seeds', []):
            if seed.get('id') == seed_id:
                return seed
        
        return None
    
    def suggest_assets_for_line(self, line_text: str, line_num: int, 
                                 seed: Optional[Dict] = None) -> Dict:
        """Suggest visual assets for a script line."""
        line_lower = line_text.lower()
        
        # Default asset suggestion
        asset = {
            "line_number": line_num,
            "type": "b-roll",
            "description": f"Visual for: {line_text[:60]}...",
            "source_url": None,
            "local_path": None,
            "copyright_status": "needs_verification",
            "permission_needed": True,
            "sourcing_notes": ""
        }
        
        # Check for specific patterns
        if any(word in line_lower for word in ['tweet', 'posted', 'x.com', 'twitter']):
            asset["type"] = "screenshot"
            asset["description"] = f"Screenshot of tweet mentioned in line {line_num}"
            if seed and seed.get('source_urls'):
                asset["source_url"] = seed['source_urls'][0]
                asset["copyright_status"] = "fair_use_commentary"
                asset["permission_needed"] = False
                asset["sourcing_notes"] = "Capture screenshot from source URL"
        
        elif any(word in line_lower for word in ['video', 'clip', 'footage', 'showed']):
            asset["type"] = "clip"
            asset["description"] = f"Video clip related to: {line_text[:60]}..."
            asset["sourcing_notes"] = "Search for clean footage without watermarks"
        
        elif any(word in line_lower for word in ['photo', 'image', 'picture', 'pic']):
            asset["type"] = "screenshot"
            asset["description"] = f"Image related to: {line_text[:60]}..."
        
        elif line_num == 1:
            # Hook line needs strongest visual
            asset["type"] = "hook_visual"
            asset["description"] = f"EYE-CATCHING visual for hook: {line_text[:60]}..."
            asset["sourcing_notes"] = "Most important visual - must stop the scroll"
        
        return asset
    
    def download_screenshot(self, url: str, output_path: Path) -> bool:
        """Download screenshot from URL.
        
        Note: For X/Twitter screenshots, we need to use a screenshot service
        or browser automation. This is a placeholder.
        """
        # Placeholder: In production, use playwright/selenium to capture
        # For now, create a placeholder file with instructions
        
        placeholder = {
            "url": url,
            "output_path": str(output_path),
            "status": "pending_capture",
            "instructions": "Use browser screenshot tool or manual capture",
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        
        meta_path = output_path.with_suffix('.json')
        with open(meta_path, 'w') as f:
            json.dump(placeholder, f, indent=2)
        
        print(f"[AssetHunter] Screenshot placeholder: {meta_path}")
        return True
    
    def search_stock_footage(self, query: str) -> List[Dict]:
        """Search for stock footage related to query.
        
        Sources: Pexels, Pixabay, or other free stock sites.
        """
        # Placeholder for stock footage search
        # In production, integrate with Pexels/Pixabay APIs
        
        return [{
            "source": "stock_search_pending",
            "query": query,
            "note": "Search Pexels/Pixabay for: " + query
        }]
    
    def hunt_assets(self, date_str: str, script_id: Optional[str] = None) -> Optional[Dict]:
        """Hunt for all assets needed for a script."""
        print(f"[AssetHunter] Hunting assets for {date_str}...")
        
        # Load approved script
        approved = self.load_approved_script(date_str, script_id)
        if not approved:
            print(f"[AssetHunter] No approved script found")
            return None
        
        script = approved.get('script', {})
        lines = script.get('lines', [])
        seed_id = script.get('seed_id')
        
        # Load seed data
        seed = self.load_seed_data(seed_id, date_str) if seed_id else None
        
        # Create output directory
        output_dir = self.assets_dir / date_str
        output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        screenshots_dir = output_dir / "screenshots"
        clips_dir = output_dir / "clips"
        stock_dir = output_dir / "stock"
        
        for d in [screenshots_dir, clips_dir, stock_dir]:
            d.mkdir(exist_ok=True)
        
        # Suggest assets for each line
        assets = []
        missing_lines = []
        
        print(f"[AssetHunter] Analyzing {len(lines)} lines for asset needs...")
        
        for i, line in enumerate(lines, 1):
            asset = self.suggest_assets_for_line(line, i, seed)
            
            # Try to source the asset
            if asset["type"] == "screenshot" and asset.get("source_url"):
                # Download screenshot
                output_path = screenshots_dir / f"line-{i}.png"
                if self.download_screenshot(asset["source_url"], output_path):
                    asset["local_path"] = str(output_path)
            
            assets.append(asset)
            
            # Track missing assets
            if not asset.get("local_path"):
                missing_lines.append(i)
        
        # Generate editor notes
        editor_notes = self._generate_editor_notes(script, assets, missing_lines)
        
        # Build manifest
        manifest = {
            "script_id": seed_id,
            "date": date_str,
            "assets": assets,
            "missing_assets": missing_lines,
            "editor_notes": editor_notes,
            "output_dir": str(output_dir),
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        
        # Save manifest
        manifest_file = output_dir / "manifest.json"
        with open(manifest_file, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Generate sourcing instructions
        self._generate_sourcing_instructions(output_dir, assets, seed)
        
        print(f"[AssetHunter] Manifest saved: {manifest_file}")
        print(f"[AssetHunter] Assets found: {len(assets) - len(missing_lines)}/{len(assets)}")
        
        log_operation('AssetHunter', 'hunt', 'success', {
            'script_id': seed_id,
            'total_assets': len(assets),
            'missing_assets': len(missing_lines)
        })
        
        return manifest
    
    def _generate_editor_notes(self, script: Dict, assets: List[Dict], 
                                missing_lines: List[int]) -> Dict:
        """Generate notes for the video editor."""
        lines = script.get('lines', [])
        
        notes = {
            "hook_placement": "First 3 seconds must have strongest visual. Use most eye-catching asset.",
            "mid_spike_location": "Line 5-6 (setup to payoff) - add visual energy spike",
            "loop_close_strategy": "Connect ending back to opening hook visually",
            "reference_channel": "Starless Shorts (youtube.com/@starless./shorts)",
            "special_instructions": "",
            "asset_mapping": []
        }
        
        # Map assets to lines
        for asset in assets:
            line_num = asset["line_number"]
            if line_num <= len(lines):
                notes["asset_mapping"].append({
                    "line": line_num,
                    "text_preview": lines[line_num - 1][:50] + "...",
                    "asset_type": asset["type"],
                    "status": "ready" if asset.get("local_path") else "needs_sourcing",
                    "notes": asset.get("sourcing_notes", "")
                })
        
        # Add missing asset guidance
        if missing_lines:
            notes["special_instructions"] = f"Lines needing manual asset sourcing: {missing_lines}"
        
        return notes
    
    def _generate_sourcing_instructions(self, output_dir: Path, assets: List[Dict],
                                         seed: Optional[Dict]):
        """Generate human-readable sourcing instructions."""
        lines = [
            f"# Asset Sourcing Instructions",
            f"",
            f"## Quick Sources",
            f"",
            f"- **X/Twitter screenshots:** Use browser dev tools or snipping tool",
            f"- **Instagram:** snapinsta.app (download without watermark)",
            f"- **YouTube clips:** yt-dlp or Crayo AI's built-in downloader",
            f"- **Stock footage:** Pexels.com, Pixabay.com (free)",
            f"- **Gameplay:** Record directly or use clean existing footage",
            f"",
            f"## Asset Rules (STRICT)",
            f"",
            f"- ❌ NO reposted TikToks (original source only)",
            f"- ❌ NO watermarks (clean footage only)",
            f"- ❌ NO stolen Shorts (don't use other creators' Shorts)",
            f"- ✅ DO DM creators for permission to use their clips",
            f"- ✅ DO use gameplay, stock, screen recordings, clean X clips",
            f"",
            f"## Assets Needed",
            f""
        ]
        
        for asset in assets:
            status = "✅ Ready" if asset.get("local_path") else "⬜ Needs Sourcing"
            lines.extend([
                f"### Line {asset['line_number']} - {asset['type'].upper()}",
                f"**Status:** {status}",
                f"**Description:** {asset['description']}",
            ])
            
            if asset.get("source_url"):
                lines.append(f"**Source:** {asset['source_url']}")
            
            if asset.get("sourcing_notes"):
                lines.append(f"**Notes:** {asset['sourcing_notes']}")
            
            if asset.get("copyright_status"):
                lines.append(f"**Copyright:** {asset['copyright_status']}")
            
            lines.append(f"")
        
        if seed and seed.get('source_urls'):
            lines.extend([
                f"## Original Sources",
                f""
            ])
            for url in seed['source_urls']:
                lines.append(f"- {url}")
        
        instructions_file = output_dir / "SOURCING.md"
        instructions_file.write_text('\n'.join(lines))
        
        print(f"[AssetHunter] Sourcing instructions: {instructions_file}")
    
    def run(self, date_str: Optional[str] = None, script_id: Optional[str] = None) -> Optional[Dict]:
        """CLI entry point."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        return self.hunt_assets(date_str, script_id)


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='AssetHunter - Visual Asset Sourcing')
    parser.add_argument('--date', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--script-id', help='Specific script ID to process')
    
    args = parser.parse_args()
    
    hunter = AssetHunter()
    result = hunter.run(args.date, args.script_id)
    
    if result:
        print(f"\n{'='*50}")
        print(f"AssetHunter Complete")
        print(f"{'='*50}")
        print(f"Assets: {len(result['assets'])}")
        print(f"Missing: {len(result['missing_assets'])}")
        print(f"Manifest: {result['output_dir']}/manifest.json")
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
