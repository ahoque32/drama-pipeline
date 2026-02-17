#!/usr/bin/env python3
"""
HandoffAssembler - Complete Package Assembly for Editor
Assembles script + voiceover + assets + notes into handoff package
"""

import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, load_config, log_operation


class HandoffAssembler:
    """Assembles complete handoff packages for Lemuel."""
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.approved_dir = self.pipeline_dir / "approved"
        self.audio_dir = self.pipeline_dir / "audio"
        self.assets_dir = self.pipeline_dir / "assets"
        self.handoffs_dir = self.pipeline_dir / "handoffs"
        self.handoffs_dir.mkdir(exist_ok=True)
        
        self.config = load_config()
        self.gog_account = self.config.get('google_drive', {}).get('gog_account', 'admin@renderwise.net')
    
    def load_approved_script(self, date_str: str) -> Optional[Dict]:
        """Load approved script data."""
        approved_date_dir = self.approved_dir / date_str
        
        if not approved_date_dir.exists():
            return None
        
        # Check root first
        script_file = approved_date_dir / "script.json"
        if script_file.exists():
            with open(script_file) as f:
                return json.load(f)
        
        # Check subdirs
        for subdir in approved_date_dir.iterdir():
            if subdir.is_dir():
                script_file = subdir / "script.json"
                if script_file.exists():
                    with open(script_file) as f:
                        return json.load(f)
        
        return None
    
    def load_voiceover(self, date_str: str) -> Optional[Dict]:
        """Load voiceover data."""
        voiceover_file = self.audio_dir / date_str / "voiceover.json"
        
        if voiceover_file.exists():
            with open(voiceover_file) as f:
                return json.load(f)
        
        return None
    
    def load_assets(self, date_str: str) -> Optional[Dict]:
        """Load assets manifest."""
        manifest_file = self.assets_dir / date_str / "manifest.json"
        
        if manifest_file.exists():
            with open(manifest_file) as f:
                return json.load(f)
        
        return None
    
    def assemble_handoff(self, date_str: str, content_id: Optional[str] = None) -> Optional[Dict]:
        """Assemble complete handoff package."""
        print(f"[Handoff] Assembling package for {date_str}...")
        
        # Load all components
        approved = self.load_approved_script(date_str)
        voiceover = self.load_voiceover(date_str)
        assets = self.load_assets(date_str)
        
        if not approved:
            print(f"[Handoff] Error: No approved script found for {date_str}")
            return None
        
        script = approved.get('script', {})
        
        # Create handoff directory
        if content_id:
            handoff_id = f"handoff-{date_str}-{content_id}"
        else:
            handoff_id = f"handoff-{date_str}-001"
        
        handoff_dir = self.handoffs_dir / date_str
        handoff_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        script_dir = handoff_dir / "script"
        audio_dir = handoff_dir / "audio"
        assets_out_dir = handoff_dir / "assets"
        
        for d in [script_dir, audio_dir, assets_out_dir]:
            d.mkdir(exist_ok=True)
        
        # Copy script
        script_data = {
            "text": '\n'.join(script.get('lines', [])),
            "lines": script.get('lines', []),
            "duration_sec": script.get('duration_sec', 0),
            "tone": script.get('tone', 'serious'),
            "word_count": script.get('word_count', 0)
        }
        
        with open(script_dir / "script.json", 'w') as f:
            json.dump(script_data, f, indent=2)
        
        with open(script_dir / "script.md", 'w') as f:
            f.write(f"# Script - {date_str}\n\n")
            for i, line in enumerate(script.get('lines', []), 1):
                f.write(f"{i}. {line}\n")
        
        # Copy voiceover if available
        voiceover_data = {
            "combined_path": None,
            "segment_paths": [],
            "voice_id": "DanDan",
            "duration_sec": script.get('duration_sec', 0)
        }
        
        if voiceover:
            # Copy combined audio
            if voiceover.get('combined_audio'):
                src = Path(voiceover['combined_audio'])
                if src.exists():
                    dst = audio_dir / "combined.mp3"
                    shutil.copy2(src, dst)
                    voiceover_data['combined_path'] = str(dst)
            
            # Copy segments
            for seg_path in voiceover.get('segment_paths', []):
                src = Path(seg_path)
                if src.exists():
                    dst = audio_dir / "segments" / src.name
                    dst.parent.mkdir(exist_ok=True)
                    shutil.copy2(src, dst)
                    voiceover_data['segment_paths'].append(str(dst))
            
            voiceover_data['voice_id'] = voiceover.get('voice_id', 'DanDan')
        
        # Copy assets if available
        assets_data = {
            "clips": [],
            "missing": []
        }
        
        if assets:
            # Copy available assets
            for asset in assets.get('assets', []):
                if asset.get('local_path'):
                    src = Path(asset['local_path'])
                    if src.exists():
                        dst = assets_out_dir / src.name
                        shutil.copy2(src, dst)
                        assets_data['clips'].append({
                            "line_number": asset['line_number'],
                            "path": str(dst),
                            "type": asset.get('type', 'b-roll'),
                            "description": asset.get('description', '')
                        })
            
            assets_data['missing'] = assets.get('missing_assets', [])
            
            # Copy manifest
            shutil.copy2(self.assets_dir / date_str / "manifest.json", 
                        assets_out_dir / "manifest.json")
        
        # Generate editor notes
        editor_notes = self._generate_editor_notes(script, assets, voiceover)
        
        with open(handoff_dir / "editor_notes.json", 'w') as f:
            json.dump(editor_notes, f, indent=2)
        
        # Generate README
        readme = self._generate_readme(date_str, script_data, voiceover_data, 
                                        assets_data, editor_notes)
        
        with open(handoff_dir / "README.md", 'w') as f:
            f.write(readme)
        
        # Build handoff package
        handoff_package = {
            "handoff_id": handoff_id,
            "content_id": content_id or f"drama-{date_str}-001",
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "script": script_data,
            "voiceover": voiceover_data,
            "assets": assets_data,
            "editor_notes": editor_notes,
            "quality_targets": {
                "max_dead_air_sec": 0.3,
                "retention_target_pct": 90,
                "first_frame_hook": True,
                "caption_required": True
            }
        }
        
        with open(handoff_dir / "handoff.json", 'w') as f:
            json.dump(handoff_package, f, indent=2)
        
        print(f"[Handoff] Package assembled: {handoff_dir}")
        
        # Sync to Google Drive
        self._sync_to_drive(handoff_dir, date_str)
        
        log_operation('HandoffAssembler', 'assemble', 'success', {
            'handoff_id': handoff_id,
            'has_voiceover': voiceover_data['combined_path'] is not None,
            'asset_count': len(assets_data['clips'])
        })
        
        return handoff_package
    
    def _generate_editor_notes(self, script: Dict, assets: Optional[Dict],
                                voiceover: Optional[Dict]) -> Dict:
        """Generate comprehensive editor notes."""
        lines = script.get('lines', [])
        
        notes = {
            "hook_placement": "First 3 seconds must have strongest visual. Use most eye-catching asset.",
            "mid_spike_location": "Line 5-6 (setup to payoff) - add visual energy spike",
            "loop_close_strategy": "Connect ending back to opening hook visually",
            "reference_channel": "Starless Shorts (youtube.com/@starless./shorts)",
            "special_instructions": "",
            "line_notes": []
        }
        
        # Add line-by-line notes
        for i, line in enumerate(lines, 1):
            note = {
                "line": i,
                "text_preview": line[:60] + "..." if len(line) > 60 else line,
                "timing": f"~{len(line.split()) / 2.8:.1f}s"
            }
            
            # Add asset info if available
            if assets:
                for asset in assets.get('assets', []):
                    if asset['line_number'] == i:
                        note['asset_type'] = asset.get('type', 'b-roll')
                        note['asset_status'] = 'ready' if asset.get('local_path') else 'needs_sourcing'
            
            notes['line_notes'].append(note)
        
        # Add voiceover guidance
        if voiceover and voiceover.get('combined_audio'):
            notes['special_instructions'] += "Voiceover ready. Use combined.mp3 or individual segments. "
        else:
            notes['special_instructions'] += "Voiceover pending - check RECORDING.md in audio folder. "
        
        # Add missing asset guidance
        if assets and assets.get('missing_assets'):
            notes['special_instructions'] += f"Missing assets for lines: {assets['missing_assets']}. "
        
        return notes
    
    def _generate_readme(self, date_str: str, script: Dict, voiceover: Dict,
                          assets: Dict, editor_notes: Dict) -> str:
        """Generate human-readable README for editor."""
        lines = [
            f"# Handoff Package - {date_str}",
            f"",
            f"## Quick Stats",
            f"",
            f"- **Word Count:** {script.get('word_count', 0)}",
            f"- **Target Duration:** {script.get('duration_sec', 0)}s",
            f"- **Tone:** {script.get('tone', 'serious')}",
            f"- **Voiceover:** {'✅ Ready' if voiceover.get('combined_path') else '⬜ Pending'}",
            f"- **Assets:** {len(assets.get('clips', []))} ready, {len(assets.get('missing', []))} missing",
            f"",
            f"## Script",
            f"",
        ]
        
        for i, line in enumerate(script.get('lines', []), 1):
            lines.append(f"{i}. {line}")
        
        lines.extend([
            f"",
            f"## Quality Targets",
            f"",
            f"- **Max Dead Air:** 0.3 seconds between cuts",
            f"- **Retention Target:** 90%+",
            f"- **First Frame:** Must be eye-catching hook visual",
            f"- **Captions:** Required, large, high contrast",
            f"",
            f"## Editor Notes",
            f"",
            f"**Hook Placement:** {editor_notes['hook_placement']}",
            f"",
            f"**Mid-Spike:** {editor_notes['mid_spike_location']}",
            f"",
            f"**Loop Close:** {editor_notes['loop_close_strategy']}",
            f"",
            f"**Reference:** {editor_notes['reference_channel']}",
            f"",
        ])
        
        if editor_notes.get('special_instructions'):
            lines.extend([
                f"## Special Instructions",
                f"",
                editor_notes['special_instructions'],
                f"",
            ])
        
        lines.extend([
            f"## Folder Structure",
            f"",
            f"```",
            f"script/",
            f"  ├── script.md       - Human-readable script",
            f"  └── script.json     - Structured data",
            f"audio/",
            f"  ├── combined.mp3    - Full voiceover (if ready)",
            f"  └── segments/       - Individual line audio files",
            f"assets/",
            f"  ├── manifest.json   - Asset index",
            f"  └── [asset files]   - Screenshots, clips, etc.",
            f"```",
            f"",
            f"## CapCut Checklist",
            f"",
            f"- [ ] Import voiceover to timeline",
            f"- [ ] Remove ALL dead air (>0.3s gaps)",
            f"- [ ] Add captions/subtitles (large, high contrast)",
            f"- [ ] First frame: eye-catching visual",
            f"- [ ] Visual change every 3-5 seconds",
            f"- [ ] Ken Burns effect on screenshots",
            f"- [ ] Aspect ratio: 9:16 (1080×1920)",
            f"- [ ] Export: 1080×1920, 30fps",
            f"",
            f"## Questions?",
            f"",
            f"Contact Ahawk via Telegram for clarifications.",
        ])
        
        return '\n'.join(lines)
    
    def _sync_to_drive(self, handoff_dir: Path, date_str: str):
        """Sync handoff package to Google Drive."""
        print(f"[Handoff] Syncing to Google Drive...")
        
        # Find or create Drive folder
        folder_name = f"Autonomous YouTube/DramaPipeline/{date_str}"
        
        try:
            # Check if gog is available
            result = subprocess.run(
                ["which", "gog"],
                capture_output=True, text=True
            )
            
            if result.returncode != 0:
                print(f"[Handoff] gog CLI not found, skipping Drive sync")
                return
            
            # Try to find folder
            result = subprocess.run(
                ["gog", "drive", "search", folder_name, "--account", self.gog_account],
                capture_output=True, text=True
            )
            
            print(f"[Handoff] Drive sync: folder '{folder_name}'")
            print(f"[Handoff] Note: Manual upload may be required. Package location: {handoff_dir}")
            
        except Exception as e:
            print(f"[Handoff] Drive sync error: {e}")
    
    def run(self, date_str: Optional[str] = None) -> Optional[Dict]:
        """CLI entry point."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        return self.assemble_handoff(date_str)


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='HandoffAssembler - Package Assembly')
    parser.add_argument('--date', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--breaking', help='Breaking news ID (for internal use)')
    
    args = parser.parse_args()
    
    assembler = HandoffAssembler()
    result = assembler.run(args.date)
    
    if result:
        print(f"\n{'='*50}")
        print(f"Handoff Complete")
        print(f"{'='*50}")
        print(f"ID: {result['handoff_id']}")
        print(f"Location: handoffs/{result['created_at'][:10]}/")
        print(f"README: handoffs/{result['created_at'][:10]}/README.md")
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
