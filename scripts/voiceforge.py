#!/usr/bin/env python3
"""
VoiceForge - Voiceover Generation Module
Generates sentence-by-sentence voiceover using Crayo AI
"""

import json
import os
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))
from utils import get_pipeline_dir, log_operation


class VoiceForge:
    """Voiceover generation agent using Crayo AI."""
    
    def __init__(self):
        self.pipeline_dir = get_pipeline_dir()
        self.approved_dir = self.pipeline_dir / "approved"
        self.audio_dir = self.pipeline_dir / "audio"
        self.audio_dir.mkdir(exist_ok=True)
        
        # Voice settings
        self.voice_id = "DanDan"  # Default natural-sounding voice
        self.target_pace_wpm = 185  # Words per minute
        self.target_duration_range = (40, 55)  # seconds
    
    def load_approved_script(self, date_str: str, script_id: Optional[str] = None) -> Optional[Dict]:
        """Load approved script for voice generation."""
        approved_date_dir = self.approved_dir / date_str
        
        if not approved_date_dir.exists():
            print(f"[VoiceForge] No approved scripts for {date_str}")
            return None
        
        # Find script - check subdirs first, then root
        if script_id:
            script_file = approved_date_dir / script_id / "script.json"
            if script_file.exists():
                with open(script_file) as f:
                    return json.load(f)
        
        # Check for script.json directly in date dir
        script_file = approved_date_dir / "script.json"
        if script_file.exists():
            with open(script_file) as f:
                return json.load(f)
        
        # Find first approved script in subdirs
        for subdir in approved_date_dir.iterdir():
            if subdir.is_dir():
                script_file = subdir / "script.json"
                if script_file.exists():
                    with open(script_file) as f:
                        return json.load(f)
        
        return None
    
    def generate_line_audio(self, line_text: str, line_num: int, output_dir: Path) -> Optional[Path]:
        """Generate audio for a single line using Crayo AI.
        
        Note: Crayo AI has no public API. This uses browser automation
        via Playwright or similar. For now, we create a placeholder
        that documents what needs to be recorded.
        """
        # Create segment directory
        segments_dir = output_dir / "segments"
        segments_dir.mkdir(exist_ok=True)
        
        output_file = segments_dir / f"line-{line_num}.mp3"
        
        # Placeholder: In production, this would:
        # 1. Open Crayo AI in browser
        # 2. Input the line text
        # 3. Select voice (DanDan)
        # 4. Generate and download
        # 5. Save to output_file
        
        # For now, create a metadata file indicating what needs recording
        metadata = {
            "line_number": line_num,
            "text": line_text,
            "voice": self.voice_id,
            "status": "pending",
            "target_duration_sec": len(line_text.split()) / (self.target_pace_wpm / 60),
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        
        meta_file = segments_dir / f"line-{line_num}.json"
        with open(meta_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"[VoiceForge] Line {line_num} metadata created: {line_text[:50]}...")
        return output_file
    
    def combine_audio_segments(self, segments_dir: Path, output_file: Path) -> bool:
        """Combine individual line audio files into full voiceover.
        
        Uses ffmpeg to concatenate with minimal gaps.
        """
        # Find all segment files
        segment_files = sorted(segments_dir.glob("line-*.mp3"))
        
        if len(segment_files) != 8:
            print(f"[VoiceForge] Warning: Expected 8 segments, found {len(segment_files)}")
            return False
        
        # Create ffmpeg concat file list
        concat_file = segments_dir / "concat.txt"
        with open(concat_file, 'w') as f:
            for seg in segment_files:
                f.write(f"file '{seg.absolute()}'\n")
        
        # Run ffmpeg
        try:
            cmd = [
                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", str(concat_file),
                "-acodec", "libmp3lame", "-q:a", "2",
                str(output_file)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                print(f"[VoiceForge] Combined audio saved: {output_file}")
                return True
            else:
                print(f"[VoiceForge] ffmpeg error: {result.stderr}")
                return False
                
        except FileNotFoundError:
            print("[VoiceForge] ffmpeg not found. Install with: brew install ffmpeg")
            return False
        except Exception as e:
            print(f"[VoiceForge] Error combining audio: {e}")
            return False
    
    def validate_voiceover(self, output_dir: Path, script: Dict) -> Dict:
        """Validate generated voiceover meets quality gates."""
        segments_dir = output_dir / "segments"
        combined_file = output_dir / "combined.mp3"
        
        validation = {
            "segment_count": len(list(segments_dir.glob("line-*.mp3"))),
            "combined_exists": combined_file.exists(),
            "duration_sec": None,
            "pace_wpm": None,
            "passed": False,
            "issues": []
        }
        
        # Check segment count
        if validation["segment_count"] != 8:
            validation["issues"].append(f"Expected 8 segments, got {validation['segment_count']}")
        
        # Get duration if combined file exists
        if combined_file.exists():
            try:
                cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                       "-of", "default=noprint_wrappers=1:nokey=1", str(combined_file)]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                duration = float(result.stdout.strip())
                validation["duration_sec"] = round(duration, 1)
                
                # Calculate pace
                word_count = script.get('script', {}).get('word_count', 0)
                if word_count > 0:
                    validation["pace_wpm"] = round((word_count / duration) * 60, 1)
                
                # Check duration range
                min_dur, max_dur = self.target_duration_range
                if not (min_dur <= duration <= max_dur):
                    validation["issues"].append(f"Duration {duration}s outside range {min_dur}-{max_dur}s")
                
                # Check pace
                if validation["pace_wpm"]:
                    if not (170 <= validation["pace_wpm"] <= 200):
                        validation["issues"].append(f"Pace {validation['pace_wpm']} WPM outside 170-200 range")
                
            except Exception as e:
                validation["issues"].append(f"Could not analyze audio: {e}")
        else:
            validation["issues"].append("Combined audio file not found")
        
        validation["passed"] = len(validation["issues"]) == 0
        return validation
    
    def generate_voiceover(self, date_str: str, script_id: Optional[str] = None) -> Optional[Dict]:
        """Generate complete voiceover for approved script."""
        print(f"[VoiceForge] Starting voiceover generation for {date_str}...")
        
        # Load approved script
        approved = self.load_approved_script(date_str, script_id)
        if not approved:
            print(f"[VoiceForge] No approved script found")
            return None
        
        script = approved.get('script', {})
        lines = script.get('lines', [])
        
        if len(lines) != 8:
            print(f"[VoiceForge] Error: Script has {len(lines)} lines, expected 8")
            return None
        
        # Create output directory
        output_dir = self.audio_dir / date_str
        output_dir.mkdir(exist_ok=True)
        
        # Generate audio for each line
        print(f"[VoiceForge] Generating {len(lines)} audio segments...")
        for i, line in enumerate(lines, 1):
            self.generate_line_audio(line, i, output_dir)
        
        # Create recording instructions
        self._create_recording_instructions(output_dir, script)
        
        # Try to combine (will fail if no actual audio files, that's ok for now)
        combined_file = output_dir / "combined.mp3"
        if self.combine_audio_segments(output_dir / "segments", combined_file):
            validation = self.validate_voiceover(output_dir, approved)
        else:
            validation = {
                "status": "pending_manual_recording",
                "segments_created": len(lines),
                "message": "Audio files need to be recorded via Crayo AI"
            }
        
        # Save metadata
        voiceover_data = {
            "script_id": script.get('seed_id', 'unknown'),
            "voice_id": self.voice_id,
            "tone_applied": script.get('tone', 'serious'),
            "lines": lines,
            "output_dir": str(output_dir),
            "combined_audio": str(combined_file) if combined_file.exists() else None,
            "segment_paths": [str(output_dir / "segments" / f"line-{i}.mp3") for i in range(1, 9)],
            "validation": validation,
            "created_at": datetime.utcnow().isoformat() + "Z"
        }
        
        metadata_file = output_dir / "voiceover.json"
        with open(metadata_file, 'w') as f:
            json.dump(voiceover_data, f, indent=2)
        
        print(f"[VoiceForge] Voiceover metadata saved: {metadata_file}")
        
        log_operation('VoiceForge', 'generate', 'success' if validation.get('passed') else 'pending', {
            'script_id': script.get('seed_id'),
            'segments': len(lines),
            'validation': validation
        })
        
        return voiceover_data
    
    def _create_recording_instructions(self, output_dir: Path, script: Dict):
        """Create human-readable recording instructions."""
        lines = script.get('lines', [])
        
        instructions = [
            f"# Voiceover Recording Instructions",
            f"",
            f"**Voice:** {self.voice_id}",
            f"**Tone:** {script.get('tone', 'serious')}",
            f"**Target Pace:** {self.target_pace_wpm} WPM",
            f"",
            f"## Recording Steps (Crayo AI)",
            f"",
            f"1. Go to https://crayo.ai",
            f"2. Select voice: **{self.voice_id}**",
            f"3. For each line below, copy text → generate → export",
            f"4. Save each line as: line-1.mp3, line-2.mp3, etc.",
            f"5. Place files in: `{output_dir}/segments/`",
            f"6. Run: `python scripts/voiceforge.py --combine {output_dir}`",
            f"",
            f"## Lines to Record",
            f""
        ]
        
        for i, line in enumerate(lines, 1):
            word_count = len(line.split())
            est_duration = round(word_count / (self.target_pace_wpm / 60), 1)
            instructions.append(f"### Line {i} (~{est_duration}s)")
            instructions.append(f"```")
            instructions.append(line)
            instructions.append(f"```")
            instructions.append(f"")
        
        instructions.extend([
            f"",
            f"## Quality Checks",
            f"- [ ] Total duration: 40-55 seconds",
            f"- [ ] No audio artifacts or distortion",
            f"- [ ] Celebrity names pronounced correctly",
            f"- [ ] Pace: 170-200 WPM",
            f"- [ ] Sentence gaps: < 0.3 seconds (Lemuel will remove dead air)",
            f"",
            f"## Full Script",
            f"",
            f"```"
        ])
        instructions.extend(lines)
        instructions.append("```")
        
        instructions_file = output_dir / "RECORDING.md"
        instructions_file.write_text('\n'.join(instructions))
        
        print(f"[VoiceForge] Recording instructions: {instructions_file}")
    
    def run(self, date_str: Optional[str] = None, script_id: Optional[str] = None) -> Optional[Dict]:
        """CLI entry point."""
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        return self.generate_voiceover(date_str, script_id)


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='VoiceForge - Voiceover Generation')
    parser.add_argument('--date', help='Date to process (YYYY-MM-DD)')
    parser.add_argument('--script-id', help='Specific script ID to process')
    parser.add_argument('--combine', help='Combine existing segments in directory')
    
    args = parser.parse_args()
    
    forge = VoiceForge()
    
    if args.combine:
        # Just combine existing segments
        output_dir = Path(args.combine)
        combined_file = output_dir / "combined.mp3"
        success = forge.combine_audio_segments(output_dir / "segments", combined_file)
        sys.exit(0 if success else 1)
    
    result = forge.run(args.date, args.script_id)
    
    if result:
        print(f"\n{'='*50}")
        print(f"VoiceForge Complete")
        print(f"{'='*50}")
        print(f"Segments: {len(result['lines'])}")
        print(f"Status: {result['validation'].get('status', 'unknown')}")
        if result['combined_audio']:
            print(f"Combined: {result['combined_audio']}")
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
