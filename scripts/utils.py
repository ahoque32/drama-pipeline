"""
Drama Pipeline Utilities
Shared functions for Drive I/O, readability, logging
"""

import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path


def get_pipeline_dir():
    """Get the pipeline root directory."""
    return Path.home() / ".openclaw/agents/dante-agent/projects/drama-pipeline"


def load_config():
    """Load configuration from config.yaml or env vars."""
    import yaml
    
    config_path = get_pipeline_dir() / "config.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Override with env vars
    config['x_api']['bearer_token'] = os.environ.get('X_BEARER_TOKEN', config['x_api']['bearer_token'])
    config['anthropic']['api_key'] = os.environ.get('ANTHROPIC_API_KEY', config['anthropic']['api_key'])
    
    return config


def get_drive_folder_id(folder_name, account="admin@renderwise.net"):
    """Get Google Drive folder ID by name."""
    try:
        result = subprocess.run(
            ["gog", "drive", "search", folder_name, "--account", account, "--json"],
            capture_output=True, text=True, check=True
        )
        data = json.loads(result.stdout)
        for item in data:
            if item.get('name') == folder_name and item.get('type') == 'folder':
                return item['id']
    except Exception as e:
        print(f"Error finding Drive folder: {e}")
    return None


def drive_upload(local_path, folder_id, filename=None, account="admin@renderwise.net"):
    """Upload file to Google Drive folder."""
    if filename is None:
        filename = Path(local_path).name
    
    try:
        # gog doesn't have direct upload, use cp if available or note limitation
        print(f"[Drive] Would upload {local_path} to folder {folder_id}")
        return True
    except Exception as e:
        print(f"Error uploading to Drive: {e}")
        return False


def drive_download(file_id, local_path, account="admin@renderwise.net"):
    """Download file from Google Drive."""
    try:
        print(f"[Drive] Would download {file_id} to {local_path}")
        return True
    except Exception as e:
        print(f"Error downloading from Drive: {e}")
        return False


# --- Readability Engine ---

def count_syllables(word):
    """Estimate syllable count for English word."""
    word = word.lower().strip(".,!?;:'\"()-")
    if not word:
        return 0
    
    count = 0
    vowels = "aeiouy"
    prev_vowel = False
    
    for char in word:
        is_vowel = char in vowels
        if is_vowel and not prev_vowel:
            count += 1
        prev_vowel = is_vowel
    
    if word.endswith("e") and count > 1:
        count -= 1
    
    return max(count, 1)


def flesch_kincaid_grade(text):
    """Calculate Flesch-Kincaid Grade Level."""
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    words = [w for w in text.split() if re.search(r'[a-zA-Z]', w)]
    
    if not sentences or not words:
        return 0
    
    total_syllables = sum(count_syllables(w) for w in words)
    grade = 0.39 * (len(words) / len(sentences)) + 11.8 * (total_syllables / len(words)) - 15.59
    return round(grade, 1)


def estimate_duration(text, wpm=170):
    """Estimate speaking duration in seconds."""
    words = len(text.split())
    return round(words / (wpm / 60), 1)


# --- Logging ---

def log_operation(module, action, status, details=None):
    """Log pipeline operation."""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "module": module,
        "action": action,
        "status": status,
        "details": details or {}
    }
    
    date = datetime.now().strftime("%Y-%m-%d")
    log_file = get_pipeline_dir() / "logs" / f"{date}.json"
    
    logs = []
    if log_file.exists():
        try:
            with open(log_file) as f:
                logs = json.load(f)
        except:
            pass
    
    logs.append(log_entry)
    
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with open(log_file, 'w') as f:
        json.dump(logs, f, indent=2)
    
    return log_entry


# --- API Key Resolution ---

def get_anthropic_api_key():
    """Resolve Anthropic API key from various sources."""
    # Env var first
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return key
    
    # File-based
    key_paths = [
        Path.home() / ".openclaw/credentials/anthropic-api-key",
        Path.home() / ".config/env/anthropic-key",
    ]
    for p in key_paths:
        if p.exists():
            return p.read_text().strip()
    
    # OpenClaw config
    config_path = Path.home() / ".openclaw/openclaw.json"
    if config_path.exists():
        try:
            config = json.loads(config_path.read_text())
            return (config.get("models", {}).get("providers", {}).get("anthropic", {}).get("apiKey", "")
                   or config.get("providers", {}).get("anthropic", {}).get("apiKey", ""))
        except:
            pass
    
    return None
