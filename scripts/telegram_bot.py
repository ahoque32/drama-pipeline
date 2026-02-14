#!/usr/bin/env python3
"""
TelegramBot - Inline button handler for script approvals
✅ Approve, ✏️ Edit, ❌ Kill, 🔄 Rewrite with callback handling
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
from utils import get_pipeline_dir, load_config, log_operation


class TelegramBot:
    """Handle Telegram inline buttons and callbacks."""
    
    def __init__(self):
        self.config = load_config()
        self.pipeline_dir = get_pipeline_dir()
        self.drafts_dir = self.pipeline_dir / "drafts"
        self.approved_dir = self.pipeline_dir / "approved"
        self.state_dir = self.pipeline_dir / "state"
        self.state_dir.mkdir(exist_ok=True)
        
        # Telegram config
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        if not self.telegram_token:
            print("[TelegramBot] Warning: TELEGRAM_BOT_TOKEN not set")
    
    def get_state_file(self) -> Path:
        """Get state file for pending approvals."""
        return self.state_dir / "pending_approvals.json"
    
    def load_pending_approvals(self) -> Dict:
        """Load pending approvals state."""
        state_file = self.get_state_file()
        
        if state_file.exists():
            with open(state_file) as f:
                return json.load(f)
        
        return {
            "pending": {},  # message_id -> script_info
            "history": []   # approval history
        }
    
    def save_pending_approvals(self, state: Dict):
        """Save pending approvals state."""
        state_file = self.get_state_file()
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    def register_script(self, message_id: str, script: Dict, date_str: str, index: int):
        """Register a script for approval tracking."""
        state = self.load_pending_approvals()
        
        state["pending"][str(message_id)] = {
            "script": script,
            "date": date_str,
            "index": index,
            "registered_at": datetime.utcnow().isoformat() + "Z",
            "status": "pending"
        }
        
        self.save_pending_approvals(state)
        print(f"[TelegramBot] Registered script {index} for approval (msg: {message_id})")
    
    def build_inline_keyboard(self, script_index: int) -> Dict:
        """Build inline keyboard with approval buttons."""
        return {
            "inline_keyboard": [
                [
                    {"text": "✅ Approve", "callback_data": f"approve:{script_index}"},
                    {"text": "❌ Kill", "callback_data": f"kill:{script_index}"}
                ],
                [
                    {"text": "✏️ Edit", "callback_data": f"edit:{script_index}"},
                    {"text": "🔄 Rewrite", "callback_data": f"rewrite:{script_index}"}
                ]
            ]
        }
    
    def send_script_with_buttons(self, script: Dict, date_str: str, index: int) -> Optional[str]:
        """Send script to Telegram with inline approval buttons."""
        if not self.telegram_token or not self.telegram_chat_id:
            print("[TelegramBot] Telegram not configured")
            return None
        
        # Format message
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
        
        message = '\n'.join(lines)
        
        # Build keyboard
        keyboard = self.build_inline_keyboard(index)
        
        # Send message
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        data = {
            "chat_id": self.telegram_chat_id,
            "text": message[:4000],
            "parse_mode": "HTML",
            "reply_markup": keyboard
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
                    message_id = str(result['result']['message_id'])
                    # Register for tracking
                    self.register_script(message_id, script, date_str, index)
                    print(f"[TelegramBot] Script {index} sent with buttons (msg: {message_id})")
                    return message_id
                else:
                    print(f"[TelegramBot] Send error: {result}")
                    return None
        except Exception as e:
            print(f"[TelegramBot] Send error: {e}")
            return None
    
    def handle_callback(self, callback_data: str, message_id: str) -> Dict:
        """Handle inline button callback."""
        print(f"[TelegramBot] Handling callback: {callback_data} for msg {message_id}")
        
        # Parse callback data
        parts = callback_data.split(":")
        if len(parts) != 2:
            return {"status": "error", "message": "Invalid callback format"}
        
        action, script_index = parts[0], int(parts[1])
        
        # Load pending state
        state = self.load_pending_approvals()
        pending = state["pending"].get(message_id)
        
        if not pending:
            return {"status": "error", "message": "Script not found or already processed"}
        
        script = pending["script"]
        date_str = pending["date"]
        
        result = {
            "action": action,
            "script_index": script_index,
            "message_id": message_id,
            "status": "unknown"
        }
        
        if action == "approve":
            result = self._handle_approve(script, date_str, message_id, state)
        elif action == "kill":
            result = self._handle_kill(script, message_id, state)
        elif action == "edit":
            result = self._handle_edit(script, message_id, state)
        elif action == "rewrite":
            result = self._handle_rewrite(script, date_str, message_id, state)
        
        return result
    
    def _handle_approve(self, script: Dict, date_str: str, message_id: str, state: Dict) -> Dict:
        """Handle ✅ Approve action."""
        print(f"[TelegramBot] Approving script: {script['headline'][:40]}...")
        
        # Update state
        state["pending"][message_id]["status"] = "approved"
        state["history"].append({
            "action": "approve",
            "script_id": script.get('seed_id'),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
        self.save_pending_approvals(state)
        
        # Trigger approval in DramaMaestro
        try:
            result = subprocess.run(
                [sys.executable, str(self.pipeline_dir / "scripts" / "drama_maestro.py"),
                 "--approve-script", script.get('seed_id', ''), "--date", date_str],
                capture_output=True, text=True, timeout=300
            )
            
            success = result.returncode == 0
            
            return {
                "action": "approve",
                "status": "success" if success else "failed",
                "script_id": script.get('seed_id'),
                "message": "Script approved and pipeline triggered" if success else "Approval failed"
            }
        except Exception as e:
            return {
                "action": "approve",
                "status": "error",
                "message": str(e)
            }
    
    def _handle_kill(self, script: Dict, message_id: str, state: Dict) -> Dict:
        """Handle ❌ Kill action."""
        print(f"[TelegramBot] Killing script: {script['headline'][:40]}...")
        
        # Update state
        state["pending"][message_id]["status"] = "killed"
        state["history"].append({
            "action": "kill",
            "script_id": script.get('seed_id'),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
        self.save_pending_approvals(state)
        
        return {
            "action": "kill",
            "status": "success",
            "script_id": script.get('seed_id'),
            "message": "Script rejected"
        }
    
    def _handle_edit(self, script: Dict, message_id: str, state: Dict) -> Dict:
        """Handle ✏️ Edit action."""
        print(f"[TelegramBot] Edit requested for script: {script['headline'][:40]}...")
        
        # Update state to mark as awaiting edit
        state["pending"][message_id]["status"] = "awaiting_edit"
        state["pending"][message_id]["edit_requested_at"] = datetime.utcnow().isoformat() + "Z"
        self.save_pending_approvals(state)
        
        # Send edit instructions
        self._send_edit_instructions(message_id, script)
        
        return {
            "action": "edit",
            "status": "awaiting_input",
            "script_id": script.get('seed_id'),
            "message": "Edit mode activated - reply with edit instructions"
        }
    
    def _send_edit_instructions(self, message_id: str, script: Dict):
        """Send edit instructions to user."""
        if not self.telegram_token or not self.telegram_chat_id:
            return
        
        lines = [
            "✏️ <b>EDIT MODE</b>",
            "",
            "Reply with your edits in this format:",
            "",
            "<code>edit: Line 3 - Make it punchier",
            "edit: Line 6 - Add more shock value",
            "tone: more ironic</code>",
            "",
            "Or reply with the full rewritten script."
        ]
        
        message = '\n'.join(lines)
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        
        data = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "HTML",
            "reply_to_message_id": int(message_id)
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
                    print(f"[TelegramBot] Edit instructions sent")
        except Exception as e:
            print(f"[TelegramBot] Edit instructions error: {e}")
    
    def _handle_rewrite(self, script: Dict, date_str: str, message_id: str, state: Dict) -> Dict:
        """Handle 🔄 Rewrite action."""
        print(f"[TelegramBot] Rewrite requested for script: {script['headline'][:40]}...")
        
        # Update state
        state["pending"][message_id]["status"] = "rewriting"
        state["history"].append({
            "action": "rewrite",
            "script_id": script.get('seed_id'),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        })
        self.save_pending_approvals(state)
        
        # Trigger rewrite via ScriptSmith
        try:
            # This would call a rewrite function in ScriptSmith
            # For now, just mark it for manual rewrite
            return {
                "action": "rewrite",
                "status": "pending",
                "script_id": script.get('seed_id'),
                "message": "Rewrite queued - run: python scripts/scriptsmith.py --rewrite-seed " + script.get('seed_id', '')
            }
        except Exception as e:
            return {
                "action": "rewrite",
                "status": "error",
                "message": str(e)
            }
    
    def answer_callback(self, callback_query_id: str, text: Optional[str] = None):
        """Answer callback query to remove loading state."""
        if not self.telegram_token:
            return
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/answerCallbackQuery"
        
        data = {
            "callback_query_id": callback_query_id
        }
        
        if text:
            data["text"] = text
        
        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers={"Content-Type": "application/json"},
                method='POST'
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))
                return result.get('ok', False)
        except Exception as e:
            print(f"[TelegramBot] Answer callback error: {e}")
            return False
    
    def update_message(self, message_id: str, new_text: str, parse_mode: str = "HTML"):
        """Update message text after action."""
        if not self.telegram_token or not self.telegram_chat_id:
            return
        
        url = f"https://api.telegram.org/bot{self.telegram_token}/editMessageText"
        
        data = {
            "chat_id": self.telegram_chat_id,
            "message_id": int(message_id),
            "text": new_text[:4000],
            "parse_mode": parse_mode
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
                return result.get('ok', False)
        except Exception as e:
            print(f"[TelegramBot] Update message error: {e}")
            return False
    
    def get_pending_count(self) -> int:
        """Get count of pending approvals."""
        state = self.load_pending_approvals()
        return len([s for s in state["pending"].values() if s["status"] == "pending"])
    
    def list_pending(self) -> List[Dict]:
        """List all pending approvals."""
        state = self.load_pending_approvals()
        return [
            {"message_id": mid, **info}
            for mid, info in state["pending"].items()
            if info["status"] == "pending"
        ]


def main():
    """CLI entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='TelegramBot - Inline button handler')
    parser.add_argument('--send-scripts', action='store_true', help='Send pending scripts with buttons')
    parser.add_argument('--date', help='Date for scripts (YYYY-MM-DD)')
    parser.add_argument('--handle-callback', help='Handle callback data (format: action:index:message_id)')
    parser.add_argument('--callback-query-id', help='Callback query ID to answer')
    parser.add_argument('--pending-count', action='store_true', help='Show pending approval count')
    parser.add_argument('--list-pending', action='store_true', help='List pending approvals')
    
    args = parser.parse_args()
    
    bot = TelegramBot()
    
    if args.pending_count:
        count = bot.get_pending_count()
        print(f"Pending approvals: {count}")
        return 0
    
    if args.list_pending:
        pending = bot.list_pending()
        print(f"Pending approvals ({len(pending)}):")
        for p in pending:
            print(f"  - {p['message_id']}: Script {p['index']} ({p['script']['headline'][:40]}...)")
        return 0
    
    if args.handle_callback:
        # Parse callback data: action:index:message_id
        parts = args.handle_callback.split(":")
        if len(parts) != 3:
            print("Error: callback format should be action:index:message_id")
            return 1
        
        action, index, message_id = parts[0], int(parts[1]), parts[2]
        callback_data = f"{action}:{index}"
        
        result = bot.handle_callback(callback_data, message_id)
        print(json.dumps(result, indent=2))
        
        # Answer callback if ID provided
        if args.callback_query_id:
            bot.answer_callback(args.callback_query_id, result.get("message"))
        
        return 0 if result["status"] in ["success", "pending", "awaiting_input"] else 1
    
    if args.send_scripts:
        date_str = args.date or datetime.now().strftime("%Y-%m-%d")
        
        # Load passing scripts
        drafts_file = bot.drafts_dir / f"{date_str}.json"
        if not drafts_file.exists():
            print(f"No drafts found for {date_str}")
            return 1
        
        with open(drafts_file) as f:
            data = json.load(f)
        
        scripts = [s for s in data.get('scripts', []) if s.get('quality_passed')]
        scripts.sort(key=lambda x: x.get('hook_strength', 0), reverse=True)
        
        # Send top 5 with buttons
        for i, script in enumerate(scripts[:5], 1):
            msg_id = bot.send_script_with_buttons(script, date_str, i)
            if not msg_id:
                print(f"Failed to send script {i}")
        
        return 0
    
    print("Use --send-scripts, --handle-callback, --pending-count, or --list-pending")
    return 0


if __name__ == "__main__":
    sys.exit(main())
