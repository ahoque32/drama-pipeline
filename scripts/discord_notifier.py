#!/usr/bin/env python3
"""
Discord Notifier for Drama Pipeline
Posts stage updates, alerts, approval requests, and daily summaries to Discord channels.
Uses Discord webhooks for simplicity and reliability.
"""

import os
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum

import requests


# ============================================================================
# Configuration
# ============================================================================

# Channel IDs
CHANNELS = {
    "dp-00-control": "1477905647122251898",
    "dp-01-scout": "1477905648216969277",
    "dp-02-script": "1477905649089380485",
    "dp-03-assets": "1477905650603524096",
    "dp-04-voice": "1477905652360675361",
    "dp-05-assemble": "1477905653128237076",
    "dp-06-approve": "1477905653782806662",
    "dp-90-runs": "1477905654160162938",
    "dp-91-alerts": "1477905655523315795",
    "dp-92-health": "1477905656907305072",
    "dp-93-daily-summary": "1477905657968463994",
    "dp-94-audit-log": "1477905659222691840",
}

GUILD_ID = "1477891199938461799"

# Stage to channel mapping
STAGE_CHANNELS = {
    "control": "dp-00-control",
    "scout": "dp-01-scout",
    "script": "dp-02-script",
    "assets": "dp-03-assets",
    "voice": "dp-04-voice",
    "assemble": "dp-05-assemble",
    "approve": "dp-06-approve",
}


class Status(Enum):
    STARTED = "started"
    PROGRESS = "progress"
    DONE = "done"
    FAILED = "failed"
    RETRYING = "retrying"


class Priority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class StageUpdate:
    """Stage update message data."""
    run_id: str
    stage: str
    status: Status
    title: str = ""
    source: str = ""
    priority: Priority = Priority.MEDIUM
    artifacts: List[str] = field(default_factory=list)
    next_stage: str = ""
    owner: str = ""
    duration_seconds: float = 0.0


@dataclass
class Alert:
    """Alert message data."""
    run_id: str
    stage: str
    title: str
    message: str
    exception: Optional[str] = None
    traceback: Optional[str] = None
    retry_count: int = 0
    priority: Priority = Priority.HIGH


@dataclass
class ApprovalRequest:
    """Approval request data."""
    run_id: str
    stage: str
    title: str
    description: str
    artifacts: List[str] = field(default_factory=list)
    approver: str = ""
    due_by: str = ""


@dataclass
class DailySummary:
    """Daily summary data."""
    date: str
    runs_total: int
    runs_success: int
    runs_failed: int
    runs_in_progress: int
    top_dramas: List[Dict] = field(default_factory=list)
    alerts_triggered: int = 0


# ============================================================================
# Discord Webhook Client
# ============================================================================

class DiscordNotifier:
    """Discord webhook notifier for Drama Pipeline."""
    
    def __init__(self, webhook_url: str = None):
        """
        Initialize the notifier.
        
        Args:
            webhook_url: Discord webhook URL. Falls back to env var DISCORD_WEBHOOK_URL.
        """
        self.webhook_url = webhook_url or os.environ.get('DISCORD_WEBHOOK_URL')
        self.bot_token = os.environ.get('DISCORD_BOT_TOKEN')
        self.guild_id = GUILD_ID
        
    def _get_channel_webhook(self, channel_id: str) -> Optional[str]:
        """Get or create webhook for a channel."""
        if self.webhook_url:
            return self.webhook_url
        
        if not self.bot_token:
            print("[DiscordNotifier] Warning: No DISCORD_WEBHOOK_URL or DISCORD_BOT_TOKEN set")
            return None
        
        # Try to get existing webhook for channel
        try:
            url = f"https://discord.com/api/v10/channels/{channel_id}/webhooks"
            headers = {"Authorization": f"Bot {self.bot_token}"}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                webhooks = resp.json()
                # Return first webhook or None
                if webhooks:
                    return webhooks[0]['url']
        except Exception as e:
            print(f"[DiscordNotifier] Error getting webhook: {e}")
        
        return None
    
    def _post(self, channel_key: str, payload: Dict[str, Any]) -> bool:
        """Post a message to a Discord channel via bot API."""
        channel_id = CHANNELS.get(channel_key)
        if not channel_id:
            print(f"[DiscordNotifier] Unknown channel: {channel_key}")
            return False
        
        if not self.bot_token:
            print(f"[DiscordNotifier] No DISCORD_BOT_TOKEN set")
            return False
        
        # Convert webhook payload to message create payload
        msg_payload = {}
        if 'embeds' in payload:
            msg_payload['embeds'] = payload['embeds']
        if 'content' in payload:
            msg_payload['content'] = payload['content']
        if not msg_payload:
            msg_payload['content'] = str(payload)
        
        url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
        headers = {
            "Authorization": f"Bot {self.bot_token}",
            "Content-Type": "application/json"
        }
        
        try:
            resp = requests.post(url, json=msg_payload, headers=headers, timeout=15)
            if resp.status_code in (200, 201):
                return True
            else:
                print(f"[DiscordNotifier] Failed to post to {channel_key}: {resp.status_code} - {resp.text[:200]}")
                return False
        except Exception as e:
            print(f"[DiscordNotifier] Error posting: {e}")
            return False
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human-readable form."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    # =========================================================================
    # Public API
    # =========================================================================
    
    def post_stage_update(self, update: StageUpdate) -> bool:
        """
        Post a stage update message.
        
        Format:
        [RUN:<run_id>] [STAGE:<stage>] [STATUS:<status>]
        """
        status_emoji = {
            Status.STARTED: "🆕",
            Status.PROGRESS: "🔄",
            Status.DONE: "✅",
            Status.FAILED: "❌",
            Status.RETRYING: "🔁",
        }
        
        priority_emoji = {
            Priority.LOW: "🟢",
            Priority.MEDIUM: "🟡",
            Priority.HIGH: "🟠",
            Priority.CRITICAL: "🔴",
        }
        
        # Build the header line
        header = f"[RUN:{update.run_id}] [STAGE:{update.stage}] [STATUS:{update.status.value}]"
        
        # Build embed
        embed = {
            "title": f"{status_emoji.get(update.status, '')} {update.title or f'Stage {update.stage}'}",
            "color": self._get_status_color(update.status),
            "fields": []
        }
        
        # Add source if provided
        if update.source:
            embed["fields"].append({
                "name": "Source",
                "value": update.source,
                "inline": True
            })
        
        # Add priority
        embed["fields"].append({
            "name": "Priority",
            "value": f"{priority_emoji.get(update.priority, '')} {update.priority.value}",
            "inline": True
        })
        
        # Add duration if provided
        if update.duration_seconds > 0:
            embed["fields"].append({
                "name": "Duration",
                "value": self._format_duration(update.duration_seconds),
                "inline": True
            })
        
        # Add owner if provided
        if update.owner:
            embed["fields"].append({
                "name": "Owner",
                "value": update.owner,
                "inline": True
            })
        
        # Add artifacts if provided
        if update.artifacts:
            artifacts_text = "\n".join([f"- {a}" for a in update.artifacts[:5]])
            if len(update.artifacts) > 5:
                artifacts_text += f"\n- ... and {len(update.artifacts) - 5} more"
            embed["fields"].append({
                "name": "Artifacts",
                "value": artifacts_text,
                "inline": False
            })
        
        # Add next stage if provided
        if update.next_stage:
            embed["fields"].append({
                "name": "Next",
                "value": update.next_stage,
                "inline": True
            })
        
        # Determine channel
        channel_key = STAGE_CHANNELS.get(update.stage, "dp-90-runs")
        
        payload = {
            "content": header,
            "embeds": [embed]
        }
        
        return self._post(channel_key, payload)
    
    def post_alert(self, alert: Alert) -> bool:
        """Post an alert message (for errors and warnings)."""
        priority_colors = {
            Priority.LOW: 0x22C55E,      # Green
            Priority.MEDIUM: 0xEAB308,   # Yellow
            Priority.HIGH: 0xF97316,    # Orange
            Priority.CRITICAL: 0xEF4444, # Red
        }
        
        embed = {
            "title": f"🚨 {alert.title}",
            "color": priority_colors.get(alert.priority, 0xF97316),
            "fields": [
                {
                    "name": "Run",
                    "value": alert.run_id,
                    "inline": True
                },
                {
                    "name": "Stage",
                    "value": alert.stage,
                    "inline": True
                }
            ]
        }
        
        # Add error message
        if alert.message:
            embed["fields"].append({
                "name": "Message",
                "value": alert.message[:500],
                "inline": False
            })
        
        # Add exception details
        if alert.exception:
            embed["fields"].append({
                "name": "Exception",
                "value": f"```{alert.exception[:500]}```",
                "inline": False
            })
        
        # Add traceback
        if alert.traceback:
            embed["fields"].append({
                "name": "Traceback",
                "value": f"```{alert.traceback[-1000:]}```",
                "inline": False
            })
        
        # Add retry count
        if alert.retry_count > 0:
            embed["fields"].append({
                "name": "Retries",
                "value": str(alert.retry_count),
                "inline": True
            })
        
        payload = {
            "content": "@everyone" if alert.priority == Priority.CRITICAL else "",
            "embeds": [embed]
        }
        
        return self._post("dp-91-alerts", payload)
    
    def post_approval_request(self, req: ApprovalRequest) -> bool:
        """Post an approval request."""
        embed = {
            "title": f"📋 Approval Request: {req.title}",
            "color": 0x8B5CF6,  # Purple
            "fields": [
                {
                    "name": "Run",
                    "value": req.run_id,
                    "inline": True
                },
                {
                    "name": "Stage",
                    "value": req.stage,
                    "inline": True
                }
            ]
        }
        
        if req.description:
            embed["fields"].append({
                "name": "Description",
                "value": req.description[:500],
                "inline": False
            })
        
        if req.artifacts:
            artifacts_text = "\n".join([f"- {a}" for a in req.artifacts[:5]])
            embed["fields"].append({
                "name": "Artifacts to Review",
                "value": artifacts_text,
                "inline": False
            })
        
        if req.approver:
            embed["fields"].append({
                "name": "Requested Approver",
                "value": req.approver,
                "inline": True
            })
        
        if req.due_by:
            embed["fields"].append({
                "name": "Due By",
                "value": req.due_by,
                "inline": True
            })
        
        payload = {
            "content": "📋 Approval needed!",
            "embeds": [embed]
        }
        
        return self._post("dp-06-approve", payload)
    
    def post_daily_summary(self, summary: DailySummary) -> bool:
        """Post a daily summary."""
        success_rate = (summary.runs_success / summary.runs_total * 100) if summary.runs_total > 0 else 0
        
        embed = {
            "title": f"📊 Daily Summary - {summary.date}",
            "color": 0x3B82F6,  # Blue
            "fields": [
                {
                    "name": "Total Runs",
                    "value": str(summary.runs_total),
                    "inline": True
                },
                {
                    "name": "✅ Success",
                    "value": str(summary.runs_success),
                    "inline": True
                },
                {
                    "name": "❌ Failed",
                    "value": str(summary.runs_failed),
                    "inline": True
                },
                {
                    "name": "🔄 In Progress",
                    "value": str(summary.runs_in_progress),
                    "inline": True
                },
                {
                    "name": "Success Rate",
                    "value": f"{success_rate:.1f}%",
                    "inline": True
                },
                {
                    "name": "🚨 Alerts",
                    "value": str(summary.alerts_triggered),
                    "inline": True
                }
            ]
        }
        
        if summary.top_dramas:
            top_text = "\n".join([
                f"{i+1}. **{d.get('title', 'Unknown')}** (score: {d.get('score', 0)})"
                for i, d in enumerate(summary.top_dramas[:5])
            ])
            embed["fields"].append({
                "name": "Top Dramas",
                "value": top_text,
                "inline": False
            })
        
        payload = {
            "content": "📊 Daily Pipeline Summary",
            "embeds": [embed]
        }
        
        return self._post("dp-93-daily-summary", payload)
    
    def _get_status_color(self, status: Status) -> int:
        """Get Discord color for status."""
        colors = {
            Status.STARTED: 0x3B82F6,   # Blue
            Status.PROGRESS: 0xEAB308,  # Yellow
            Status.DONE: 0x22C55E,     # Green
            Status.FAILED: 0xEF4444,   # Red
            Status.RETRYING: 0xF97316, # Orange
        }
        return colors.get(status, 0x6B7280)


# ============================================================================
# Convenience Functions
# ============================================================================

# Global notifier instance
_notifier = None

def get_notifier() -> DiscordNotifier:
    """Get or create the global notifier instance."""
    global _notifier
    if _notifier is None:
        _notifier = DiscordNotifier()
    return _notifier


def post_stage_update(
    run_id: str,
    stage: str,
    status: str,
    title: str = "",
    source: str = "",
    priority: str = "medium",
    artifacts: List[str] = None,
    next_stage: str = "",
    owner: str = "",
    duration_seconds: float = 0.0
) -> bool:
    """Convenience function for posting stage updates."""
    update = StageUpdate(
        run_id=run_id,
        stage=stage,
        status=Status(status) if isinstance(status, str) else status,
        title=title,
        source=source,
        priority=Priority(priority) if isinstance(priority, str) else priority,
        artifacts=artifacts or [],
        next_stage=next_stage,
        owner=owner,
        duration_seconds=duration_seconds
    )
    return get_notifier().post_stage_update(update)


def post_alert(
    run_id: str,
    stage: str,
    title: str,
    message: str = "",
    exception: str = None,
    traceback: str = None,
    retry_count: int = 0,
    priority: str = "high"
) -> bool:
    """Convenience function for posting alerts."""
    alert = Alert(
        run_id=run_id,
        stage=stage,
        title=title,
        message=message,
        exception=exception,
        traceback=traceback,
        retry_count=retry_count,
        priority=Priority(priority) if isinstance(priority, str) else priority
    )
    return get_notifier().post_alert(alert)


def post_approval_request(
    run_id: str,
    stage: str,
    title: str = "",
    description: str = "",
    artifacts: List[str] = None,
    approver: str = "",
    due_by: str = ""
) -> bool:
    """Convenience function for posting approval requests."""
    req = ApprovalRequest(
        run_id=run_id,
        stage=stage,
        title=title,
        description=description,
        artifacts=artifacts or [],
        approver=approver,
        due_by=due_by
    )
    return get_notifier().post_approval_request(req)


def post_daily_summary(
    date: str = None,
    runs_total: int = 0,
    runs_success: int = 0,
    runs_failed: int = 0,
    runs_in_progress: int = 0,
    top_dramas: List[Dict] = None,
    alerts_triggered: int = 0
) -> bool:
    """Convenience function for posting daily summaries."""
    summary = DailySummary(
        date=date or datetime.now().strftime("%Y-%m-%d"),
        runs_total=runs_total,
        runs_success=runs_success,
        runs_failed=runs_failed,
        runs_in_progress=runs_in_progress,
        top_dramas=top_dramas or [],
        alerts_triggered=alerts_triggered
    )
    return get_notifier().post_daily_summary(summary)


# ============================================================================
# Main (for testing)
# ============================================================================

if __name__ == "__main__":
    import sys
    
    print("Discord Notifier for Drama Pipeline")
    print("=" * 40)
    print(f"Available channels: {', '.join(CHANNELS.keys())}")
    print(f"Webhook URL set: {bool(os.environ.get('DISCORD_WEBHOOK_URL'))}")
    print(f"Bot token set: {bool(os.environ.get('DISCORD_BOT_TOKEN'))}")
    
    # Test basic posting if webhook is configured
    if os.environ.get('DISCORD_WEBHOOK_URL') or os.environ.get('DISCORD_BOT_TOKEN'):
        print("\nTesting webhook connection...")
        
        # Test stage update
        result = post_stage_update(
            run_id="TEST-001",
            stage="scout",
            status="started",
            title="Test Run",
            source="test_script",
            priority="medium",
            duration_seconds=5.5
        )
        print(f"Stage update: {'OK' if result else 'FAILED'}")
    else:
        print("\nNo Discord credentials set. Set DISCORD_WEBHOOK_URL or DISCORD_BOT_TOKEN to post messages.")
        sys.exit(1)
