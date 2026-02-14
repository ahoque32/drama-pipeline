# Operation Scout Drama — Drama Pipeline

Modular, decoupled YouTube Shorts drama content pipeline.

## Architecture

```
ScoutDrama → ScriptSmith → DramaMaestro
     ↓            ↓              ↓
  seeds/      drafts/        logs/
     ↓            ↓
  Google Drive sync
```

Each module is independent and can be run standalone.

## Quick Start

```bash
# Full pipeline
python scripts/drama_maestro.py

# Individual modules
python scripts/scout_drama.py              # Generate seeds
python scripts/scriptsmith.py              # Generate scripts from seeds
python scripts/drama_maestro.py --skip-scout  # Only run scriptsmith

# Phase 1C Features
python scripts/breaking_news.py            # Check/process breaking news
python scripts/breaking_news.py --check-only  # Just check for breaking seeds
python scripts/daily_summary.py            # Generate daily summary report
python scripts/cost_tracker.py             # Show daily cost report
python scripts/telegram_bot.py --send-scripts # Send scripts with inline buttons

# Phase 2 Features
python scripts/youtube_uploader.py --auth  # Authenticate with YouTube
python scripts/youtube_uploader.py --upload video.mp4 --title "Title"
python scripts/retention_watcher.py --report  # Performance tracking
python scripts/cron_scheduler.py --install    # Install daily schedule
python scripts/cron_scheduler.py --show       # Show current schedule
python scripts/error_recovery.py --health     # Check pipeline health
```

## Configuration

Edit `config.yaml` or set environment variables:
- `X_BEARER_TOKEN` — X/Twitter API token
- `ANTHROPIC_API_KEY` — Claude API key
- `TELEGRAM_BOT_TOKEN` — Telegram bot token
- `TELEGRAM_CHAT_ID` — Telegram chat ID for notifications
- `YOUTUBE_CLIENT_ID` — YouTube OAuth client ID
- `YOUTUBE_CLIENT_SECRET` — YouTube OAuth client secret

## Output Structure

- `seeds/YYYY-MM-DD.json` — Full seed cards with validation
- `seeds/YYYY-MM-DD-summary.md` — Human-readable summary
- `drafts/YYYY-MM-DD.json` — All generated scripts
- `drafts/YYYY-MM-DD-passing.md` — Scripts passing quality checks
- `approved/YYYY-MM-DD/` — Approved scripts ready for production
- `audio/YYYY-MM-DD/` — Generated voiceovers
- `assets/YYYY-MM-DD/` — Downloaded video assets
- `handoffs/YYYY-MM-DD/` — Complete video packages
- `breaking/` — Breaking news fast-track records
- `costs/YYYY-MM-DD.json` — Daily API cost tracking
- `uploads/YYYY-MM-DD.json` — YouTube upload records
- `analytics/` — Performance analytics reports
- `logs/YYYY-MM-DD.json` — Pipeline execution log
- `logs/YYYY-MM-DD-summary.json` — Daily summary reports
- `state/pending_approvals.json` — Pending approval state
- `state/error_log.json` — Error tracking
- `state/circuit_breakers.json` — Circuit breaker states
- `tokens/youtube_tokens.json` — YouTube OAuth tokens

All outputs sync to Google Drive: `Autonomous YouTube/DramaPipeline`

## Phase 1C Features

### ⚡ Breaking News Override
Fast-track protocol for high-priority seeds:
- Detects `time_sensitivity: "high"` in seeds
- Generates 1 urgent script (skips variations)
- Sends ⚡ URGENT Telegram alert
- Auto-approves and triggers full pipeline
- Target: < 1 hour from detection to handoff

### 📊 Daily Summary Bot
22:00 EST automated report including:
- Seeds scanned, scripts generated, approved, published
- Cost tracking per video
- Pipeline success metrics
- Telegram-formatted output

### 💰 Cost Tracking
Per-video API cost logging:
- Track Claude calls, token usage
- Daily/weekly cost reports
- Budget alert if > $2/day

### 💬 Telegram Inline Buttons
Proper callback handling for approvals:
- ✅ Approve — Triggers VoiceForge → AssetHunter → Handoff
- ✏️ Edit — Activates edit mode with instructions
- ❌ Kill — Rejects script
- 🔄 Rewrite — Queues script for rewrite

## Phase 2 Features

### 📺 YouTube Upload API
OAuth-based video upload:
- OAuth2 flow with refresh tokens
- `videos.insert` with resumable upload
- Metadata builder (title, description, tags)
- Privacy status control (private/unlisted/public)
- Upload tracking in `uploads/` directory

### 📈 RetentionWatcher
YouTube Analytics integration:
- Views, watch time, retention metrics
- Engagement rate tracking
- Video health checks with alerts
- Performance reports with top performers
- Telegram-formatted reports

### ⏰ Cron Scheduling
Automated daily operations:
- 08:00 EST — Pipeline run
- 12:00 EST — Retention tracking
- 22:00 EST — Daily summary
- Install/remove/manage via CLI

### 🛡️ Error Recovery
Robust failure handling:
- Retry logic with exponential backoff
- Circuit breaker pattern
- Error logging with severity levels
- Telegram alerts for critical errors
- Pipeline health monitoring
- `@retry` decorator for functions

## Pipeline Stages

1. **ScoutDrama** — Fetches drama seeds from X/Twitter and Reddit
2. **ScriptSmith** — Generates 8-line retention-optimized scripts
3. **DramaMaestro** — Orchestrates approval and production
4. **VoiceForge** — Generates AI voiceover
5. **AssetHunter** — Downloads video assets
6. **HandoffAssembler** — Packages complete video project
7. **YouTubeUploader** — Uploads to YouTube
8. **RetentionWatcher** — Tracks performance metrics
