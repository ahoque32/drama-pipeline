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
```

## Configuration

Edit `config.yaml` or set environment variables:
- `X_BEARER_TOKEN` — X/Twitter API token
- `ANTHROPIC_API_KEY` — Claude API key

## Output Structure

- `seeds/YYYY-MM-DD.json` — Full seed cards with validation
- `seeds/YYYY-MM-DD-summary.md` — Human-readable summary
- `drafts/YYYY-MM-DD.json` — All generated scripts
- `drafts/YYYY-MM-DD-passing.md` — Scripts passing quality checks
- `logs/YYYY-MM-DD.json` — Pipeline execution log

All outputs sync to Google Drive: `Autonomous YouTube/DramaPipeline`
