# OPERATING_PROTOCOL.md — Maestro Daily Execution Standard

Last updated: 2026-03-06

This is the permanent protocol. If daily memory and automation conflict, follow this document unless Ahawk/Anton explicitly override it.

## 1) Core Operating Rules

1. **Manual scripting is default.**
   - Do not wait on ScriptSmith/Codex API availability.
   - If scripted generation is empty/failing, immediately produce manual scripts.

2. **Own asset retrieval directly.**
   - Do not stop at passive adapter output.
   - Continue with manual sourcing until package is usable.

3. **No silent failures.**
   - Immediately alert `#proj-maestro-drama` when a stage fails/degrades.

4. **No weak handoffs.**
   - Block handoff if asset viability thresholds are not met.

5. **Quality > autopilot.**
   - If automation says pass but creative quality is weak, reject and iterate.

## 2) Daily Workflow
- Session start: SOUL + PLAYBOOK + memory + logs
- Scout: proof-of-concept drama seeds
- Script: manual-first, 5-part, 7-8 lines, 30-35s, contextual CTA
- Assets: X → YouTube → IG(auth) → web/manual
- Viability gate: block handoff if assets weak
- Handoff/Drive: only with usable package
- Findings + memory update

## 3) Mandatory Alerts
Alert channel immediately on stage failure/degradation with:
- stage
- cause
- corrective action

## 4) Downloader Standards
- yt-dlp: quality merge, retries, ipv4 fallback, cookies support
- IG: maintain valid cookie auth
- YouTube fallback: query quality + relevance filtering
- Avoid watermark-heavy/low-value media

## 5) Non-Negotiables
- No false-complete reporting
- No bypassing approval policy
- No infra modifications unless explicitly requested
