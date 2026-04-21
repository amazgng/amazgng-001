# Loom Real-Time Monitoring Program

A Python-based host/dashboard system for SMIT looms over Ethernet TCP/IP.

This project is intended for QT5-based loom HMIs and provides:

- real-time loom monitoring dashboard
- Machine IP Management
- TS / TC communication handling
- administrative declaration workflow
- per-loom enable/disable control
- live in-place dashboard refresh
- configuration-based deployment
- Windows EXE packaging via PyInstaller

## Current frozen baseline

This repository is prepared around the frozen V2 UI release baseline:

- `loom_host_master.py`
- `loom_host_v2.py`

These are the main runtime source files used for packaging and deployment.

## Main features

### Monitoring
- Read All Status
- pattern information
- speed / picks / density / efficiency display
- loom status category and detail
- preselection state
- software ID / version
- warp tensions
- last stop / error display

### Operations
- send pattern
- preselection write / reset
- lamp write
- remote stop
- administrative declaration workflow

### Management
- Machine IP Management page
- per-loom TS port setting
- per-loom QT5 full-status support flag
- per-loom enabled / disabled flag
- config auto-save through dashboard UI

### Diagnostics
- diagnostics page
- live events page
- communication-layer visibility
- log path / config path display
- SQLite event persistence

## Compatibility note

This dashboard is intended for **QT5 loom displays**.

Different loom software families may return slightly different protocol payloads.
The current frozen release supports both 26-byte and 28-byte `0x0C` full-status variants, but field validation is still recommended after loom replacement or HMI software changes.

## Repository structure

Suggested repository layout:

```text
loom-real-time-monitoring-program/
├─ loom_host_master.py
├─ loom_host_v2.py
├─ loom_host_master_config.example.json
├─ README.md
├─ CHANGELOG.md
├─ RELEASE_FREEZE_NOTES.md
├─ RELEASE_DESCRIPTION.md
└─ .gitignore
```

## Local run

```bash
python loom_host_master.py --config loom_host_master_config.example.json run
```

## Packaging

Before packaging, confirm that the active runtime source files are:

- `loom_host_master.py`
- `loom_host_v2.py`

Then build with PyInstaller:

```bash
py -m PyInstaller --noconfirm --clean loom_host_dashboard.spec
```

## Recommended runtime package contents

For a packaged Windows runtime folder:

```text
LoomHostDashboard/
├─ LoomHostDashboard.exe
├─ _internal/
├─ loom_host_master_config.example.json
├─ loom_events.db
└─ start_dashboard.bat
```

## Administrative declaration note

Administrative declaration data is not refreshed by ordinary TS polling alone.
It depends on the loom HMI returning the declaration completion through the TC path.
If declaration values do not appear, verify both:

- the loom-side declaration transmission
- the host-side declaration completion capture

## Known limitations

- declaration completion display depends on actual loom HMI return format
- some loom software families may still need protocol-specific refinements
- EXE packaging must be rebuilt whenever Python source logic changes
- field verification is recommended after any loom or HMI replacement

## Recommended GitHub release practice

Store in the repository:

- source files
- example config
- README
- changelog
- freeze notes

Store in GitHub Release assets:

- packaged `.exe`
- zipped runtime folder
- user manual (DOCX / PDF)
