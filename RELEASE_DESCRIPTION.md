# Loom Real-Time Monitoring Program — Release v2

## Release title
**Loom Real-Time Monitoring Program v2**

## Summary
This release is the current frozen baseline of the Loom Real-Time Monitoring Program for QT5-based SMIT loom integration.

It includes the stable V2 UI dashboard, the current protocol/core implementation, the GitHub documentation set, and the example configuration needed for packaging and deployment.

## Included files
- `loom_host_master.py`
- `loom_host_v2.py`
- `README.md`
- `CHANGELOG.md`
- `RELEASE_FREEZE_NOTES.md`
- `loom_host_master_config.example.json`

## Highlights
- real-time monitoring dashboard
- Machine IP Management
- per-loom enable / disable control
- live in-place refresh
- administrative declaration workflow
- diagnostics support
- support for multiple loom protocol full-status variants
- QT5-oriented deployment baseline

## Technical notes
- supports both 26-byte and 28-byte `0x0C` full-status variants
- keeps flexible administrative declaration request/completion parsing
- uses `loom_host_master.py` and `loom_host_v2.py` as the active runtime source pair
- intended to be the current stable repository baseline

## Recommended release assets
Attach these to the GitHub Release if available:
- packaged Windows EXE
- zipped runtime folder
- user manual (DOCX / PDF)

## Known limitations
- administrative declaration completion still depends on actual loom HMI return content
- some loom software families may require additional protocol adaptation
- packaged EXE must be rebuilt after any Python source logic change
- field validation is still recommended after loom replacement or HMI changes

## Notes
This release should be used as the branching point for future updates.
Do not mix older master/core files with this baseline.
