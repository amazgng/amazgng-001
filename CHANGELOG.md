# Loom Real-Time Monitoring Program

# CHANGELOG

## V2 UI Frozen Release
**Date:** 2026-04-20

### Main changes
- frozen stable V2 UI baseline prepared
- denser dashboard layout with improved visual hierarchy
- compact administrative declaration area
- improved action button organization
- Machine IP Management with enabled / disabled loom support
- live in-place dashboard refresh
- QT5-oriented monitoring flow retained
- diagnostics and live events navigation retained
- Beijing time display integrated into dashboard timestamps

### Protocol / backend changes included
- support for both 26-byte and 28-byte `0x0C` full-status payload variants
- flexible administrative declaration request / completion parsing
- improved handling of disabled looms in polling logic
- SQLite persistence retained for raw frames and decoded events

### Known limitations
- administrative declaration completion still depends on actual HMI return content
- some loom software families may require additional protocol adaptation
- packaged EXE must be rebuilt after any Python source logic change
- real field validation is still recommended after loom replacement

---

## Release V2
**Date:** 2026-04-20

### Main changes
- built-in release/version display
- startup/shutdown robustness improvements
- communication diagnostics indicators
- per-loom polling settings support
- file logging support
- config auto-backup support
- diagnostics page additions

### Known limitations
- cross-family protocol differences may still exist
- not all undocumented replies are decoded
