# Loom Real-Time Monitoring Program — Release Freeze Notes

## Frozen baseline
- `loom_host_master.py`
- `loom_host_v2.py`

## Freeze status
- accepted for functional use
- accepted for UI use
- intended as the current stable baseline

## Notes
- use this pair together
- do not mix older master/core files with this frozen pair
- rebuild the EXE whenever Python source logic changes
- keep config and SQLite DB backed up separately

## Packaging reminder
Before packaging:
- confirm correct active source pair
- confirm example config file is present
- confirm old `build/`, `dist/`, and `__pycache__/` folders are cleared if needed

## Recommended tag
- `v2-ui-freeze`
