# MyFactoryInsight MFI — Claude Code Guide

## What This Project Does
Industrial IoT pipeline that simulates, ingests, normalizes, enriches, and visualizes manufacturing machine data in real time. Serves two web UIs: a desktop dashboard (port 8000) and a mobile PWA (port 8001).

## How to Run
```bash
# Full system
python MyFactoryInsight_MFI.py

# Smoke test (no external services needed)
python MyFactoryInsight_MFI.py --self-test

# Lightweight dev mode
python MyFactoryInsight_MFI.py --dry-run --no-storage

# Individual phase testing
python mfi_phase_01_simulation.py --self-test
python mfi_phase_04_core.py --cycles 5
python mfi_phase_06_dashboard.py --host 0.0.0.0 --port 9000
```

## How to Test
Every file has a `--self-test` flag. The master self-test validates all 11 phases end-to-end:
```bash
python MyFactoryInsight_MFI.py --self-test
```
Expected: 13 assertions pass, HTTP /api/health returns 200, pipeline completes 2 cycles.

## Project Structure
One file per phase, flat directory:
```
MyFactoryInsight_MFI.py       # Master orchestrator (entry point)
mfi_phase_01_simulation.py    # Fleet simulator (50 machines)
mfi_phase_02_readers.py       # Protocol adapters (OPC UA, Modbus, MQTT)
mfi_phase_03_json_model.py    # Pydantic normalizer → MFIStandardModel
mfi_phase_04_core.py          # Enrichment + KPI derivation + routing
mfi_phase_05_storage.py       # MQTT / InfluxDB integration
mfi_phase_06_dashboard.py     # FastAPI dashboard REST + HTML UI (port 8000)
mfi_phase_07_alerts.py        # Alert rules engine + journal
mfi_phase_08_users.py         # User auth stubs (not yet implemented)
mfi_phase_09_reports.py       # Report export stubs
mfi_phase_10_predict.py       # ML risk scoring (scikit-learn)
mfi_phase_11_mobile.py        # Mobile PWA API + WebSocket (port 8001)
```

## Key Conventions
- **Pydantic v2** for all data validation — add fields to `MFIStandardModel` (phase 3) or `MFIEnrichedRecord` (phase 4), never pass raw dicts between phases
- **Router pattern** — phase 4 `MFIRouter` dispatches to named handlers; phase 5 registers live handlers at startup
- **No silent failures** — log every error with context before continuing; never swallow exceptions
- **CLI flags** control optional features: `--dry-run`, `--no-dashboard`, `--no-mobile`, `--no-predict`, `--no-storage`
- **Thread safety** — all cross-phase state goes through `PipelineStore` or `MobileStore`, not direct imports

## Data Flow Summary
```
Phase 1 (Simulate) → Phase 2 (Read) → Phase 3 (Normalize/Pydantic)
  → Phase 4 (Enrich + Route)
    → Phase 5 (MQTT/InfluxDB)   [optional]
    → Phase 6 (Dashboard API)   [port 8000]
    → Phase 7 (Alerts)
    → Phase 10 (Predict AI)
    → Phase 11 (Mobile API)     [port 8001]
```

## Known Stubs (Not Yet Implemented)
- Phase 8: user authentication / RBAC
- Phase 9: report generation / PDF export
- Phase 5: MQTT publish and InfluxDB write are no-ops in `--dry-run`

## Dependencies to Install
```bash
pip install fastapi uvicorn pydantic httpx
# Optional: pip install asyncua pymodbus paho-mqtt influxdb-client scikit-learn
```

## Architecture Advice
See [ARCHITECTURE_COMPARISON.md](./ARCHITECTURE_COMPARISON.md) for a full cross-project analysis and prioritized improvement roadmap.
