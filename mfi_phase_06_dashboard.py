#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_06_dashboard.py
# PHASE        : 6 — Dashboard
# PURPOSE      : Industrial real-time dashboard — FastAPI backend serving a
#                pure HTML/CSS/JS frontend (Chart.js). Runs the Phase 4 MFICore
#                pipeline in a background thread and exposes REST API endpoints
#                for live fleet KPIs. No database dependency required.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : fastapi, uvicorn, httpx, pydantic>=2.0, mfi_phase_01..04
# CLI          : python mfi_phase_06_dashboard.py --self-test
#                python mfi_phase_06_dashboard.py
#                python mfi_phase_06_dashboard.py --host 0.0.0.0 --port 8080
#                python mfi_phase_06_dashboard.py --interval 2.0
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import json
import logging
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

# --- FastAPI ---
try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
except ImportError as exc:
    print(
        f"[FATAL] fastapi/uvicorn not found: {exc}\n"
        "Install: pip install fastapi uvicorn --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Phase 4 (pulls in Phases 1-3 transitively) ---
try:
    from mfi_phase_04_core import (
        MFICore,
        MFIEnrichedRecord,
        DEFAULT_SITE,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_04_core: {exc}", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID            = "06"
PHASE_NAME          = "Dashboard"
PHASE_VERSION       = "1.0.0"

SERVER_HOST         = "127.0.0.1"
SERVER_PORT         = 8000
PIPELINE_INTERVAL   = 1.0           # Seconds between Core pipeline cycles
API_PREFIX          = "/api"        # REST API prefix (portability)
CORS_ORIGINS        = ["*"]         # Dev: allow all origins

# Status color mapping used by both backend and frontend
STATUS_COLORS: dict[str, str] = {
    "RUN"         : "#00e676",
    "IDLE"        : "#ffd600",
    "FAULT"       : "#ff1744",
    "MAINTENANCE" : "#ff9100",
    "UNKNOWN"     : "#546e7a",
}

# =============================================================================
# SECTION 3 — LOGGER SETUP
# =============================================================================
LOG_FORMAT = (
    "[%(asctime)s] "
    "[%(levelname)-8s] "
    "[MFI-P%(phase)s] "
    "[%(funcName)s] "
    "%(message)s"
)


class PhaseAdapter(logging.LoggerAdapter):
    """Injects phase ID into every log record."""

    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        kwargs.setdefault("extra", {})
        kwargs["extra"]["phase"] = PHASE_ID
        return msg, kwargs


def build_logger(name: str, level: int = logging.INFO) -> PhaseAdapter:
    """
    Build and return a PhaseAdapter-wrapped logger.

    Args:
        name  : Logger namespace.
        level : Logging level (default INFO for dashboard — less noise).

    Returns:
        PhaseAdapter wrapping a configured StreamHandler logger.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    base = logging.getLogger(name)
    base.setLevel(level)
    base.handlers.clear()
    base.addHandler(handler)
    base.propagate = False
    return PhaseAdapter(base, extra={"phase": PHASE_ID})


LOG = build_logger("mfi.phase06")

# =============================================================================
# SECTION 4 — PIPELINE STATE STORE
# =============================================================================

class PipelineStore:
    """
    Thread-safe in-memory store for the latest pipeline cycle results.

    The background pipeline thread writes here after each cycle;
    the API routes read from here on every request.
    """

    def __init__(self) -> None:
        self._lock          = threading.RLock()
        self._records       : list[MFIEnrichedRecord] = []
        self._cycle_count   : int = 0
        self._last_updated  : Optional[str] = None
        self._errors        : int = 0

    def update(
        self,
        records     : list[MFIEnrichedRecord],
        cycle_count : int,
    ) -> None:
        """
        Replace stored records with the latest pipeline cycle output.

        Args:
            records     : Latest enriched records from MFICore.run_cycle().
            cycle_count : Cumulative cycle number.
        """
        with self._lock:
            self._records       = records
            self._cycle_count   = cycle_count
            self._last_updated  = datetime.now(timezone.utc).isoformat()

    def snapshot(self) -> tuple[list[MFIEnrichedRecord], int, Optional[str]]:
        """
        Return a consistent snapshot of current state.

        Returns:
            (records, cycle_count, last_updated)
        """
        with self._lock:
            return list(self._records), self._cycle_count, self._last_updated

    def increment_errors(self) -> None:
        """Increment the pipeline error counter."""
        with self._lock:
            self._errors += 1

    @property
    def error_count(self) -> int:
        """Return the cumulative error count."""
        with self._lock:
            return self._errors


# =============================================================================
# SECTION 5 — PIPELINE BACKGROUND THREAD
# =============================================================================

class PipelineThread(threading.Thread):
    """
    Background thread that runs the MFICore pipeline continuously.

    Writes results to a PipelineStore after each cycle.
    Stops cleanly when stop() is called.
    """

    def __init__(
        self,
        store       : PipelineStore,
        site_id     : str   = DEFAULT_SITE,
        interval    : float = PIPELINE_INTERVAL,
    ) -> None:
        """
        Args:
            store    : Shared store for latest pipeline results.
            site_id  : Site identifier for fleet simulation.
            interval : Sleep time between cycles (seconds).
        """
        super().__init__(name="MFIPipelineThread", daemon=True)
        self._store     = store
        self._interval  = interval
        self._stop_evt  = threading.Event()
        self._core      = MFICore(site_id=site_id)
        LOG.info(
            "PipelineThread initialized │ site=%s │ interval=%.1fs",
            site_id, interval,
        )

    def stop(self) -> None:
        """Signal the thread to stop after the current cycle."""
        self._stop_evt.set()

    def run(self) -> None:
        """Main thread loop — runs pipeline cycles until stopped."""
        LOG.info("PipelineThread started")
        while not self._stop_evt.is_set():
            try:
                result  = self._core.run_cycle()
                self._store.update(
                    records     = result["enriched"],
                    cycle_count = result["cycle_number"],
                )
                LOG.debug(
                    "Pipeline cycle %d │ enriched=%d │ avail=%s",
                    result["cycle_number"],
                    len(result["enriched"]),
                    (
                        f"{result['fleet_availability']:.1%}"
                        if result["fleet_availability"] else "N/A"
                    ),
                )
            except Exception as exc:
                self._store.increment_errors()
                LOG.error("Pipeline cycle ERROR │ %s", exc)

            self._stop_evt.wait(timeout=self._interval)

        LOG.info("PipelineThread stopped")


# =============================================================================
# SECTION 6 — API RESPONSE BUILDERS
# =============================================================================

def _build_fleet_response(
    records     : list[MFIEnrichedRecord],
    cycle_count : int,
    last_updated: Optional[str],
    errors      : int,
) -> dict[str, Any]:
    """
    Build the /api/fleet response payload.

    Args:
        records      : Current enriched records.
        cycle_count  : Pipeline cycle number.
        last_updated : ISO 8601 timestamp of last update.
        errors       : Cumulative pipeline error count.

    Returns:
        Fleet summary dict.
    """
    status_dist : dict[str, int] = {}
    total_alarms    = 0
    total_crits     = 0
    total_warns     = 0
    total_pieces    = 0
    running_count   = 0
    availability_sum= 0.0
    avail_count     = 0

    for r in records:
        status_dist[r.status] = status_dist.get(r.status, 0) + 1
        if r.alarm_code != 0:
            total_alarms += 1
        if r.temp_severity == "CRITICAL":
            total_crits += 1
        elif r.temp_severity == "WARN":
            total_warns += 1
        if r.is_running:
            running_count += 1
        total_pieces += r.piece_count
        if r.availability is not None:
            availability_sum += r.availability
            avail_count += 1

    fleet_avail = (
        round(availability_sum / avail_count, 4)
        if avail_count > 0 else None
    )

    return {
        "cycle_number"          : cycle_count,
        "machine_count"         : len(records),
        "status_distribution"   : status_dist,
        "running_count"         : running_count,
        "fleet_availability"    : fleet_avail,
        "active_alarms"         : total_alarms,
        "critical_temps"        : total_crits,
        "warn_temps"            : total_warns,
        "total_pieces"          : total_pieces,
        "pipeline_errors"       : errors,
        "last_updated"          : last_updated,
    }


def _build_machines_response(
    records: list[MFIEnrichedRecord],
) -> list[dict[str, Any]]:
    """
    Build the /api/machines response — compact list of all machines.

    Args:
        records : Current enriched records.

    Returns:
        List of machine summary dicts, sorted by machine_id.
    """
    machines = []
    for r in sorted(records, key=lambda x: x.machine_id):
        machines.append({
            "machine_id"    : r.machine_id,
            "machine_type"  : r.machine_type,
            "protocol"      : r.protocol,
            "status"        : r.status,
            "alarm_code"    : r.alarm_code,
            "piece_count"   : r.piece_count,
            "good_count"    : r.good_count,
            "bad_count"     : r.bad_count,
            "cycle_time_sec": r.cycle_time_sec,
            "temperature_c" : r.temperature_c,
            "speed"         : r.speed,
            "quality_rate"  : r.quality_rate,
            "availability"  : r.availability,
            "temp_severity" : r.temp_severity,
            "is_running"    : r.is_running,
            "is_faulted"    : r.is_faulted,
            "status_changed": r.status_changed,
            "piece_delta"   : r.piece_delta,
            "cycle_number"  : r.cycle_number,
            "timestamp"     : r.timestamp,
        })
    return machines


def _build_kpi_response(
    records: list[MFIEnrichedRecord],
) -> dict[str, Any]:
    """
    Build the /api/kpi response — aggregated KPI summary.

    Args:
        records : Current enriched records.

    Returns:
        KPI summary dict.
    """
    if not records:
        return {"error": "no data"}

    # OEE = Availability × Performance × Quality (simplified)
    # Availability: fleet running ratio
    run_count   = sum(1 for r in records if r.is_running)
    avail_ratio = run_count / len(records)

    # Quality: weighted average quality_rate (exclude None)
    qrates  = [r.quality_rate for r in records if r.quality_rate is not None]
    quality = round(sum(qrates) / len(qrates), 4) if qrates else None

    # Performance: % of machines with non-zero cycle time (simplified)
    active_with_cycle   = sum(1 for r in records if r.cycle_time_sec > 0)
    performance         = round(active_with_cycle / len(records), 4)

    oee = (
        round(avail_ratio * performance * quality, 4)
        if quality is not None else None
    )

    # Production totals
    total_pieces    = sum(r.piece_count for r in records)
    total_good      = sum(r.good_count for r in records)
    total_bad       = sum(r.bad_count for r in records)

    # Type breakdown
    type_counts: dict[str, dict] = {}
    for r in records:
        t = r.machine_type
        if t not in type_counts:
            type_counts[t] = {"count": 0, "running": 0, "pieces": 0}
        type_counts[t]["count"]  += 1
        type_counts[t]["pieces"] += r.piece_count
        if r.is_running:
            type_counts[t]["running"] += 1

    return {
        "oee"               : oee,
        "availability"      : round(avail_ratio, 4),
        "performance"       : performance,
        "quality"           : quality,
        "total_pieces"      : total_pieces,
        "total_good"        : total_good,
        "total_bad"         : total_bad,
        "machines_total"    : len(records),
        "machines_running"  : run_count,
        "type_breakdown"    : type_counts,
    }


# =============================================================================
# SECTION 7 — DASHBOARD HTML (embedded)
# =============================================================================

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MyFactoryInsight — Live Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <style>
    :root {
      --bg:       #070d12;
      --surface:  #0d1a24;
      --border:   #1a2e40;
      --accent:   #00b4d8;
      --run:      #00e676;
      --idle:     #ffd600;
      --fault:    #ff1744;
      --maint:    #ff9100;
      --text:     #c8d8e4;
      --dim:      #4a6275;
      --mono:     'Share Tech Mono', monospace;
      --sans:     'Barlow', sans-serif;
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--sans);
      font-size: 14px;
      min-height: 100vh;
      overflow-x: hidden;
    }

    /* ── Header ── */
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 14px 28px;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
    }
    .logo {
      font-family: var(--mono);
      font-size: 18px;
      color: var(--accent);
      letter-spacing: 2px;
      text-transform: uppercase;
    }
    .logo span { color: #fff; }
    .header-meta {
      display: flex;
      align-items: center;
      gap: 24px;
      font-family: var(--mono);
      font-size: 12px;
      color: var(--dim);
    }
    .pulse-dot {
      width: 8px; height: 8px;
      border-radius: 50%;
      background: var(--run);
      display: inline-block;
      margin-right: 6px;
      animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50%       { opacity: 0.4; transform: scale(0.7); }
    }

    /* ── Layout ── */
    main { padding: 20px 28px; display: flex; flex-direction: column; gap: 20px; }

    /* ── Status grid ── */
    .status-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
    }
    .status-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 18px 20px;
      position: relative;
      overflow: hidden;
      transition: border-color 0.2s;
    }
    .status-card::before {
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0;
      height: 3px;
    }
    .status-card.run::before    { background: var(--run); }
    .status-card.idle::before   { background: var(--idle); }
    .status-card.fault::before  { background: var(--fault); }
    .status-card.maint::before  { background: var(--maint); }

    .card-label {
      font-family: var(--mono);
      font-size: 10px;
      letter-spacing: 3px;
      color: var(--dim);
      text-transform: uppercase;
      margin-bottom: 10px;
    }
    .card-value {
      font-family: var(--mono);
      font-size: 42px;
      font-weight: 400;
      line-height: 1;
    }
    .status-card.run .card-value   { color: var(--run); }
    .status-card.idle .card-value  { color: var(--idle); }
    .status-card.fault .card-value { color: var(--fault); }
    .status-card.maint .card-value { color: var(--maint); }
    .card-sub {
      font-size: 11px;
      color: var(--dim);
      margin-top: 6px;
      font-family: var(--mono);
    }

    /* ── KPI row ── */
    .kpi-row {
      display: grid;
      grid-template-columns: repeat(5, 1fr);
      gap: 14px;
    }
    .kpi-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 16px 18px;
    }
    .kpi-label {
      font-family: var(--mono);
      font-size: 9px;
      letter-spacing: 3px;
      color: var(--dim);
      text-transform: uppercase;
      margin-bottom: 8px;
    }
    .kpi-value {
      font-family: var(--mono);
      font-size: 26px;
      color: var(--accent);
    }
    .kpi-unit { font-size: 12px; color: var(--dim); margin-left: 3px; }

    /* ── Panels row ── */
    .panels-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
    }

    /* ── Panel ── */
    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 6px;
      overflow: hidden;
    }
    .panel-header {
      padding: 12px 18px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .panel-title {
      font-family: var(--mono);
      font-size: 11px;
      letter-spacing: 3px;
      color: var(--accent);
      text-transform: uppercase;
    }
    .panel-body { padding: 16px 18px; }

    /* ── Machine table ── */
    .machine-table {
      width: 100%;
      border-collapse: collapse;
      font-family: var(--mono);
      font-size: 11px;
    }
    .machine-table th {
      text-align: left;
      color: var(--dim);
      letter-spacing: 2px;
      padding: 0 8px 10px 0;
      border-bottom: 1px solid var(--border);
      font-weight: 400;
    }
    .machine-table td {
      padding: 7px 8px 7px 0;
      border-bottom: 1px solid rgba(26,46,64,0.5);
      vertical-align: middle;
    }
    .machine-table tr:last-child td { border-bottom: none; }

    .status-badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 3px;
      font-size: 10px;
      letter-spacing: 1px;
      font-weight: 700;
    }
    .badge-RUN         { background: rgba(0,230,118,0.12); color: var(--run);   border: 1px solid rgba(0,230,118,0.3); }
    .badge-IDLE        { background: rgba(255,214,0,0.12);  color: var(--idle);  border: 1px solid rgba(255,214,0,0.3); }
    .badge-FAULT       { background: rgba(255,23,68,0.15);  color: var(--fault); border: 1px solid rgba(255,23,68,0.4); }
    .badge-MAINTENANCE { background: rgba(255,145,0,0.12);  color: var(--maint); border: 1px solid rgba(255,145,0,0.3); }

    .temp-crit { color: var(--fault); }
    .temp-warn { color: var(--idle); }
    .temp-ok   { color: var(--dim); }
    .alarm-active { color: var(--fault); font-weight: 700; }

    /* ── Alarm list ── */
    .alarm-list { display: flex; flex-direction: column; gap: 8px; }
    .alarm-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 10px 14px;
      background: rgba(255,23,68,0.06);
      border: 1px solid rgba(255,23,68,0.2);
      border-radius: 4px;
      font-family: var(--mono);
      font-size: 11px;
    }
    .alarm-icon { color: var(--fault); font-size: 14px; }
    .alarm-machine { color: var(--accent); font-weight: 700; }
    .alarm-code { color: var(--fault); }
    .alarm-temp { color: var(--idle); }
    .no-alarms {
      text-align: center;
      padding: 30px;
      color: var(--dim);
      font-family: var(--mono);
      font-size: 12px;
    }
    .no-alarms-icon { font-size: 28px; margin-bottom: 8px; }

    /* ── Chart container ── */
    .chart-wrap { position: relative; height: 200px; }

    /* ── Footer ── */
    footer {
      text-align: center;
      padding: 14px;
      font-family: var(--mono);
      font-size: 10px;
      color: var(--dim);
      border-top: 1px solid var(--border);
      letter-spacing: 2px;
    }

    /* ── Scrollable table area ── */
    .table-scroll { max-height: 280px; overflow-y: auto; }
    .table-scroll::-webkit-scrollbar { width: 4px; }
    .table-scroll::-webkit-scrollbar-track { background: transparent; }
    .table-scroll::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

    /* ── Availability bar ── */
    .avail-bar-wrap { margin-top: 10px; }
    .avail-bar-bg {
      height: 6px;
      background: var(--border);
      border-radius: 3px;
      overflow: hidden;
    }
    .avail-bar-fill {
      height: 100%;
      background: var(--run);
      border-radius: 3px;
      transition: width 0.6s ease;
    }
    .avail-label {
      display: flex;
      justify-content: space-between;
      font-family: var(--mono);
      font-size: 10px;
      color: var(--dim);
      margin-bottom: 4px;
    }

    /* ── Fade-in animation ── */
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(6px); }
      to   { opacity: 1; transform: translateY(0); }
    }
    .status-card, .kpi-card { animation: fadeIn 0.4s ease both; }
  </style>
</head>
<body>

<header>
  <div class="logo">My<span>Factory</span>Insight</div>
  <div class="header-meta">
    <span><span class="pulse-dot"></span>LIVE</span>
    <span id="cycle-counter">CYCLE —</span>
    <span id="last-updated">—</span>
    <span id="site-id">SITE: —</span>
  </div>
</header>

<main>

  <!-- STATUS CARDS -->
  <div class="status-grid">
    <div class="status-card run">
      <div class="card-label">Running</div>
      <div class="card-value" id="count-run">—</div>
      <div class="card-sub" id="pct-run">— of fleet</div>
    </div>
    <div class="status-card idle">
      <div class="card-label">Idle</div>
      <div class="card-value" id="count-idle">—</div>
      <div class="card-sub" id="pct-idle">— of fleet</div>
    </div>
    <div class="status-card fault">
      <div class="card-label">Fault</div>
      <div class="card-value" id="count-fault">—</div>
      <div class="card-sub" id="pct-fault">— of fleet</div>
    </div>
    <div class="status-card maint">
      <div class="card-label">Maintenance</div>
      <div class="card-value" id="count-maint">—</div>
      <div class="card-sub" id="pct-maint">— of fleet</div>
    </div>
  </div>

  <!-- KPI ROW -->
  <div class="kpi-row">
    <div class="kpi-card">
      <div class="kpi-label">Fleet Availability</div>
      <div class="kpi-value" id="kpi-avail">—<span class="kpi-unit">%</span></div>
      <div class="avail-bar-wrap">
        <div class="avail-label"><span>0%</span><span>TARGET 85%</span><span>100%</span></div>
        <div class="avail-bar-bg"><div class="avail-bar-fill" id="avail-bar" style="width:0%"></div></div>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">OEE</div>
      <div class="kpi-value" id="kpi-oee">—<span class="kpi-unit">%</span></div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Quality Rate</div>
      <div class="kpi-value" id="kpi-quality">—<span class="kpi-unit">%</span></div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Total Pieces</div>
      <div class="kpi-value" id="kpi-pieces">—</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Active Alarms</div>
      <div class="kpi-value" id="kpi-alarms" style="color:var(--fault)">—</div>
    </div>
  </div>

  <!-- PANELS ROW -->
  <div class="panels-row">

    <!-- MACHINES TABLE -->
    <div class="panel">
      <div class="panel-header">
        <span class="panel-title">Machine Status</span>
        <span style="font-family:var(--mono);font-size:10px;color:var(--dim)" id="machine-count">0 machines</span>
      </div>
      <div class="panel-body" style="padding:0">
        <div class="table-scroll" style="padding:0 18px">
          <table class="machine-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>TYPE</th>
                <th>STATUS</th>
                <th>TEMP</th>
                <th>ALARM</th>
                <th>AVAIL</th>
              </tr>
            </thead>
            <tbody id="machine-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- RIGHT COLUMN -->
    <div style="display:flex;flex-direction:column;gap:20px">

      <!-- PRODUCTION CHART -->
      <div class="panel">
        <div class="panel-header">
          <span class="panel-title">Production by Type</span>
        </div>
        <div class="panel-body">
          <div class="chart-wrap">
            <canvas id="production-chart"></canvas>
          </div>
        </div>
      </div>

      <!-- ALARMS -->
      <div class="panel">
        <div class="panel-header">
          <span class="panel-title">Active Alarms &amp; Warnings</span>
          <span style="font-family:var(--mono);font-size:10px;color:var(--fault)" id="alarm-badge"></span>
        </div>
        <div class="panel-body">
          <div class="alarm-list" id="alarm-list">
            <div class="no-alarms">
              <div class="no-alarms-icon">✓</div>
              No active alarms
            </div>
          </div>
        </div>
      </div>

    </div>
  </div>

</main>

<footer>MYFACTORYINSIGHT v1.0 &nbsp;·&nbsp; PHASE 06 DASHBOARD &nbsp;·&nbsp; REAL-TIME INDUSTRIAL ANALYTICS</footer>

<script>
  const API = '';   // Same origin
  let prodChart = null;

  // ── Formatters ──────────────────────────────────────────────────────────
  const pct = v => v != null ? (v * 100).toFixed(1) : '—';
  const num = v => v != null ? v.toLocaleString() : '—';
  const ts  = s => {
    if (!s) return '—';
    const d = new Date(s);
    return d.toLocaleTimeString([], { hour12: false });
  };

  // ── Update header ────────────────────────────────────────────────────────
  function updateHeader(fleet) {
    document.getElementById('cycle-counter').textContent = 'CYCLE ' + fleet.cycle_number;
    document.getElementById('last-updated').textContent  = ts(fleet.last_updated);
    if (fleet.machine_count > 0) {
      const machines = window._machines || [];
      const site = machines.length ? machines[0].machine_id.replace(/\\d+$/, '').toUpperCase() : '—';
      document.getElementById('site-id').textContent = 'MACHINES: ' + fleet.machine_count;
    }
  }

  // ── Update status cards ──────────────────────────────────────────────────
  function updateStatusCards(fleet) {
    const d = fleet.status_distribution || {};
    const total = fleet.machine_count || 1;
    const statuses = ['RUN', 'IDLE', 'FAULT', 'MAINTENANCE'];
    const ids      = ['run', 'idle', 'fault', 'maint'];
    statuses.forEach((s, i) => {
      const count = d[s] || 0;
      document.getElementById('count-' + ids[i]).textContent = count;
      document.getElementById('pct-' + ids[i]).textContent =
        (count / total * 100).toFixed(0) + '% of fleet';
    });
  }

  // ── Update KPI row ────────────────────────────────────────────────────────
  function updateKPI(fleet, kpi) {
    document.getElementById('kpi-avail').innerHTML =
      pct(fleet.fleet_availability) + '<span class="kpi-unit">%</span>';
    document.getElementById('avail-bar').style.width =
      (fleet.fleet_availability ? (fleet.fleet_availability * 100).toFixed(1) : 0) + '%';
    document.getElementById('kpi-oee').innerHTML =
      pct(kpi.oee) + '<span class="kpi-unit">%</span>';
    document.getElementById('kpi-quality').innerHTML =
      pct(kpi.quality) + '<span class="kpi-unit">%</span>';
    document.getElementById('kpi-pieces').textContent =
      num(kpi.total_pieces);
    const alarmEl = document.getElementById('kpi-alarms');
    alarmEl.textContent = fleet.active_alarms ?? '—';
    alarmEl.style.color = (fleet.active_alarms > 0) ? 'var(--fault)' : 'var(--run)';
  }

  // ── Update machine table ──────────────────────────────────────────────────
  function updateMachineTable(machines) {
    window._machines = machines;
    document.getElementById('machine-count').textContent = machines.length + ' machines';
    const tbody = document.getElementById('machine-tbody');
    tbody.innerHTML = machines.map(m => {
      const tempClass = m.temp_severity === 'CRITICAL' ? 'temp-crit'
                      : m.temp_severity === 'WARN'     ? 'temp-warn' : 'temp-ok';
      const alarmClass = m.alarm_code !== 0 ? 'alarm-active' : '';
      const avail = m.availability != null ? (m.availability * 100).toFixed(0) + '%' : '—';
      return '<tr>' +
        '<td>' + m.machine_id + '</td>' +
        '<td style="color:var(--dim)">' + m.machine_type + '</td>' +
        '<td><span class="status-badge badge-' + m.status + '">' + m.status + '</span></td>' +
        '<td class="' + tempClass + '">' + m.temperature_c.toFixed(1) + '°C</td>' +
        '<td class="' + alarmClass + '">' + (m.alarm_code || '—') + '</td>' +
        '<td style="color:var(--dim)">' + avail + '</td>' +
      '</tr>';
    }).join('');
  }

  // ── Update production chart ───────────────────────────────────────────────
  function updateProductionChart(kpi) {
    const breakdown = kpi.type_breakdown || {};
    const labels    = Object.keys(breakdown);
    const pieces    = labels.map(k => breakdown[k].pieces);
    const running   = labels.map(k => breakdown[k].running);

    const COLORS = {
      CNC:      '#00b4d8',
      PRESS:    '#7b2fff',
      ROBOT:    '#00e676',
      CONVEYOR: '#ffd600',
      WELDER:   '#ff7043',
    };
    const bgColors = labels.map(l => COLORS[l] || '#546e7a');

    if (!prodChart) {
      const ctx = document.getElementById('production-chart').getContext('2d');
      prodChart = new Chart(ctx, {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'Total Pieces',
            data: pieces,
            backgroundColor: bgColors.map(c => c + '33'),
            borderColor: bgColors,
            borderWidth: 1.5,
            borderRadius: 3,
          }]
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: {
              ticks: { color: '#4a6275', font: { family: "'Share Tech Mono'", size: 10 } },
              grid:  { color: 'rgba(26,46,64,0.8)' },
            },
            y: {
              ticks: { color: '#4a6275', font: { family: "'Share Tech Mono'", size: 10 } },
              grid:  { color: 'rgba(26,46,64,0.8)' },
            }
          }
        }
      });
    } else {
      prodChart.data.labels                  = labels;
      prodChart.data.datasets[0].data        = pieces;
      prodChart.data.datasets[0].backgroundColor = bgColors.map(c => c + '33');
      prodChart.data.datasets[0].borderColor = bgColors;
      prodChart.update('none');
    }
  }

  // ── Update alarms panel ────────────────────────────────────────────────────
  function updateAlarms(machines) {
    const alarmList = document.getElementById('alarm-list');
    const alarmed   = machines.filter(m => m.alarm_code !== 0 || m.temp_severity !== 'OK');

    document.getElementById('alarm-badge').textContent =
      alarmed.length > 0 ? alarmed.length + ' ACTIVE' : '';

    if (alarmed.length === 0) {
      alarmList.innerHTML =
        '<div class="no-alarms"><div class="no-alarms-icon">✓</div>No active alarms</div>';
      return;
    }

    alarmList.innerHTML = alarmed.slice(0, 8).map(m => {
      const parts = [];
      if (m.alarm_code !== 0)
        parts.push('<span class="alarm-code">ALM ' + m.alarm_code + '</span>');
      if (m.temp_severity === 'CRITICAL')
        parts.push('<span class="alarm-temp">⚠ TEMP ' + m.temperature_c.toFixed(1) + '°C</span>');
      else if (m.temp_severity === 'WARN')
        parts.push('<span class="alarm-temp">~ TEMP ' + m.temperature_c.toFixed(1) + '°C</span>');
      return '<div class="alarm-item">' +
        '<span class="alarm-icon">▲</span>' +
        '<span class="alarm-machine">' + m.machine_id + '</span>' +
        '<span style="color:var(--dim)">' + m.machine_type + '</span>' +
        '<span class="status-badge badge-' + m.status + '">' + m.status + '</span>' +
        parts.join(' ') +
      '</div>';
    }).join('');
  }

  // ── Main data fetch ───────────────────────────────────────────────────────
  async function refresh() {
    try {
      const [fleetRes, machinesRes, kpiRes] = await Promise.all([
        fetch(API + '/api/fleet'),
        fetch(API + '/api/machines'),
        fetch(API + '/api/kpi'),
      ]);
      if (!fleetRes.ok || !machinesRes.ok || !kpiRes.ok) return;
      const fleet    = await fleetRes.json();
      const machines = await machinesRes.json();
      const kpi      = await kpiRes.json();

      updateHeader(fleet);
      updateStatusCards(fleet);
      updateKPI(fleet, kpi);
      updateMachineTable(machines);
      updateProductionChart(kpi);
      updateAlarms(machines);
    } catch(e) {
      console.warn('Dashboard fetch error:', e);
    }
  }

  // ── Boot ──────────────────────────────────────────────────────────────────
  refresh();
  setInterval(refresh, 2000);
</script>
</body>
</html>"""

# =============================================================================
# SECTION 8 — FASTAPI APPLICATION
# =============================================================================

def create_app(store: PipelineStore) -> FastAPI:
    """
    Create and configure the FastAPI application.

    All routes are defined here. Route handlers only read from the
    PipelineStore — no pipeline logic lives in the API layer.

    Args:
        store : Shared PipelineStore written by the background thread.

    Returns:
        Configured FastAPI application instance.
    """
    app = FastAPI(
        title       = "MyFactoryInsight Dashboard API",
        description = "Real-time industrial analytics REST API — Phase 6",
        version     = PHASE_VERSION,
    )

    # CORS — allow all origins for local dev
    app.add_middleware(
        CORSMiddleware,
        allow_origins       = CORS_ORIGINS,
        allow_methods       = ["GET"],
        allow_headers       = ["*"],
    )

    # ── Route: Dashboard HTML ─────────────────────────────────────────────
    @app.get("/", response_class=HTMLResponse, tags=["ui"])
    async def dashboard_html() -> HTMLResponse:
        """
        Serve the dashboard HTML page.
        Returns the embedded single-page application.
        """
        return HTMLResponse(content=DASHBOARD_HTML, status_code=200)

    # ── Route: Fleet summary ──────────────────────────────────────────────
    @app.get(f"{API_PREFIX}/fleet", tags=["api"])
    async def get_fleet() -> dict:
        """
        Fleet-level summary: status distribution, availability, alarms.

        Returns:
            JSON fleet summary object.
        """
        records, cycle_count, last_updated = store.snapshot()
        return _build_fleet_response(
            records     = records,
            cycle_count = cycle_count,
            last_updated= last_updated,
            errors      = store.error_count,
        )

    # ── Route: Machine list ───────────────────────────────────────────────
    @app.get(f"{API_PREFIX}/machines", tags=["api"])
    async def get_machines() -> list:
        """
        List of all machines with their current enriched KPIs.

        Returns:
            JSON array of machine objects sorted by machine_id.
        """
        records, _, _ = store.snapshot()
        return _build_machines_response(records)

    # ── Route: Single machine ─────────────────────────────────────────────
    @app.get(f"{API_PREFIX}/machines/{{machine_id}}", tags=["api"])
    async def get_machine(machine_id: str) -> dict:
        """
        Single machine detail by machine_id.

        Args:
            machine_id : URL path parameter, e.g. "machine01".

        Returns:
            JSON object for the requested machine.

        Raises:
            404 : If machine_id is not found in current cycle data.
        """
        records, _, _ = store.snapshot()
        for r in records:
            if r.machine_id == machine_id:
                return r.to_dict()
        raise HTTPException(
            status_code = 404,
            detail      = f"Machine '{machine_id}' not found in current cycle data.",
        )

    # ── Route: KPI summary ────────────────────────────────────────────────
    @app.get(f"{API_PREFIX}/kpi", tags=["api"])
    async def get_kpi() -> dict:
        """
        Aggregated KPI summary: OEE, quality, production totals, type breakdown.

        Returns:
            JSON KPI object.
        """
        records, _, _ = store.snapshot()
        return _build_kpi_response(records)

    # ── Route: Health check ───────────────────────────────────────────────
    @app.get(f"{API_PREFIX}/health", tags=["api"])
    async def health_check() -> dict:
        """
        Health check endpoint for monitoring.

        Returns:
            JSON with status, cycle_count, and last_updated.
        """
        _, cycle_count, last_updated = store.snapshot()
        return {
            "status"        : "ok",
            "phase"         : PHASE_ID,
            "version"       : PHASE_VERSION,
            "cycle_count"   : cycle_count,
            "last_updated"  : last_updated,
        }

    return app


# =============================================================================
# SECTION 9 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 6. Uses FastAPI TestClient — no live server needed.

    Validates:
      1.  PipelineStore initializes and snapshot() returns empty state.
      2.  PipelineStore.update() stores records and updates cycle_count.
      3.  PipelineStore.snapshot() returns consistent state under lock.
      4.  _build_fleet_response() computes correct status distribution.
      5.  _build_fleet_response() computes availability correctly.
      6.  _build_machines_response() returns sorted list.
      7.  _build_kpi_response() returns OEE, availability, quality fields.
      8.  _build_kpi_response() returns type_breakdown with correct keys.
      9.  GET / returns 200 with HTML content.
      10. GET /api/health returns 200 with ok status.
      11. GET /api/fleet returns 200 with machine_count.
      12. GET /api/machines returns 200 with list of length matching records.
      13. GET /api/machines/{id} returns 200 with correct machine_id.
      14. GET /api/machines/nonexistent returns 404.
      15. GET /api/kpi returns 200 with oee field.
      16. Full pipeline: run MFICore cycle, inject into store, verify API.
      17. DASHBOARD_HTML contains Chart.js script tag.
      18. DASHBOARD_HTML contains all required element IDs.
      19. API prefix is applied to all non-root routes.
      20. PipelineStore error_count increments correctly.

    Returns:
        True if all assertions pass, False otherwise.
    """
    from fastapi.testclient import TestClient
    from mfi_phase_03_json_model import MFIStandardModel
    from mfi_phase_04_core import MFIEnricher, MFICollector

    LOG.info("══════════ SELF-TEST START ══════════")
    passed = 0
    failed = 0

    def check(label: str, condition: bool, detail: str = "") -> None:
        nonlocal passed, failed
        if condition:
            LOG.info("  ✓ PASS │ %s", label)
            passed += 1
        else:
            LOG.error("  ✗ FAIL │ %s%s", label, f" → {detail}" if detail else "")
            failed += 1

    # ── Build test data ───────────────────────────────────────────────────
    def _make_er(mid: str, status: str, alarm: int = 0, temp: float = 50.0) -> MFIEnrichedRecord:
        """Build a minimal MFIEnrichedRecord for testing."""
        std = MFIStandardModel(
            site_id="mecanitec", machine_id=mid, machine_type="CNC",
            protocol="opcua", status=status, alarm_code=alarm,
            piece_count=10000, good_count=5 if status == "RUN" else 0,
            bad_count=0, cycle_time_sec=42.0 if status == "RUN" else 0.0,
            temperature_c=temp, speed=1000.0 if status == "RUN" else 0.0,
            timestamp="2026-05-16T20:00:00+00:00",
        )
        col = MFICollector()
        col.collect([std])
        return MFIEnricher().enrich(std, col.get_state(mid))

    records = [
        _make_er("machine01", "RUN",         alarm=0,   temp=50.0),
        _make_er("machine02", "IDLE",        alarm=0,   temp=40.0),
        _make_er("machine03", "FAULT",       alarm=102, temp=115.0),
        _make_er("machine04", "MAINTENANCE", alarm=400, temp=35.0),
        _make_er("machine05", "RUN",         alarm=0,   temp=85.0),
    ]

    # ── Tests 1-3: PipelineStore ──────────────────────────────────────────
    store = PipelineStore()
    snap_records, snap_cycle, snap_ts = store.snapshot()
    check("PipelineStore.snapshot() starts empty", len(snap_records) == 0 and snap_cycle == 0)

    store.update(records, cycle_count=5)
    snap_records2, snap_cycle2, snap_ts2 = store.snapshot()
    check("PipelineStore.update() stores records", len(snap_records2) == 5)
    check("PipelineStore.update() updates cycle_count", snap_cycle2 == 5)

    # ── Tests 4-5: _build_fleet_response ─────────────────────────────────
    fleet = _build_fleet_response(records, 5, snap_ts2, 0)
    dist  = fleet["status_distribution"]
    check(
        "_build_fleet_response() status_distribution correct",
        dist.get("RUN") == 2 and dist.get("FAULT") == 1,
        str(dist),
    )
    check(
        "_build_fleet_response() fleet_availability > 0",
        isinstance(fleet["fleet_availability"], float),
        str(fleet["fleet_availability"]),
    )

    # ── Tests 6-8: response builders ─────────────────────────────────────
    machines_list = _build_machines_response(records)
    check(
        "_build_machines_response() returns sorted list",
        machines_list[0]["machine_id"] == "machine01",
    )
    kpi = _build_kpi_response(records)
    check("_build_kpi_response() has oee field",         "oee"          in kpi)
    check("_build_kpi_response() has type_breakdown",    "type_breakdown" in kpi)

    # ── Tests 9-15: FastAPI TestClient ────────────────────────────────────
    app    = create_app(store)
    client = TestClient(app, raise_server_exceptions=True)

    r = client.get("/")
    check("GET / returns 200", r.status_code == 200, str(r.status_code))

    r = client.get("/api/health")
    check("GET /api/health returns 200", r.status_code == 200)
    check("GET /api/health status == ok", r.json().get("status") == "ok")

    r = client.get("/api/fleet")
    check("GET /api/fleet returns 200",            r.status_code == 200)
    check("GET /api/fleet has machine_count",  "machine_count" in r.json())

    r = client.get("/api/machines")
    check("GET /api/machines returns 200",         r.status_code == 200)
    check("GET /api/machines length == 5",     len(r.json()) == 5)

    r = client.get("/api/machines/machine01")
    check("GET /api/machines/machine01 returns 200",  r.status_code == 200)
    check("Correct machine_id in response",  r.json().get("machine_id") == "machine01")

    r = client.get("/api/machines/nonexistent")
    check("GET /api/machines/nonexistent returns 404", r.status_code == 404)

    r = client.get("/api/kpi")
    check("GET /api/kpi returns 200",      r.status_code == 200)
    check("GET /api/kpi has oee field",    "oee" in r.json())

    # ── Test 16: Full pipeline cycle via MFICore ──────────────────────────
    store2 = PipelineStore()
    core   = MFICore(site_id="mecanitec")
    result = core.run_cycle()
    store2.update(result["enriched"], cycle_count=result["cycle_number"])
    app2    = create_app(store2)
    client2 = TestClient(app2)
    r2      = client2.get("/api/fleet")
    check(
        "Full MFICore pipeline → API: machine_count ≥ 40",
        r2.json().get("machine_count", 0) >= 40,
        str(r2.json().get("machine_count")),
    )

    # ── Test 17: DASHBOARD_HTML has Chart.js ─────────────────────────────
    check(
        "DASHBOARD_HTML contains Chart.js CDN script",
        "chart.umd.min.js" in DASHBOARD_HTML,
    )

    # ── Test 18: DASHBOARD_HTML required element IDs ──────────────────────
    required_ids = [
        "count-run", "count-fault", "kpi-avail", "kpi-oee",
        "kpi-quality", "kpi-pieces", "machine-tbody", "alarm-list",
        "production-chart",
    ]
    ids_ok = all(f'id="{eid}"' in DASHBOARD_HTML for eid in required_ids)
    check(
        "DASHBOARD_HTML contains all required element IDs",
        ids_ok,
        str([eid for eid in required_ids if f'id="{eid}"' not in DASHBOARD_HTML]),
    )

    # ── Test 19: API prefix applied ───────────────────────────────────────
    r_no_prefix = client.get("/fleet")
    check(
        "Non-prefixed /fleet returns 404 (prefix enforced)",
        r_no_prefix.status_code == 404,
    )

    # ── Test 20: PipelineStore error_count ────────────────────────────────
    store3 = PipelineStore()
    store3.increment_errors()
    store3.increment_errors()
    check("PipelineStore.error_count == 2", store3.error_count == 2)

    # --- Summary ---
    total = passed + failed
    LOG.info(
        "══════════ SELF-TEST RESULT: %d/%d PASSED %s ══════════",
        passed,
        total,
        "✓ OK" if failed == 0 else "✗ FAIL",
    )
    return failed == 0


# =============================================================================
# SECTION 10 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for Phase 6."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_06_dashboard.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Industrial real-time dashboard — FastAPI + Chart.js"
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--self-test",
        action  = "store_true",
        help    = "Run built-in self-test (no server started). Exit 0=OK, 1=FAIL.",
    )
    parser.add_argument(
        "--host",
        type    = str,
        default = SERVER_HOST,
        help    = f"Server bind host (default: {SERVER_HOST}).",
    )
    parser.add_argument(
        "--port",
        type    = int,
        default = SERVER_PORT,
        help    = f"Server port (default: {SERVER_PORT}).",
    )
    parser.add_argument(
        "--site",
        type    = str,
        default = DEFAULT_SITE,
        metavar = "SITE_ID",
        help    = f"Site identifier (default: {DEFAULT_SITE}).",
    )
    parser.add_argument(
        "--interval",
        type    = float,
        default = PIPELINE_INTERVAL,
        metavar = "SECS",
        help    = f"Pipeline cycle interval in seconds (default: {PIPELINE_INTERVAL}).",
    )
    return parser


def main() -> None:
    """
    Phase 6 entry point.

    Modes:
      --self-test : Run validation suite (no server).
      (default)   : Start background pipeline thread and uvicorn server.
    """
    parser  = build_arg_parser()
    args    = parser.parse_args()

    # ── Phase header ──────────────────────────────────────────────────────
    LOG.info("╔══════════════════════════════════════════════╗")
    LOG.info("║  MyFactoryInsight  │  Phase %-2s │ %-20s ║", PHASE_ID, PHASE_NAME)
    LOG.info("║  Version %-7s   │ Site: %-23s ║", PHASE_VERSION, args.site)
    LOG.info("╚══════════════════════════════════════════════╝")

    # ── Self-test mode ────────────────────────────────────────────────────
    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)

    # ── Server mode ───────────────────────────────────────────────────────
    store   = PipelineStore()
    thread  = PipelineThread(
        store       = store,
        site_id     = args.site,
        interval    = args.interval,
    )

    # Run one immediate cycle so dashboard has data before first browser request
    LOG.info("Running initial pipeline cycle...")
    initial_core = thread._core
    result = initial_core.run_cycle()
    store.update(result["enriched"], cycle_count=result["cycle_number"])
    LOG.info(
        "Initial cycle complete │ enriched=%d machines",
        len(result["enriched"]),
    )

    # Start background thread
    thread.start()

    app = create_app(store)

    LOG.info(
        "Dashboard available at http://%s:%d",
        args.host, args.port,
    )
    LOG.info("Press Ctrl+C to stop.")

    try:
        uvicorn.run(
            app,
            host        = args.host,
            port        = args.port,
            log_level   = "warning",   # Suppress uvicorn access logs (MFI logger used)
        )
    finally:
        thread.stop()
        thread.join(timeout=3.0)
        LOG.info("Phase %s shutdown complete", PHASE_ID)


# =============================================================================
# SECTION 11 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
