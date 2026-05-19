#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_main.py
# PURPOSE      : Master orchestrator — starts the full MFI system in one command.
#                Launches one shared pipeline (Phase 4), wires Phase 5/7/10
#                service handlers, serves the dashboard (Phase 6, port 8000)
#                and the mobile API (Phase 11, port 8001) in background threads.
#                Displays a live terminal status view. Shuts down cleanly on
#                Ctrl+C.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-19
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : All mfi_phase_01..11 files in the same directory.
# CLI          : python mfi_main.py                     # start everything
#                python mfi_main.py --dry-run           # no MQTT/InfluxDB
#                python mfi_main.py --no-dashboard      # skip Phase 6
#                python mfi_main.py --no-mobile         # skip Phase 11
#                python mfi_main.py --no-predict        # skip Phase 10 AI
#                python mfi_main.py --no-storage        # skip Phase 5
#                python mfi_main.py --self-test         # validate imports
#                python mfi_main.py --status            # check running ports
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

# --- FastAPI / uvicorn ---
try:
    import uvicorn
    from fastapi.testclient import TestClient
except ImportError as exc:
    print(f"[FATAL] fastapi/uvicorn not found: {exc}", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# SECTION 2 — UPSTREAM PHASE IMPORTS
# =============================================================================
# Each import is wrapped individually so a missing phase gives a clear error.

def _require(module: str):
    """Import a phase module or exit with a clear error."""
    import importlib
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        print(
            f"[FATAL] Cannot import {module}: {exc}\n"
            f"Ensure {module}.py is in the same directory as mfi_main.py.",
            file=sys.stderr,
        )
        sys.exit(1)


_p04 = _require("mfi_phase_04_core")
_p05 = _require("mfi_phase_05_storage")
_p06 = _require("mfi_phase_06_dashboard")
_p07 = _require("mfi_phase_07_alerts")
_p10 = _require("mfi_phase_10_predict")
_p11 = _require("mfi_phase_11_mobile")

# Convenience aliases
MFICore             = _p04.MFICore
MFIEnrichedRecord   = _p04.MFIEnrichedRecord
ROUTE_ALERT         = _p04.ROUTE_ALERT

MFIStorageService   = _p05.MFIStorageService

PipelineStore       = _p06.PipelineStore
create_dashboard    = _p06.create_app

AlertService        = _p07.AlertService

PredictService      = _p10.PredictService
PredictionResult    = _p10.PredictionResult

MobileStore         = _p11.MobileStore
MobileCard          = _p11.MobileCard
MobileFleet         = _p11.MobileFleet
MobileAlert         = _p11.MobileAlert
MobileRisk          = _p11.MobileRisk
build_mobile_fleet  = _p11.build_mobile_fleet
create_mobile_app   = _p11.create_mobile_app
WebSocketManager    = _p11.WebSocketManager
NotificationService = _p11.NotificationService

# =============================================================================
# SECTION 3 — CONFIG / CONSTANTS
# =============================================================================
VERSION             = "1.0.0"
APP_NAME            = "MyFactoryInsight"

DEFAULT_SITE        = "mecanitec"
PIPELINE_INTERVAL   = 1.0           # Seconds between pipeline cycles
BASELINE_CYCLES     = 3             # AI warm-up cycles before scoring

DASHBOARD_HOST      = "127.0.0.1"
DASHBOARD_PORT      = 8000
MOBILE_HOST         = "127.0.0.1"
MOBILE_PORT         = 8001
SERVER_STARTUP_WAIT = 2.5           # Seconds to wait for uvicorn to bind port

STATUS_DISPLAY_INTERVAL = 5.0       # Seconds between terminal status refresh

# ANSI color codes for terminal display
ANSI_RESET  = "\033[0m"
ANSI_BOLD   = "\033[1m"
ANSI_CYAN   = "\033[96m"
ANSI_GREEN  = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_RED    = "\033[91m"
ANSI_ORANGE = "\033[38;5;208m"
ANSI_DIM    = "\033[2m"
ANSI_CLEAR  = "\033[2J\033[H"

# =============================================================================
# SECTION 4 — LOGGER SETUP
# =============================================================================
LOG_FORMAT = (
    "[%(asctime)s] "
    "[%(levelname)-8s] "
    "[MFI-MAIN] "
    "[%(funcName)s] "
    "%(message)s"
)


def build_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Build a StreamHandler logger for the orchestrator.

    Args:
        name  : Logger name.
        level : Log level (default INFO — less noise than phases).

    Returns:
        Configured Logger instance.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    base = logging.getLogger(name)
    base.setLevel(level)
    base.handlers.clear()
    base.addHandler(handler)
    base.propagate = False
    return base


LOG = build_logger("mfi.main")


# =============================================================================
# SECTION 5 — UVICORN SERVER THREAD RUNNER
# =============================================================================

class UvicornThread:
    """
    Runs a FastAPI application in a daemon thread using its own asyncio event loop.

    Supports graceful shutdown via stop().

    Usage:
        srv = UvicornThread(app, host, port, name)
        srv.start()
        time.sleep(1)
        srv.stop()
    """

    def __init__(
        self,
        app     : Any,
        host    : str,
        port    : int,
        name    : str = "uvicorn",
    ) -> None:
        """
        Args:
            app  : FastAPI application instance.
            host : Bind host (e.g., "127.0.0.1").
            port : TCP port.
            name : Thread name (for logging and diagnostics).
        """
        self.host   = host
        self.port   = port
        self.name   = name
        self._app   = app
        self._server: Optional[uvicorn.Server] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the uvicorn server in a background daemon thread."""

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            config = uvicorn.Config(
                self._app,
                host        = self.host,
                port        = self.port,
                log_level   = "error",
            )
            self._server = uvicorn.Server(config)
            loop.run_until_complete(self._server.serve())
            loop.close()

        self._thread = threading.Thread(
            target  = _run,
            name    = self.name,
            daemon  = True,
        )
        self._thread.start()
        LOG.info(
            "%-20s started │ http://%s:%d",
            self.name, self.host, self.port,
        )

    def stop(self) -> None:
        """Signal the uvicorn server to stop."""
        if self._server:
            self._server.should_exit = True
        LOG.info("%-20s stopping...", self.name)

    def is_alive(self) -> bool:
        """Return True if the server thread is still running."""
        return self._thread is not None and self._thread.is_alive()


# =============================================================================
# SECTION 6 — MASTER PIPELINE THREAD
# =============================================================================

class MasterPipelineThread(threading.Thread):
    """
    Single background thread that drives the entire MFI pipeline.

    Each cycle:
      1. MFICore.run_cycle()         → enriched records + triggers MQTT/InfluxDB/Alert
      2. PredictService.ingest()     → AI risk scores (optional)
      3. Phase 6 PipelineStore update → feeds dashboard REST API
      4. Phase 11 MobileStore update  → feeds mobile REST + WebSocket

    This replaces the individual pipeline threads from Phase 6 and Phase 11.
    One shared pipeline — single source of truth.
    """

    def __init__(
        self,
        core            : MFICore,
        dash_store      : PipelineStore,
        mobile_store    : MobileStore,
        alert_service   : AlertService,
        predict_service : Optional[PredictService],
        notif_service   : NotificationService,
        interval        : float = PIPELINE_INTERVAL,
    ) -> None:
        """
        Args:
            core            : Shared MFICore orchestrator (Phase 4).
            dash_store      : PipelineStore for dashboard (Phase 6).
            mobile_store    : MobileStore for mobile API (Phase 11).
            alert_service   : AlertService for alert journal queries.
            predict_service : PredictService (Phase 10), or None if disabled.
            notif_service   : NotificationService for mobile push.
            interval        : Sleep between pipeline cycles (seconds).
        """
        super().__init__(name="MasterPipeline", daemon=True)
        self._core          = core
        self._dash_store    = dash_store
        self._mobile_store  = mobile_store
        self._alert_svc     = alert_service
        self._predict       = predict_service
        self._notif         = notif_service
        self._interval      = interval
        self._stop_evt      = threading.Event()
        self._cycle         = 0
        self._errors        = 0

        # Cumulative stats for the status display
        self.stats: dict[str, Any] = {
            "cycles"            : 0,
            "enriched_total"    : 0,
            "failed_total"      : 0,
            "alert_total"       : 0,
            "predict_scored"    : 0,
            "last_avail"        : None,
            "last_faults"       : 0,
            "last_alarms"       : 0,
            "last_cycle_ts"     : None,
        }

    def stop(self) -> None:
        """Signal the thread to stop after the current cycle."""
        self._stop_evt.set()

    def run(self) -> None:
        """Main pipeline loop."""
        LOG.info("MasterPipelineThread started")

        while not self._stop_evt.is_set():
            try:
                self._cycle += 1
                self._run_one_cycle()
            except Exception as exc:
                self._errors += 1
                LOG.error("Pipeline cycle %d ERROR: %s", self._cycle, exc)

            self._stop_evt.wait(timeout=self._interval)

        LOG.info("MasterPipelineThread stopped (errors=%d)", self._errors)

    def _run_one_cycle(self) -> None:
        """Execute one complete pipeline cycle and update all consumers."""

        # ── Phase 4 + Phase 5 + Phase 7 (via router) ─────────────────────
        result      = self._core.run_cycle()
        enriched    = result["enriched"]
        cycle_num   = result["cycle_number"]

        # ── Phase 6: Dashboard store ──────────────────────────────────────
        self._dash_store.update(enriched, cycle_count=cycle_num)

        # ── Phase 10: Predictive AI ───────────────────────────────────────
        predictions: list[PredictionResult] = []
        if self._predict is not None:
            predictions = self._predict.ingest(enriched)
            scored = sum(1 for p in predictions if p.model_status == "SCORED")
            self.stats["predict_scored"] += scored

        # ── Phase 11: Mobile store ────────────────────────────────────────
        recent_alerts = [
            MobileAlert.from_alert_event(e)
            for e in self._alert_svc.journal().query(limit=30)
        ]
        risk_cards = [
            MobileRisk.from_prediction(p)
            for p in predictions
            if p.model_status in ("SCORED", "BASELINE")
        ]
        fleet = build_mobile_fleet(
            records = enriched,
            cycle   = cycle_num,
            site_id = self._core.site_id,
            risks   = predictions,
        )
        mobile_cards = sorted(
            [MobileCard.from_enriched(r) for r in enriched],
            key=lambda c: c.machine_id,
        )
        self._mobile_store.update(
            fleet   = fleet,
            cards   = mobile_cards,
            alerts  = recent_alerts,
            risks   = risk_cards,
            cycle   = cycle_num,
        )

        # ── Push CRITICAL notifications ───────────────────────────────────
        self._notif.push_critical_predictions(predictions)

        # ── Update stats dict ─────────────────────────────────────────────
        self.stats["cycles"]            = cycle_num
        self.stats["enriched_total"]   += len(enriched)
        self.stats["failed_total"]     += len(result.get("failed_norm", []))
        self.stats["last_avail"]        = result.get("fleet_availability")
        self.stats["last_faults"]       = result.get("status_dist", {}).get("FAULT", 0)
        self.stats["last_alarms"]       = sum(
            1 for r in enriched if r.alarm_code != 0
        )
        self.stats["last_cycle_ts"]     = datetime.now(timezone.utc).isoformat()

        j_stats = self._alert_svc.journal().stats()
        self.stats["alert_total"]       = j_stats.get("total", 0)


# =============================================================================
# SECTION 7 — TERMINAL STATUS DISPLAY
# =============================================================================

def _color_status(status: str) -> str:
    """Return ANSI-colored status string."""
    palette = {
        "RUN"           : ANSI_GREEN,
        "IDLE"          : ANSI_YELLOW,
        "FAULT"         : ANSI_RED,
        "MAINTENANCE"   : ANSI_ORANGE,
    }
    col = palette.get(status, ANSI_DIM)
    return f"{col}{status}{ANSI_RESET}"


def _fmt_avail(avail: Optional[float]) -> str:
    """Format fleet availability with color coding."""
    if avail is None:
        return f"{ANSI_DIM}N/A{ANSI_RESET}"
    pct = avail * 100
    if pct >= 85:
        col = ANSI_GREEN
    elif pct >= 70:
        col = ANSI_YELLOW
    else:
        col = ANSI_RED
    return f"{col}{pct:.1f}%{ANSI_RESET}"


def print_status_block(
    pipeline    : MasterPipelineThread,
    dash_url    : Optional[str],
    mobile_url  : Optional[str],
    alert_svc   : AlertService,
    predict     : Optional[PredictService],
    storage_dry : bool,
    started_at  : float,
) -> None:
    """
    Print a formatted live status block to stdout.

    Clears the terminal and redraws the full status panel.

    Args:
        pipeline    : Master pipeline thread for stats.
        dash_url    : Dashboard URL string or None if disabled.
        mobile_url  : Mobile URL string or None if disabled.
        alert_svc   : AlertService for journal stats.
        predict     : PredictService for AI stats or None.
        storage_dry : True if storage is in dry-run mode.
        started_at  : time.monotonic() of system start.
    """
    s       = pipeline.stats
    uptime  = time.monotonic() - started_at
    hh      = int(uptime // 3600)
    mm      = int((uptime % 3600) // 60)
    ss      = int(uptime % 60)

    j_stats = alert_svc.journal().stats()
    by_lvl  = j_stats.get("by_level", {})

    # AI status
    if predict is not None:
        ai_trained  = predict.engine().is_trained
        ai_scored   = s["predict_scored"]
        ai_str      = (
            f"{ANSI_GREEN}ACTIVE{ANSI_RESET} · scored={ai_scored}"
            if ai_trained
            else f"{ANSI_YELLOW}WARMING UP{ANSI_RESET}"
        )
    else:
        ai_str = f"{ANSI_DIM}DISABLED{ANSI_RESET}"

    storage_str = (
        f"{ANSI_YELLOW}DRY-RUN{ANSI_RESET}"
        if storage_dry
        else f"{ANSI_GREEN}LIVE{ANSI_RESET}"
    )

    print(ANSI_CLEAR, end="")
    print(
        f"{ANSI_BOLD}{ANSI_CYAN}"
        f"╔══════════════════════════════════════════════════════════════╗\n"
        f"║  MyFactoryInsight  ·  System Status  ·  v{VERSION:<24}║\n"
        f"╚══════════════════════════════════════════════════════════════╝"
        f"{ANSI_RESET}"
    )

    print(f"\n  {ANSI_DIM}Uptime :{ANSI_RESET} {hh:02d}:{mm:02d}:{ss:02d}  "
          f"{ANSI_DIM}Site :{ANSI_RESET} {pipeline._core.site_id}")

    print(f"\n{ANSI_BOLD}  ── Pipeline ──{ANSI_RESET}")
    print(f"  Cycles        : {ANSI_CYAN}{s['cycles']}{ANSI_RESET}")
    print(f"  Fleet avail   : {_fmt_avail(s['last_avail'])}")
    print(f"  Faults active : "
          f"{'  ' + ANSI_RED + str(s['last_faults']) + ANSI_RESET if s['last_faults'] else ANSI_GREEN + '0' + ANSI_RESET}")
    print(f"  Active alarms : "
          f"{'  ' + ANSI_YELLOW + str(s['last_alarms']) + ANSI_RESET if s['last_alarms'] else ANSI_GREEN + '0' + ANSI_RESET}")
    print(f"  Enriched total: {s['enriched_total']:,}")
    print(f"  Failed norm   : "
          f"{'  ' + ANSI_YELLOW + str(s['failed_total']) + ANSI_RESET if s['failed_total'] else ANSI_GREEN + '0' + ANSI_RESET}")

    print(f"\n{ANSI_BOLD}  ── Alerts ──{ANSI_RESET}")
    print(f"  Total events  : {s['alert_total']}")
    print(f"  CRITICAL      : {ANSI_RED}{by_lvl.get('CRITICAL', 0)}{ANSI_RESET}")
    print(f"  WARN          : {ANSI_YELLOW}{by_lvl.get('WARN', 0)}{ANSI_RESET}")
    print(f"  Unacknowledged: {ANSI_ORANGE}{j_stats.get('unacknowledged', 0)}{ANSI_RESET}")

    print(f"\n{ANSI_BOLD}  ── Services ──{ANSI_RESET}")
    print(f"  AI Predict    : {ai_str}")
    print(f"  Storage       : {storage_str}")

    print(f"\n{ANSI_BOLD}  ── Endpoints ──{ANSI_RESET}")
    if dash_url:
        print(f"  Dashboard     : {ANSI_CYAN}{dash_url}{ANSI_RESET}")
    else:
        print(f"  Dashboard     : {ANSI_DIM}disabled{ANSI_RESET}")
    if mobile_url:
        print(f"  Mobile PWA    : {ANSI_CYAN}{mobile_url}{ANSI_RESET}")
        print(f"  Mobile WS     : {ANSI_DIM}ws://{MOBILE_HOST}:{MOBILE_PORT}/mobile/ws{ANSI_RESET}")
    else:
        print(f"  Mobile        : {ANSI_DIM}disabled{ANSI_RESET}")

    ts = s.get("last_cycle_ts", "—")
    if ts and ts != "—":
        try:
            dt = datetime.fromisoformat(ts)
            ts = dt.strftime("%H:%M:%S UTC")
        except ValueError:
            pass
    print(f"\n  {ANSI_DIM}Last cycle: {ts}  ·  Ctrl+C to stop{ANSI_RESET}\n")


# =============================================================================
# SECTION 8 — PORT CHECK UTILITY
# =============================================================================

def check_port(host: str, port: int) -> bool:
    """
    Check whether a TCP port is accepting connections.

    Args:
        host : Target host.
        port : Target port.

    Returns:
        True if port is open, False otherwise.
    """
    import socket
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def print_status_check() -> None:
    """
    --status mode: check which MFI ports are currently open and print results.
    Does NOT start any services.
    """
    checks = [
        ("Dashboard (Phase 6)", DASHBOARD_HOST, DASHBOARD_PORT, f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}"),
        ("Mobile API (Phase 11)", MOBILE_HOST, MOBILE_PORT, f"http://{MOBILE_HOST}:{MOBILE_PORT}/mobile"),
    ]
    print(f"\n{ANSI_BOLD}{ANSI_CYAN}MyFactoryInsight — Port Status{ANSI_RESET}\n")
    for name, host, port, url in checks:
        if check_port(host, port):
            print(f"  {ANSI_GREEN}●{ANSI_RESET}  {name:<25} {ANSI_CYAN}{url}{ANSI_RESET}")
        else:
            print(f"  {ANSI_DIM}○{ANSI_RESET}  {name:<25} {ANSI_DIM}not running{ANSI_RESET}")
    print()


# =============================================================================
# SECTION 9 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for mfi_main.py. Validates:
      1.  All phase imports resolve successfully.
      2.  MFICore initializes (site=mecanitec).
      3.  AlertService initializes and registers handler.
      4.  PredictService initializes.
      5.  MFIStorageService initializes (dry_run=True).
      6.  PipelineStore (Phase 6) initializes.
      7.  MobileStore (Phase 11) initializes.
      8.  NotificationService initializes.
      9.  One MFICore pipeline cycle completes.
      10. PipelineStore receives enriched records.
      11. MobileStore receives fleet summary.
      12. AlertService journal is queryable.
      13. PredictService ingests records without error.
      14. StorageService registers ROUTE_MQTT and ROUTE_INFLUX handlers.
      15. ROUTE_ALERT is replaced by AlertService.
      16. Dashboard FastAPI app creates successfully.
      17. Mobile FastAPI app creates successfully.
      18. Dashboard GET /api/health returns 200 via TestClient.
      19. Mobile GET /mobile/api/health returns 200 via TestClient.
      20. MasterPipelineThread runs 2 cycles and updates all stores.

    Returns:
        True if all assertions pass, False otherwise.
    """
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

    # ── Tests 1-2: imports and core init ──────────────────────────────────
    check("All phase imports resolved", True)   # reaching here means OK

    core = MFICore(site_id="mecanitec")
    check("MFICore initializes", core is not None)

    # ── Tests 3-8: service initialization ────────────────────────────────
    alert_svc   = AlertService()
    check("AlertService initializes", alert_svc is not None)

    alert_svc.register_handler(core.router())
    check("AlertService registers ROUTE_ALERT handler", not core.router().is_stub(ROUTE_ALERT))

    predict_svc = PredictService(baseline_cycles=2, site_id="mecanitec")
    check("PredictService initializes", predict_svc is not None)

    storage_svc = MFIStorageService(dry_run=True)
    check("MFIStorageService initializes (dry_run)", storage_svc is not None)

    storage_svc.register_handlers(core.router())
    from mfi_phase_04_core import ROUTE_MQTT, ROUTE_INFLUX
    check("StorageService registers ROUTE_MQTT",   not core.router().is_stub(ROUTE_MQTT))
    check("StorageService registers ROUTE_INFLUX", not core.router().is_stub(ROUTE_INFLUX))

    dash_store      = PipelineStore()
    mobile_store    = MobileStore()
    notif_svc       = NotificationService()
    check("PipelineStore initializes",    dash_store.snapshot()[0] == [])
    check("MobileStore initializes",      not mobile_store.is_ready())
    check("NotificationService initializes", notif_svc is not None)

    # ── Tests 9-13: one manual pipeline cycle ────────────────────────────
    result = core.run_cycle()
    check(
        "MFICore.run_cycle() completes",
        "enriched" in result and len(result["enriched"]) >= 40,
        f"enriched={len(result.get('enriched', []))}",
    )

    dash_store.update(result["enriched"], cycle_count=result["cycle_number"])
    check("PipelineStore receives enriched records", len(dash_store.snapshot()[0]) >= 40)

    predictions = predict_svc.ingest(result["enriched"])
    check("PredictService ingests records", len(predictions) == len(result["enriched"]))

    fleet = build_mobile_fleet(
        records = result["enriched"],
        cycle   = result["cycle_number"],
        site_id = "mecanitec",
        risks   = predictions,
    )
    mobile_cards = [MobileCard.from_enriched(r) for r in result["enriched"]]
    mobile_store.update(fleet=fleet, cards=mobile_cards, alerts=[], risks=[], cycle=1)
    check("MobileStore receives fleet summary", mobile_store.is_ready())

    check(
        "AlertService journal is queryable",
        isinstance(alert_svc.journal().stats(), dict),
    )

    # ── Tests 16-19: FastAPI apps + TestClient ────────────────────────────
    dash_app    = create_dashboard(dash_store)
    mobile_app  = create_mobile_app(mobile_store, WebSocketManager(), notif_svc)

    check("Dashboard FastAPI app creates", dash_app is not None)
    check("Mobile FastAPI app creates",    mobile_app is not None)

    dash_client     = TestClient(dash_app)
    mobile_client   = TestClient(mobile_app)

    r = dash_client.get("/api/health")
    check("Dashboard GET /api/health → 200", r.status_code == 200)

    r = mobile_client.get("/mobile/api/health")
    check("Mobile GET /mobile/api/health → 200", r.status_code == 200)

    # ── Test 20: MasterPipelineThread ────────────────────────────────────
    core2       = MFICore(site_id="mecanitec")
    dash2       = PipelineStore()
    mob2        = MobileStore()
    alert2      = AlertService()
    predict2    = PredictService(baseline_cycles=1, site_id="mecanitec")
    notif2      = NotificationService()
    alert2.register_handler(core2.router())

    pipeline = MasterPipelineThread(
        core            = core2,
        dash_store      = dash2,
        mobile_store    = mob2,
        alert_service   = alert2,
        predict_service = predict2,
        notif_service   = notif2,
        interval        = 0.0,
    )
    pipeline.start()
    deadline = time.monotonic() + 10.0
    while pipeline.stats["cycles"] < 2 and time.monotonic() < deadline:
        time.sleep(0.2)
    pipeline.stop()
    pipeline.join(timeout=3.0)

    check(
        "MasterPipelineThread runs 2 cycles",
        pipeline.stats["cycles"] >= 2,
        f"cycles={pipeline.stats['cycles']}",
    )
    check(
        "MobileStore populated by thread",
        mob2.is_ready(),
    )

    # --- Summary ---
    total = passed + failed
    LOG.info(
        "══════════ SELF-TEST RESULT: %d/%d PASSED %s ══════════",
        passed, total,
        "✓ OK" if failed == 0 else "✗ FAIL",
    )
    return failed == 0


# =============================================================================
# SECTION 10 — MFI SYSTEM ORCHESTRATOR
# =============================================================================

class MFISystem:
    """
    MFI System — top-level orchestrator.

    Initializes all services, starts the pipeline thread, starts the
    uvicorn servers for dashboard and mobile, and runs the status display
    loop until Ctrl+C or stop() is called.
    """

    def __init__(
        self,
        site_id         : str   = DEFAULT_SITE,
        interval        : float = PIPELINE_INTERVAL,
        baseline_cycles : int   = BASELINE_CYCLES,
        dry_run         : bool  = False,
        enable_dashboard: bool  = True,
        enable_mobile   : bool  = True,
        enable_predict  : bool  = True,
        enable_storage  : bool  = True,
        dash_host       : str   = DASHBOARD_HOST,
        dash_port       : int   = DASHBOARD_PORT,
        mobile_host     : str   = MOBILE_HOST,
        mobile_port     : int   = MOBILE_PORT,
    ) -> None:
        self._site_id           = site_id
        self._interval          = interval
        self._dry_run           = dry_run
        self._enable_dashboard  = enable_dashboard
        self._enable_mobile     = enable_mobile
        self._enable_predict    = enable_predict
        self._enable_storage    = enable_storage
        self._dash_host         = dash_host
        self._dash_port         = dash_port
        self._mobile_host       = mobile_host
        self._mobile_port       = mobile_port

        # Services (populated in start())
        self._core          : Optional[MFICore]             = None
        self._alert_svc     : Optional[AlertService]        = None
        self._predict_svc   : Optional[PredictService]      = None
        self._storage_svc   : Optional[MFIStorageService]   = None
        self._dash_store    : Optional[PipelineStore]       = None
        self._mobile_store  : Optional[MobileStore]         = None
        self._notif_svc     : Optional[NotificationService] = None
        self._pipeline      : Optional[MasterPipelineThread]= None
        self._dash_server   : Optional[UvicornThread]       = None
        self._mobile_server : Optional[UvicornThread]       = None
        self._started_at    : float                         = 0.0

    # ── Initialization ────────────────────────────────────────────────────

    def _init_core(self) -> None:
        """Initialize Phase 4 MFICore and Phase 5/7/10 services."""
        LOG.info("Initializing MFICore (site=%s)...", self._site_id)
        self._core = MFICore(site_id=self._site_id)

        # Phase 7: Alert engine
        LOG.info("Initializing AlertService...")
        self._alert_svc = AlertService()
        self._alert_svc.register_handler(self._core.router())

        # Phase 10: Predictive AI (optional)
        if self._enable_predict:
            LOG.info("Initializing PredictService (baseline=%d cycles)...", BASELINE_CYCLES)
            self._predict_svc = PredictService(
                baseline_cycles = BASELINE_CYCLES,
                site_id         = self._site_id,
            )
        else:
            LOG.info("PredictService disabled.")

        # Phase 5: MQTT + InfluxDB (optional)
        if self._enable_storage:
            LOG.info(
                "Initializing StorageService (dry_run=%s)...", self._dry_run,
            )
            self._storage_svc = MFIStorageService(dry_run=self._dry_run)
            self._storage_svc.connect()
            self._storage_svc.register_handlers(self._core.router())
        else:
            LOG.info("StorageService disabled.")

    def _init_stores(self) -> None:
        """Initialize Phase 6 and Phase 11 data stores."""
        self._dash_store    = PipelineStore()
        self._mobile_store  = MobileStore()
        self._notif_svc     = NotificationService()

    def _init_pipeline(self) -> None:
        """Create and start the master pipeline thread."""
        self._pipeline = MasterPipelineThread(
            core            = self._core,
            dash_store      = self._dash_store,
            mobile_store    = self._mobile_store,
            alert_service   = self._alert_svc,
            predict_service = self._predict_svc,
            notif_service   = self._notif_svc,
            interval        = self._interval,
        )
        self._pipeline.start()

        # Wait for first data before starting servers
        LOG.info("Waiting for first pipeline cycle...")
        deadline = time.monotonic() + 15.0
        while self._dash_store.snapshot()[0] == [] and time.monotonic() < deadline:
            time.sleep(0.2)
        if self._dash_store.snapshot()[0] == []:
            LOG.error("Pipeline did not produce data within 15s — continuing anyway.")
        else:
            LOG.info("Pipeline ready (cycle 1 complete).")

    def _init_servers(self) -> None:
        """Start uvicorn servers for dashboard and mobile."""
        if self._enable_dashboard:
            dash_app = create_dashboard(self._dash_store)
            self._dash_server = UvicornThread(
                app     = dash_app,
                host    = self._dash_host,
                port    = self._dash_port,
                name    = "DashboardServer",
            )
            self._dash_server.start()

        if self._enable_mobile:
            ws_mgr      = WebSocketManager()
            mobile_app  = create_mobile_app(
                self._mobile_store, ws_mgr, self._notif_svc
            )
            self._mobile_server = UvicornThread(
                app     = mobile_app,
                host    = self._mobile_host,
                port    = self._mobile_port,
                name    = "MobileServer",
            )
            self._mobile_server.start()

        if self._enable_dashboard or self._enable_mobile:
            time.sleep(SERVER_STARTUP_WAIT)

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        """Initialize and start all MFI components."""
        self._started_at = time.monotonic()

        LOG.info("╔══════════════════════════════════════════════╗")
        LOG.info("║  MyFactoryInsight  │  Master Orchestrator     ║")
        LOG.info("║  Version %-7s   │  Site: %-18s  ║", VERSION, self._site_id)
        LOG.info("╚══════════════════════════════════════════════╝")

        self._init_core()
        self._init_stores()
        self._init_pipeline()
        self._init_servers()

        LOG.info("MFI system started.")
        if self._enable_dashboard:
            LOG.info(
                "Dashboard → http://%s:%d",
                self._dash_host, self._dash_port,
            )
        if self._enable_mobile:
            LOG.info(
                "Mobile    → http://%s:%d/mobile",
                self._mobile_host, self._mobile_port,
            )

    def run_status_loop(self) -> None:
        """
        Blocking status loop — prints a live terminal panel and handles Ctrl+C.

        Exits cleanly when interrupted.
        """
        dash_url    = (
            f"http://{self._dash_host}:{self._dash_port}"
            if self._enable_dashboard else None
        )
        mobile_url  = (
            f"http://{self._mobile_host}:{self._mobile_port}/mobile"
            if self._enable_mobile else None
        )

        try:
            while True:
                print_status_block(
                    pipeline    = self._pipeline,
                    dash_url    = dash_url,
                    mobile_url  = mobile_url,
                    alert_svc   = self._alert_svc,
                    predict     = self._predict_svc,
                    storage_dry = self._dry_run,
                    started_at  = self._started_at,
                )
                time.sleep(STATUS_DISPLAY_INTERVAL)

        except KeyboardInterrupt:
            print(f"\n{ANSI_YELLOW}Ctrl+C detected — shutting down...{ANSI_RESET}\n")

    def stop(self) -> None:
        """Stop all components in reverse order."""
        LOG.info("Stopping MFI system...")

        if self._pipeline:
            self._pipeline.stop()
            self._pipeline.join(timeout=5.0)

        if self._dash_server:
            self._dash_server.stop()

        if self._mobile_server:
            self._mobile_server.stop()

        if self._storage_svc:
            self._storage_svc.close()

        # Final stats
        if self._pipeline:
            s = self._pipeline.stats
            LOG.info(
                "Final stats │ cycles=%d │ enriched=%d │ alerts=%d │ predict_scored=%d",
                s["cycles"],
                s["enriched_total"],
                s["alert_total"],
                s["predict_scored"],
            )

        LOG.info("MFI system stopped.")


# =============================================================================
# SECTION 11 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for mfi_main.py."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_main.py",
        description = (
            f"MyFactoryInsight v{VERSION} — Master Orchestrator\n"
            "Starts all MFI phases as a single integrated system."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
        epilog = (
            "Examples:\n"
            "  python mfi_main.py                    # Start full system\n"
            "  python mfi_main.py --dry-run          # No MQTT/InfluxDB\n"
            "  python mfi_main.py --no-mobile        # Dashboard only\n"
            "  python mfi_main.py --no-dashboard     # Mobile only\n"
            "  python mfi_main.py --self-test        # Validate imports\n"
            "  python mfi_main.py --status           # Check running ports\n"
        ),
    )
    parser.add_argument(
        "--self-test",
        action  = "store_true",
        help    = "Run built-in self-test suite and exit.",
    )
    parser.add_argument(
        "--status",
        action  = "store_true",
        help    = "Check which MFI ports are currently open and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action  = "store_true",
        help    = "Run MQTT/InfluxDB in dry-run mode (no broker/DB required).",
    )
    parser.add_argument(
        "--no-dashboard",
        action  = "store_true",
        help    = "Disable Phase 6 dashboard (port 8000).",
    )
    parser.add_argument(
        "--no-mobile",
        action  = "store_true",
        help    = "Disable Phase 11 mobile API (port 8001).",
    )
    parser.add_argument(
        "--no-predict",
        action  = "store_true",
        help    = "Disable Phase 10 predictive AI.",
    )
    parser.add_argument(
        "--no-storage",
        action  = "store_true",
        help    = "Disable Phase 5 MQTT/InfluxDB entirely.",
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
    parser.add_argument(
        "--dash-port",
        type    = int,
        default = DASHBOARD_PORT,
        help    = f"Dashboard server port (default: {DASHBOARD_PORT}).",
    )
    parser.add_argument(
        "--mobile-port",
        type    = int,
        default = MOBILE_PORT,
        help    = f"Mobile server port (default: {MOBILE_PORT}).",
    )
    parser.add_argument(
        "--no-color",
        action  = "store_true",
        help    = "Disable ANSI color codes in terminal output.",
    )
    return parser


def main() -> None:
    """
    mfi_main entry point.

    Modes:
      --self-test : Validate all imports and run integration checks. Exit.
      --status    : Check running ports. Exit.
      (default)   : Start full MFI system, run status loop, shutdown on Ctrl+C.
    """
    global ANSI_RESET, ANSI_BOLD, ANSI_CYAN, ANSI_GREEN
    global ANSI_YELLOW, ANSI_RED, ANSI_ORANGE, ANSI_DIM, ANSI_CLEAR

    parser  = build_arg_parser()
    args    = parser.parse_args()

    # Disable ANSI if requested or if output is not a TTY
    if args.no_color or not sys.stdout.isatty():
        ANSI_RESET = ANSI_BOLD = ANSI_CYAN = ANSI_GREEN = ""
        ANSI_YELLOW = ANSI_RED = ANSI_ORANGE = ANSI_DIM = ANSI_CLEAR = ""

    # ── Self-test mode ────────────────────────────────────────────────────
    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)

    # ── Status check mode ─────────────────────────────────────────────────
    if args.status:
        print_status_check()
        sys.exit(0)

    # ── Full system mode ──────────────────────────────────────────────────
    system = MFISystem(
        site_id         = args.site,
        interval        = args.interval,
        baseline_cycles = BASELINE_CYCLES,
        dry_run         = args.dry_run,
        enable_dashboard= not args.no_dashboard,
        enable_mobile   = not args.no_mobile,
        enable_predict  = not args.no_predict,
        enable_storage  = not args.no_storage,
        dash_port       = args.dash_port,
        mobile_port     = args.mobile_port,
    )

    # Handle SIGTERM (Docker / systemd)
    def _sigterm_handler(sig, frame) -> None:
        LOG.info("SIGTERM received — stopping system.")
        system.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sigterm_handler)

    try:
        system.start()
        system.run_status_loop()
    finally:
        system.stop()


# =============================================================================
# SECTION 12 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
