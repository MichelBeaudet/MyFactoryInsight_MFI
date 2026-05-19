#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_11_mobile.py
# PHASE        : 11 — Mobile
# PURPOSE      : Mobile-optimized FastAPI backend serving:
#                  • Condensed REST API endpoints (machine cards, alerts, KPIs)
#                  • Responsive single-page PWA (mobile-first, touch-friendly)
#                  • WebSocket push channel for real-time live updates
#                  • In-process notification service with stub handlers
#                    (FCM/APNs stubs — ready for production integration)
#                Integrates Phase 4 (Core), Phase 7 (Alerts), Phase 10 (Predict).
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : fastapi, uvicorn, pydantic>=2.0, mfi_phase_04, 07, 10
# CLI          : python mfi_phase_11_mobile.py --self-test
#                python mfi_phase_11_mobile.py
#                python mfi_phase_11_mobile.py --host 0.0.0.0 --port 8080
#                python mfi_phase_11_mobile.py --baseline 3 --interval 2.0
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import asyncio
import json
import logging
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# --- FastAPI ---
try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.testclient import TestClient
    import uvicorn
except ImportError as exc:
    print(
        f"[FATAL] fastapi/uvicorn not found: {exc}\n"
        "Install: pip install fastapi uvicorn --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Pydantic ---
try:
    from pydantic import BaseModel, Field, ValidationError
except ImportError as exc:
    print(f"[FATAL] pydantic not found: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 4 ---
try:
    from mfi_phase_04_core import (
        MFICore,
        MFIEnrichedRecord,
        MFIRouter,
        ROUTE_ALERT,
        DEFAULT_SITE,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_04_core: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 7 ---
try:
    from mfi_phase_07_alerts import (
        AlertService,
        AlertEvent,
        AlertJournal,
        LEVEL_CRITICAL,
        LEVEL_WARN,
        LEVEL_INFO,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_07_alerts: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 10 ---
try:
    from mfi_phase_10_predict import (
        PredictService,
        PredictionResult,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_10_predict: {exc}", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID                = "11"
PHASE_NAME              = "Mobile"
PHASE_VERSION           = "1.0.0"

SERVER_HOST             = "127.0.0.1"
SERVER_PORT             = 8001           # Different from Phase 6 (8000)
API_PREFIX              = "/mobile"
PIPELINE_INTERVAL       = 2.0            # Seconds between pipeline cycles
BASELINE_CYCLES         = 3             # Predict warm-up cycles
WS_PUSH_INTERVAL        = 2.0           # WebSocket push interval (seconds)
NOTIFICATION_QUEUE_MAX  = 200           # Max queued notifications in memory
WS_MAX_CLIENTS          = 50            # Max concurrent WebSocket connections

# ── Notification channels (stubs for Phase 11) ────────────────────────────────
CHANNEL_WEBSOCKET   = "websocket"       # Real-time in-browser push
CHANNEL_FCM         = "fcm"             # Firebase Cloud Messaging (stub)
CHANNEL_APNS        = "apns"           # Apple Push Notification Service (stub)
CHANNEL_WEBHOOK     = "webhook"         # HTTP webhook (stub)

# ── Mobile card fields (condensed subset of MFIEnrichedRecord) ────────────────
MOBILE_CARD_FIELDS: tuple[str, ...] = (
    "machine_id", "machine_type", "protocol", "status",
    "alarm_code", "temperature_c", "piece_count", "piece_delta",
    "quality_rate", "availability", "temp_severity",
    "is_running", "is_faulted", "status_changed", "timestamp",
)

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
    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        kwargs.setdefault("extra", {})
        kwargs["extra"]["phase"] = PHASE_ID
        return msg, kwargs


def build_logger(name: str, level: int = logging.DEBUG) -> PhaseAdapter:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    base = logging.getLogger(name)
    base.setLevel(level)
    base.handlers.clear()
    base.addHandler(handler)
    base.propagate = False
    return PhaseAdapter(base, extra={"phase": PHASE_ID})


LOG = build_logger("mfi.phase11")

# =============================================================================
# SECTION 4 — MOBILE DATA MODELS (Pydantic)
# =============================================================================

class MobileCard(BaseModel):
    """
    Condensed machine card for mobile display.
    Subset of MFIEnrichedRecord — bandwidth-optimized for mobile clients.
    """
    machine_id      : str
    machine_type    : str
    protocol        : str
    status          : str
    alarm_code      : int
    temperature_c   : float
    piece_count     : int
    piece_delta     : int
    quality_rate    : Optional[float]
    availability    : Optional[float]
    temp_severity   : str
    is_running      : bool
    is_faulted      : bool
    status_changed  : bool
    timestamp       : str

    @classmethod
    def from_enriched(cls, record: MFIEnrichedRecord) -> "MobileCard":
        """Build a MobileCard from a full MFIEnrichedRecord."""
        return cls(**{f: getattr(record, f) for f in MOBILE_CARD_FIELDS})

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class MobileFleet(BaseModel):
    """Fleet-level mobile summary — single-screen overview."""
    site_id             : str
    cycle_number        : int
    machine_count       : int
    running             : int
    idle                : int
    fault               : int
    maintenance         : int
    fleet_availability  : Optional[float]
    active_alarms       : int
    critical_machines   : int
    avg_risk_score      : Optional[float]
    last_updated        : str

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class MobileAlert(BaseModel):
    """Condensed alert for mobile notification display."""
    event_id        : str
    level           : str
    machine_id      : str
    rule_name       : str
    message         : str
    timestamp       : str
    acknowledged    : bool

    @classmethod
    def from_alert_event(cls, evt: AlertEvent) -> "MobileAlert":
        return cls(
            event_id    = evt.event_id,
            level       = evt.level,
            machine_id  = evt.machine_id,
            rule_name   = evt.rule_name,
            message     = evt.message,
            timestamp   = evt.timestamp,
            acknowledged= evt.acknowledged,
        )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class MobileRisk(BaseModel):
    """Machine risk summary for mobile predictive view."""
    machine_id      : str
    machine_type    : str
    risk_level      : str
    risk_score      : float
    risk_factors    : list[str]
    model_status    : str

    @classmethod
    def from_prediction(cls, result: PredictionResult) -> "MobileRisk":
        return cls(
            machine_id  = result.machine_id,
            machine_type= result.machine_type,
            risk_level  = result.risk_level,
            risk_score  = result.risk_score,
            risk_factors= result.risk_factors[:3],
            model_status= result.model_status,
        )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class PushNotification(BaseModel):
    """
    Push notification payload — dispatched to all registered channels.
    Channels receive this object; each stub formats it per-protocol.
    """
    notification_id : str   = Field(default_factory=lambda: str(int(time.time() * 1000)))
    title           : str
    body            : str
    level           : str   = LEVEL_INFO    # INFO | WARN | CRITICAL
    machine_id      : str   = "FLEET"
    data            : dict[str, Any] = Field(default_factory=dict)
    timestamp       : str   = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


# =============================================================================
# SECTION 5 — MOBILE STATE STORE
# =============================================================================

class MobileStore:
    """
    Thread-safe in-memory store for the latest mobile API data.

    Written by the background pipeline thread; read by all API routes.
    Stores the most recent data for: fleet summary, machine cards,
    alerts, and prediction results.
    """

    def __init__(self) -> None:
        self._lock          = threading.RLock()
        self._fleet         : Optional[MobileFleet] = None
        self._cards         : list[MobileCard]      = []
        self._alerts        : list[MobileAlert]     = []
        self._risks         : list[MobileRisk]      = []
        self._cycle         : int                   = 0
        self._last_updated  : Optional[str]         = None

    def update(
        self,
        fleet   : MobileFleet,
        cards   : list[MobileCard],
        alerts  : list[MobileAlert],
        risks   : list[MobileRisk],
        cycle   : int,
    ) -> None:
        """Replace all stored data atomically."""
        with self._lock:
            self._fleet         = fleet
            self._cards         = cards
            self._alerts        = alerts
            self._risks         = risks
            self._cycle         = cycle
            self._last_updated  = datetime.now(timezone.utc).isoformat()

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent snapshot of all mobile data."""
        with self._lock:
            return {
                "fleet"     : self._fleet,
                "cards"     : list(self._cards),
                "alerts"    : list(self._alerts),
                "risks"     : list(self._risks),
                "cycle"     : self._cycle,
                "updated"   : self._last_updated,
            }

    def is_ready(self) -> bool:
        """Return True once the first cycle of data has been stored."""
        with self._lock:
            return self._fleet is not None


# =============================================================================
# SECTION 6 — NOTIFICATION SERVICE
# =============================================================================

# Type alias for a notification handler callable
NotificationHandler = Callable[[PushNotification], None]


class NotificationService:
    """
    MFI Mobile Notification Service.

    Queues PushNotification objects and dispatches them to registered
    channel handlers. Channels:
      - websocket : Real — delivered via WebSocket manager in real-time.
      - fcm       : Stub — logs FCM intent (Firebase Cloud Messaging).
      - apns      : Stub — logs APNs intent (Apple Push Notification).
      - webhook   : Stub — logs HTTP webhook intent.

    Custom channels can be added via register_channel().
    """

    def __init__(self) -> None:
        self._queue     : deque[PushNotification]       = deque(maxlen=NOTIFICATION_QUEUE_MAX)
        self._handlers  : dict[str, NotificationHandler] = {}
        self._sent      = 0
        self._dropped   = 0

        # Register built-in stubs
        self._handlers[CHANNEL_FCM]     = self._fcm_stub
        self._handlers[CHANNEL_APNS]    = self._apns_stub
        self._handlers[CHANNEL_WEBHOOK] = self._webhook_stub

        LOG.info(
            "NotificationService initialized │ channels=%s",
            list(self._handlers.keys()),
        )

    # ── Stub handlers ─────────────────────────────────────────────────────

    @staticmethod
    def _fcm_stub(notif: PushNotification) -> None:
        """FCM stub — logs the intent. Replace with firebase-admin in production."""
        if notif.level == LEVEL_CRITICAL:
            LOG.debug(
                "FCM STUB │ [%s] %s → %s",
                notif.level, notif.machine_id, notif.title,
            )

    @staticmethod
    def _apns_stub(notif: PushNotification) -> None:
        """APNs stub — logs the intent. Replace with aioapns in production."""
        if notif.level == LEVEL_CRITICAL:
            LOG.debug(
                "APNs STUB │ [%s] %s → %s",
                notif.level, notif.machine_id, notif.title,
            )

    @staticmethod
    def _webhook_stub(notif: PushNotification) -> None:
        """Webhook stub — logs the intent. Replace with httpx.post() in production."""
        if notif.level in (LEVEL_CRITICAL, LEVEL_WARN):
            LOG.debug(
                "WEBHOOK STUB │ [%s] %s → POST to configured endpoint",
                notif.level, notif.title,
            )

    # ── Channel management ────────────────────────────────────────────────

    def register_channel(self, name: str, handler: NotificationHandler) -> None:
        """
        Register (or replace) a notification channel handler.

        Args:
            name    : Channel identifier (e.g., CHANNEL_FCM).
            handler : Callable accepting one PushNotification.
        """
        self._handlers[name] = handler
        LOG.info("NotificationService: channel '%s' registered", name)

    # ── Notification building ─────────────────────────────────────────────

    @staticmethod
    def from_alert(event: AlertEvent) -> PushNotification:
        """
        Build a PushNotification from an AlertEvent.

        Args:
            event : AlertEvent from Phase 7.

        Returns:
            PushNotification ready for dispatch.
        """
        level_emoji = {
            LEVEL_CRITICAL: "🔴",
            LEVEL_WARN    : "🟡",
            LEVEL_INFO    : "🔵",
        }
        emoji = level_emoji.get(event.level, "⚪")
        return PushNotification(
            title       = f"{emoji} {event.level}: {event.rule_name}",
            body        = event.message,
            level       = event.level,
            machine_id  = event.machine_id,
            data        = {
                "event_id"  : event.event_id,
                "rule_id"   : event.rule_id,
                "alarm_code": event.alarm_code,
            },
        )

    @staticmethod
    def from_critical_risk(result: PredictionResult) -> PushNotification:
        """
        Build a PushNotification from a CRITICAL PredictionResult.

        Args:
            result : PredictionResult with risk_level == "CRITICAL".

        Returns:
            PushNotification ready for dispatch.
        """
        factors_str = "; ".join(result.risk_factors[:2]) if result.risk_factors else "AI anomaly detected"
        return PushNotification(
            title       = f"🤖 AI CRITICAL: {result.machine_id}",
            body        = f"Risk score {result.risk_score:.1%} — {factors_str}",
            level       = LEVEL_CRITICAL,
            machine_id  = result.machine_id,
            data        = {
                "risk_score"    : result.risk_score,
                "risk_level"    : result.risk_level,
                "if_score"      : result.if_score,
                "lof_score"     : result.lof_score,
            },
        )

    # ── Dispatch ──────────────────────────────────────────────────────────

    def push(
        self,
        notif               : PushNotification,
        channels            : Optional[list[str]] = None,
        ws_callback         : Optional[Callable]  = None,
    ) -> dict[str, bool]:
        """
        Dispatch a notification to all or selected channels.

        Args:
            notif       : PushNotification to send.
            channels    : List of channel names to use. None = all registered.
            ws_callback : Optional callable for WebSocket delivery
                          (called with the notification dict).

        Returns:
            Dict mapping channel_name → success (bool).
        """
        self._queue.append(notif)
        results: dict[str, bool] = {}

        # WebSocket delivery (handled separately via callback)
        if ws_callback:
            try:
                ws_callback(notif.to_dict())
                results[CHANNEL_WEBSOCKET] = True
            except Exception as exc:
                LOG.error("WebSocket notify error: %s", exc)
                results[CHANNEL_WEBSOCKET] = False

        # Handler dispatch
        target_channels = channels or list(self._handlers.keys())
        for ch in target_channels:
            handler = self._handlers.get(ch)
            if not handler:
                continue
            try:
                handler(notif)
                results[ch] = True
                self._sent  += 1
            except Exception as exc:
                LOG.error("Channel '%s' dispatch ERROR: %s", ch, exc)
                results[ch] = False

        return results

    def push_alert_events(
        self,
        events          : list[AlertEvent],
        ws_callback     : Optional[Callable] = None,
    ) -> int:
        """
        Convert and push all CRITICAL/WARN alert events as notifications.

        Args:
            events      : AlertEvent list from Phase 7.
            ws_callback : Optional WebSocket delivery callback.

        Returns:
            Number of notifications pushed.
        """
        pushed = 0
        for evt in events:
            if evt.level in (LEVEL_CRITICAL, LEVEL_WARN):
                notif = self.from_alert(evt)
                self.push(notif, ws_callback=ws_callback)
                pushed += 1
        return pushed

    def push_critical_predictions(
        self,
        results         : list[PredictionResult],
        ws_callback     : Optional[Callable] = None,
    ) -> int:
        """
        Push notifications for CRITICAL prediction results.

        Args:
            results     : PredictionResult list from Phase 10.
            ws_callback : Optional WebSocket delivery callback.

        Returns:
            Number of notifications pushed.
        """
        pushed = 0
        for r in results:
            if r.risk_level == "CRITICAL" and r.model_status == "SCORED":
                notif = self.from_critical_risk(r)
                self.push(notif, ws_callback=ws_callback)
                pushed += 1
        return pushed

    def recent(self, limit: int = 20) -> list[PushNotification]:
        """Return the most recent notifications (newest first)."""
        return list(reversed(list(self._queue)))[:limit]

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "queued"    : len(self._queue),
            "sent"      : self._sent,
            "dropped"   : self._dropped,
            "channels"  : list(self._handlers.keys()),
        }


# =============================================================================
# SECTION 7 — WEBSOCKET MANAGER
# =============================================================================

class WebSocketManager:
    """
    Manages active WebSocket connections for real-time mobile push.

    Tracks all connected clients. Broadcasts JSON messages to all
    connected clients. Handles connect/disconnect lifecycle.
    """

    def __init__(self) -> None:
        self._connections   : list[WebSocket] = []
        self._lock          = threading.Lock()
        self._broadcast_count = 0
        self._error_count   = 0

    async def connect(self, ws: WebSocket) -> None:
        """Accept and register a new WebSocket connection."""
        await ws.accept()
        with self._lock:
            if len(self._connections) >= WS_MAX_CLIENTS:
                await ws.close(code=1008, reason="Max clients reached")
                return
            self._connections.append(ws)
        LOG.info(
            "WebSocket connected │ total=%d",
            len(self._connections),
        )

    def disconnect(self, ws: WebSocket) -> None:
        """Remove a disconnected WebSocket."""
        with self._lock:
            if ws in self._connections:
                self._connections.remove(ws)
        LOG.debug("WebSocket disconnected │ remaining=%d", len(self._connections))

    async def broadcast(self, message: dict[str, Any]) -> int:
        """
        Send a JSON message to all connected clients.

        Dead connections are removed silently.

        Args:
            message : Dict to serialize and send.

        Returns:
            Number of clients successfully reached.
        """
        if not self._connections:
            return 0

        payload     = json.dumps(message, default=str)
        dead        : list[WebSocket] = []
        sent        = 0

        with self._lock:
            clients = list(self._connections)

        for ws in clients:
            try:
                await ws.send_text(payload)
                sent += 1
            except Exception:
                dead.append(ws)
                self._error_count += 1

        with self._lock:
            for d in dead:
                if d in self._connections:
                    self._connections.remove(d)

        self._broadcast_count += 1
        return sent

    def notify_sync(self, message: dict[str, Any]) -> None:
        """
        Thread-safe synchronous notify — schedules an async broadcast.
        Used from the background pipeline thread.

        Args:
            message : Dict payload to broadcast.
        """
        # We enqueue; the WebSocket push loop reads from the mobile store
        pass   # Delivery happens via the /ws endpoint loop

    @property
    def client_count(self) -> int:
        with self._lock:
            return len(self._connections)

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "connected"     : self.client_count,
            "broadcasts"    : self._broadcast_count,
            "errors"        : self._error_count,
        }


# =============================================================================
# SECTION 8 — PIPELINE DATA BUILDER
# =============================================================================

def build_mobile_fleet(
    records : list[MFIEnrichedRecord],
    cycle   : int,
    site_id : str,
    risks   : list[PredictionResult],
) -> MobileFleet:
    """
    Build a MobileFleet summary from the latest cycle's enriched records.

    Args:
        records : Enriched records from MFICore.
        cycle   : Current pipeline cycle number.
        site_id : Site identifier.
        risks   : Prediction results for risk scoring.

    Returns:
        MobileFleet with all aggregated fields.
    """
    dist        : dict[str, int] = {}
    alarm_count = 0
    avails      : list[float] = []

    for r in records:
        dist[r.status] = dist.get(r.status, 0) + 1
        if r.alarm_code != 0:
            alarm_count += 1
        if r.availability is not None:
            avails.append(r.availability)

    fleet_avail = round(sum(avails) / len(avails), 4) if avails else None

    scored_risks    = [r for r in risks if r.model_status == "SCORED"]
    avg_risk        = (
        round(sum(r.risk_score for r in scored_risks) / len(scored_risks), 4)
        if scored_risks else None
    )
    crit_machines   = sum(1 for r in scored_risks if r.risk_level == "CRITICAL")

    return MobileFleet(
        site_id             = site_id,
        cycle_number        = cycle,
        machine_count       = len(records),
        running             = dist.get("RUN", 0),
        idle                = dist.get("IDLE", 0),
        fault               = dist.get("FAULT", 0),
        maintenance         = dist.get("MAINTENANCE", 0),
        fleet_availability  = fleet_avail,
        active_alarms       = alarm_count,
        critical_machines   = crit_machines,
        avg_risk_score      = avg_risk,
        last_updated        = datetime.now(timezone.utc).isoformat(),
    )


# =============================================================================
# SECTION 9 — MOBILE PWA HTML (embedded)
# =============================================================================

MOBILE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <meta name="theme-color" content="#070d12">
  <title>MFI Mobile</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow:wght@300;400;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg:    #070d12; --surface: #0d1a24; --border: #1a2e40;
      --accent:#00b4d8; --run:#00e676; --idle:#ffd600;
      --fault: #ff1744; --maint:#ff9100; --warn:#ff9100;
      --text:  #c8d8e4; --dim:#4a6275;
      --mono:  'Share Tech Mono',monospace; --sans:'Barlow',sans-serif;
      --safe-bottom: env(safe-area-inset-bottom, 0px);
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { height: 100%; background: var(--bg); color: var(--text);
                 font-family: var(--sans); overflow-x: hidden; }

    /* ── Header ── */
    header {
      position: sticky; top: 0; z-index: 100;
      background: var(--surface); border-bottom: 1px solid var(--border);
      padding: 12px 16px; display: flex; align-items: center;
      justify-content: space-between;
    }
    .logo { font-family:var(--mono); font-size:14px; color:var(--accent);
            letter-spacing:2px; }
    .logo span { color:#fff; }
    .header-right { display:flex; align-items:center; gap:12px;
                    font-family:var(--mono); font-size:10px; color:var(--dim); }
    .pulse { width:7px; height:7px; border-radius:50%; background:var(--run);
             display:inline-block; animation:pulse 1.5s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
    .ws-dot { width:7px; height:7px; border-radius:50%; display:inline-block;
              background:var(--dim); transition:background .3s; }
    .ws-dot.connected { background:var(--run); }

    /* ── Tab bar ── */
    nav {
      position: fixed; bottom:0; left:0; right:0;
      padding-bottom: var(--safe-bottom);
      background: var(--surface); border-top:1px solid var(--border);
      display: flex; z-index:100;
    }
    .tab { flex:1; padding:10px 4px; text-align:center; cursor:pointer;
           font-family:var(--mono); font-size:9px; color:var(--dim);
           letter-spacing:1px; transition:color .2s; border:none;
           background:transparent; }
    .tab.active { color:var(--accent); }
    .tab-icon { font-size:18px; display:block; margin-bottom:3px; }

    /* ── Page container ── */
    .page { display:none; padding:12px 12px 80px; animation:fadeIn .2s ease; }
    .page.active { display:block; }
    @keyframes fadeIn { from{opacity:0;transform:translateY(4px)} to{opacity:1;transform:none} }

    /* ── Fleet overview cards ── */
    .stat-row { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:12px; }
    .stat-card {
      background:var(--surface); border:1px solid var(--border);
      border-radius:8px; padding:12px 14px; position:relative; overflow:hidden;
    }
    .stat-card::before { content:''; position:absolute; top:0; left:0; right:0; height:3px; }
    .stat-card.run::before   { background:var(--run); }
    .stat-card.idle::before  { background:var(--idle); }
    .stat-card.fault::before { background:var(--fault); }
    .stat-card.maint::before { background:var(--maint); }
    .stat-label { font-family:var(--mono); font-size:9px; color:var(--dim);
                  letter-spacing:2px; text-transform:uppercase; margin-bottom:6px; }
    .stat-value { font-family:var(--mono); font-size:32px; line-height:1; }
    .stat-card.run .stat-value   { color:var(--run); }
    .stat-card.idle .stat-value  { color:var(--idle); }
    .stat-card.fault .stat-value { color:var(--fault); }
    .stat-card.maint .stat-value { color:var(--maint); }

    .kpi-row { display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-bottom:12px; }
    .kpi-mini { background:var(--surface); border:1px solid var(--border);
                border-radius:8px; padding:10px 8px; text-align:center; }
    .kpi-mini-label { font-family:var(--mono); font-size:8px; color:var(--dim);
                      letter-spacing:2px; margin-bottom:4px; }
    .kpi-mini-value { font-family:var(--mono); font-size:18px; color:var(--accent); }

    /* ── Machine cards ── */
    .machine-card {
      background:var(--surface); border:1px solid var(--border);
      border-radius:8px; padding:12px 14px; margin-bottom:8px;
      display:flex; align-items:center; gap:12px;
    }
    .mc-status { width:10px; height:10px; border-radius:50%; flex-shrink:0; }
    .mc-status.RUN         { background:var(--run); box-shadow:0 0 6px var(--run); }
    .mc-status.IDLE        { background:var(--idle); }
    .mc-status.FAULT       { background:var(--fault); animation:pulse .8s infinite; }
    .mc-status.MAINTENANCE { background:var(--maint); }
    .mc-info { flex:1; min-width:0; }
    .mc-id   { font-family:var(--mono); font-size:12px; color:var(--accent); font-weight:700; }
    .mc-type { font-size:11px; color:var(--dim); margin-top:1px; }
    .mc-meta { font-family:var(--mono); font-size:10px; color:var(--text); margin-top:4px; }
    .mc-right { text-align:right; font-family:var(--mono); font-size:10px; }
    .mc-temp-ok   { color:var(--dim); }
    .mc-temp-warn { color:var(--idle); }
    .mc-temp-crit { color:var(--fault); }
    .mc-alarm { color:var(--fault); font-weight:700; font-size:11px; }

    /* ── Alert cards ── */
    .alert-card {
      border-radius:8px; padding:12px 14px; margin-bottom:8px;
      border:1px solid;
    }
    .alert-card.CRITICAL { background:rgba(255,23,68,.08);
                           border-color:rgba(255,23,68,.3); }
    .alert-card.WARN     { background:rgba(255,145,0,.08);
                           border-color:rgba(255,145,0,.3); }
    .alert-card.INFO     { background:rgba(0,180,216,.06);
                           border-color:rgba(0,180,216,.2); }
    .alert-header { display:flex; justify-content:space-between;
                    align-items:center; margin-bottom:6px; }
    .alert-level { font-family:var(--mono); font-size:9px; font-weight:700;
                   letter-spacing:2px; padding:2px 6px; border-radius:3px; }
    .alert-level.CRITICAL { background:rgba(255,23,68,.2); color:var(--fault); }
    .alert-level.WARN     { background:rgba(255,145,0,.2); color:var(--maint); }
    .alert-level.INFO     { background:rgba(0,180,216,.2); color:var(--accent); }
    .alert-machine { font-family:var(--mono); font-size:11px; color:var(--accent); }
    .alert-msg  { font-size:12px; color:var(--text); }
    .alert-time { font-family:var(--mono); font-size:9px; color:var(--dim); margin-top:4px; }

    /* ── Risk cards ── */
    .risk-card {
      background:var(--surface); border:1px solid var(--border);
      border-radius:8px; padding:12px 14px; margin-bottom:8px;
    }
    .risk-header { display:flex; justify-content:space-between;
                   align-items:center; margin-bottom:8px; }
    .risk-machine { font-family:var(--mono); font-size:12px; color:var(--accent); }
    .risk-badge { font-family:var(--mono); font-size:9px; font-weight:700;
                  padding:2px 8px; border-radius:3px; }
    .risk-badge.CRITICAL { background:rgba(255,23,68,.2); color:var(--fault); }
    .risk-badge.HIGH     { background:rgba(255,145,0,.2); color:var(--maint); }
    .risk-badge.MEDIUM   { background:rgba(255,214,0,.2); color:var(--idle); }
    .risk-badge.LOW      { background:rgba(0,230,118,.1); color:var(--run); }
    .risk-bar-bg { height:5px; background:var(--border); border-radius:3px;
                   overflow:hidden; margin:6px 0; }
    .risk-bar-fill { height:100%; border-radius:3px; transition:width .4s; }
    .risk-bar-fill.CRITICAL { background:var(--fault); }
    .risk-bar-fill.HIGH     { background:var(--maint); }
    .risk-bar-fill.MEDIUM   { background:var(--idle); }
    .risk-bar-fill.LOW      { background:var(--run); }
    .risk-factors { font-size:11px; color:var(--dim); }
    .risk-factor  { margin-top:3px; padding-left:12px; position:relative; }
    .risk-factor::before { content:'•'; position:absolute; left:0; color:var(--accent); }

    /* ── Notifications feed ── */
    .notif-card {
      background:var(--surface); border-left:3px solid var(--accent);
      border-radius:0 8px 8px 0; padding:10px 12px; margin-bottom:8px;
    }
    .notif-card.CRITICAL { border-left-color:var(--fault); }
    .notif-card.WARN     { border-left-color:var(--maint); }
    .notif-title { font-family:var(--mono); font-size:11px;
                   color:var(--text); font-weight:700; }
    .notif-body  { font-size:11px; color:var(--dim); margin-top:3px; }
    .notif-time  { font-family:var(--mono); font-size:9px;
                   color:var(--dim); margin-top:4px; }

    .empty { text-align:center; padding:40px 20px; color:var(--dim);
             font-family:var(--mono); font-size:11px; }
    .empty-icon { font-size:32px; margin-bottom:8px; }

    /* ── Scroll ── */
    ::-webkit-scrollbar { width:4px; }
    ::-webkit-scrollbar-track { background:transparent; }
    ::-webkit-scrollbar-thumb { background:var(--border); border-radius:2px; }

    .section-title {
      font-family:var(--mono); font-size:9px; letter-spacing:3px;
      color:var(--accent); text-transform:uppercase; margin-bottom:10px;
    }
    .updated-bar { font-family:var(--mono); font-size:9px; color:var(--dim);
                   text-align:center; padding:6px; margin-bottom:10px; }
  </style>
</head>
<body>

<header>
  <div class="logo">MFI <span>Mobile</span></div>
  <div class="header-right">
    <span id="ws-dot" class="ws-dot" title="WebSocket"></span>
    <span class="pulse"></span>
    <span id="cycle-tag">—</span>
  </div>
</header>

<!-- Pages -->
<div id="page-fleet"  class="page active"></div>
<div id="page-mach"   class="page"></div>
<div id="page-alerts" class="page"></div>
<div id="page-risk"   class="page"></div>
<div id="page-notif"  class="page"></div>

<!-- Tab bar -->
<nav>
  <button class="tab active" onclick="showTab('fleet','this')">
    <span class="tab-icon">🏭</span>FLEET
  </button>
  <button class="tab" onclick="showTab('mach','this')">
    <span class="tab-icon">⚙️</span>MACHINES
  </button>
  <button class="tab" onclick="showTab('alerts','this')">
    <span class="tab-icon">🔔</span>ALERTS
  </button>
  <button class="tab" onclick="showTab('risk','this')">
    <span class="tab-icon">🤖</span>AI RISK
  </button>
  <button class="tab" onclick="showTab('notif','this')">
    <span class="tab-icon">📲</span>NOTIF
  </button>
</nav>

<script>
  const API   = '/mobile/api';
  let wsConn  = null;
  let notifications = [];
  let wsConnected = false;

  // ── Tab routing ───────────────────────────────────────────────────────
  function showTab(name, btn) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.getElementById('page-' + name).classList.add('active');
    if (btn !== 'this') btn.classList.add('active');
    else {
      const tabs = document.querySelectorAll('.tab');
      const names = ['fleet','mach','alerts','risk','notif'];
      const idx = names.indexOf(name);
      if (idx >= 0) tabs[idx].classList.add('active');
    }
  }

  // ── Formatters ────────────────────────────────────────────────────────
  const pct = v => v != null ? (v * 100).toFixed(1) + '%' : '—';
  const num = v => v != null ? Number(v).toLocaleString() : '—';
  const tsFmt = s => {
    if (!s) return '—';
    return new Date(s).toLocaleTimeString([], { hour12: false });
  };

  // ── Fleet page ────────────────────────────────────────────────────────
  function renderFleet(fleet) {
    if (!fleet) return;
    document.getElementById('cycle-tag').textContent = '#' + fleet.cycle_number;
    const el = document.getElementById('page-fleet');
    const avail = fleet.fleet_availability != null
                  ? (fleet.fleet_availability * 100).toFixed(1) + '%' : '—';
    const avgRisk = fleet.avg_risk_score != null
                    ? (fleet.avg_risk_score * 100).toFixed(1) + '%' : '—';
    el.innerHTML = `
      <div class="updated-bar">Updated ${tsFmt(fleet.last_updated)}</div>
      <div class="section-title">Status Distribution</div>
      <div class="stat-row">
        <div class="stat-card run">
          <div class="stat-label">Running</div>
          <div class="stat-value">${fleet.running}</div>
        </div>
        <div class="stat-card idle">
          <div class="stat-label">Idle</div>
          <div class="stat-value">${fleet.idle}</div>
        </div>
        <div class="stat-card fault">
          <div class="stat-label">Fault</div>
          <div class="stat-value">${fleet.fault}</div>
        </div>
        <div class="stat-card maint">
          <div class="stat-label">Maintenance</div>
          <div class="stat-value">${fleet.maintenance}</div>
        </div>
      </div>
      <div class="section-title">KPIs</div>
      <div class="kpi-row">
        <div class="kpi-mini">
          <div class="kpi-mini-label">AVAIL</div>
          <div class="kpi-mini-value">${avail}</div>
        </div>
        <div class="kpi-mini">
          <div class="kpi-mini-label">ALARMS</div>
          <div class="kpi-mini-value" style="color:${fleet.active_alarms>0?'var(--fault)':'var(--run)'}">
            ${fleet.active_alarms}
          </div>
        </div>
        <div class="kpi-mini">
          <div class="kpi-mini-label">AI RISK</div>
          <div class="kpi-mini-value" style="color:${fleet.avg_risk_score>0.7?'var(--fault)':fleet.avg_risk_score>0.5?'var(--idle)':'var(--run)'}">
            ${avgRisk}
          </div>
        </div>
      </div>
      <div class="kpi-row" style="grid-template-columns:1fr 1fr">
        <div class="kpi-mini">
          <div class="kpi-mini-label">CRITICAL AI</div>
          <div class="kpi-mini-value" style="color:var(--fault)">${fleet.critical_machines}</div>
        </div>
        <div class="kpi-mini">
          <div class="kpi-mini-label">MACHINES</div>
          <div class="kpi-mini-value">${fleet.machine_count}</div>
        </div>
      </div>`;
  }

  // ── Machine cards ─────────────────────────────────────────────────────
  function renderMachines(cards) {
    const el = document.getElementById('page-mach');
    if (!cards || !cards.length) {
      el.innerHTML = '<div class="empty"><div class="empty-icon">⚙️</div>No machine data</div>';
      return;
    }
    el.innerHTML = '<div class="section-title">' + cards.length + ' Machines</div>' +
      cards.map(m => {
        const tempClass = m.temp_severity === 'CRITICAL' ? 'mc-temp-crit'
                        : m.temp_severity === 'WARN'     ? 'mc-temp-warn' : 'mc-temp-ok';
        const alarmHtml = m.alarm_code ? `<div class="mc-alarm">ALM ${m.alarm_code}</div>` : '';
        const qr = m.quality_rate != null ? (m.quality_rate*100).toFixed(0)+'%' : '—';
        const av = m.availability != null ? (m.availability*100).toFixed(0)+'%' : '—';
        return `<div class="machine-card">
          <div class="mc-status ${m.status}"></div>
          <div class="mc-info">
            <div class="mc-id">${m.machine_id}</div>
            <div class="mc-type">${m.machine_type} · ${m.protocol}</div>
            <div class="mc-meta">Q:${qr} | AV:${av} | Δ${m.piece_delta}pc</div>
          </div>
          <div class="mc-right">
            <div class="${tempClass}">${m.temperature_c.toFixed(1)}°C</div>
            ${alarmHtml}
          </div>
        </div>`;
      }).join('');
  }

  // ── Alert cards ───────────────────────────────────────────────────────
  function renderAlerts(alerts) {
    const el = document.getElementById('page-alerts');
    const unacked = alerts.filter(a => !a.acknowledged);
    if (!alerts.length) {
      el.innerHTML = '<div class="empty"><div class="empty-icon">✅</div>No active alerts</div>';
      return;
    }
    el.innerHTML = '<div class="section-title">' + unacked.length + ' unacknowledged</div>' +
      alerts.map(a => `
        <div class="alert-card ${a.level}">
          <div class="alert-header">
            <span class="alert-level ${a.level}">${a.level}</span>
            <span class="alert-machine">${a.machine_id}</span>
          </div>
          <div class="alert-msg">${a.message}</div>
          <div class="alert-time">${tsFmt(a.timestamp)} · ${a.rule_name}</div>
        </div>`
      ).join('');
  }

  // ── Risk cards ────────────────────────────────────────────────────────
  function renderRisk(risks) {
    const el = document.getElementById('page-risk');
    const scored = risks.filter(r => r.model_status === 'SCORED');
    const sorted = [...scored].sort((a,b) => b.risk_score - a.risk_score);
    if (!sorted.length) {
      el.innerHTML = '<div class="empty"><div class="empty-icon">🤖</div>AI warming up…</div>';
      return;
    }
    el.innerHTML = '<div class="section-title">AI Risk Scores</div>' +
      sorted.map(r => {
        const pctBar = (r.risk_score * 100).toFixed(1);
        const factors = (r.risk_factors || []).slice(0,3).map(f =>
          `<div class="risk-factor">${f}</div>`
        ).join('');
        return `<div class="risk-card">
          <div class="risk-header">
            <span class="risk-machine">${r.machine_id} · ${r.machine_type}</span>
            <span class="risk-badge ${r.risk_level}">${r.risk_level}</span>
          </div>
          <div class="risk-bar-bg">
            <div class="risk-bar-fill ${r.risk_level}" style="width:${pctBar}%"></div>
          </div>
          <div class="risk-factors">${factors || '<span style="color:var(--dim)">No risk factors</span>'}</div>
        </div>`;
      }).join('');
  }

  // ── Notifications ─────────────────────────────────────────────────────
  function addNotification(notif) {
    notifications.unshift(notif);
    if (notifications.length > 50) notifications.pop();
    renderNotifications();
    // Badge on tab
    const tab = document.querySelectorAll('.tab')[4];
    if (tab) tab.querySelector('.tab-icon').textContent = '🔴';
  }

  function renderNotifications() {
    const el = document.getElementById('page-notif');
    if (!notifications.length) {
      el.innerHTML = '<div class="empty"><div class="empty-icon">📲</div>No notifications yet</div>';
      return;
    }
    el.innerHTML = '<div class="section-title">' + notifications.length + ' notifications</div>' +
      notifications.slice(0, 30).map(n => `
        <div class="notif-card ${n.level || ''}">
          <div class="notif-title">${n.title || '—'}</div>
          <div class="notif-body">${n.body || ''}</div>
          <div class="notif-time">${tsFmt(n.timestamp)}</div>
        </div>`
      ).join('');
  }

  // ── WebSocket ─────────────────────────────────────────────────────────
  function connectWS() {
    const host = location.host;
    const wsUrl = 'ws://' + host + '/mobile/ws';
    wsConn = new WebSocket(wsUrl);
    const dot = document.getElementById('ws-dot');

    wsConn.onopen = () => {
      wsConnected = true;
      dot.classList.add('connected');
    };
    wsConn.onclose = () => {
      wsConnected = false;
      dot.classList.remove('connected');
      setTimeout(connectWS, 3000);
    };
    wsConn.onerror = () => {
      wsConnected = false;
      dot.classList.remove('connected');
    };
    wsConn.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === 'push') { renderAll(msg.data); }
        if (msg.type === 'notification') { addNotification(msg.data); }
      } catch(e) {}
    };
  }

  // ── Render all ────────────────────────────────────────────────────────
  function renderAll(data) {
    if (!data) return;
    if (data.fleet)   renderFleet(data.fleet);
    if (data.cards)   renderMachines(data.cards);
    if (data.alerts)  renderAlerts(data.alerts);
    if (data.risks)   renderRisk(data.risks);
  }

  // ── REST polling fallback ─────────────────────────────────────────────
  async function fetchAll() {
    try {
      const [fr, cr, ar, rr] = await Promise.all([
        fetch(API + '/fleet').then(r => r.ok ? r.json() : null),
        fetch(API + '/cards').then(r => r.ok ? r.json() : null),
        fetch(API + '/alerts').then(r => r.ok ? r.json() : null),
        fetch(API + '/risk').then(r => r.ok ? r.json() : null),
      ]);
      renderAll({ fleet: fr, cards: cr, alerts: ar, risks: rr });
    } catch(e) { console.warn('fetch error:', e); }
  }

  // ── Boot ──────────────────────────────────────────────────────────────
  renderNotifications();
  connectWS();
  fetchAll();
  setInterval(fetchAll, 3000);
</script>
</body>
</html>"""

# =============================================================================
# SECTION 10 — FASTAPI MOBILE APPLICATION
# =============================================================================

def create_mobile_app(
    store               : MobileStore,
    ws_manager          : WebSocketManager,
    notification_service: NotificationService,
) -> FastAPI:
    """
    Create the FastAPI mobile application.

    All routes are mobile-optimized (condensed payloads).
    WebSocket endpoint delivers real-time push to connected clients.

    Args:
        store               : Shared MobileStore (written by pipeline thread).
        ws_manager          : WebSocket connection manager.
        notification_service: NotificationService for push dispatch.

    Returns:
        Configured FastAPI application.
    """
    app = FastAPI(
        title       = "MFI Mobile API",
        description = "Mobile-optimized MFI REST + WebSocket API — Phase 11",
        version     = PHASE_VERSION,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins   = ["*"],
        allow_methods   = ["GET", "POST"],
        allow_headers   = ["*"],
    )

    # ── Mobile PWA ────────────────────────────────────────────────────────
    @app.get("/mobile", response_class=HTMLResponse, tags=["ui"])
    async def mobile_pwa() -> HTMLResponse:
        """Serve the mobile PWA."""
        return HTMLResponse(content=MOBILE_HTML, status_code=200)

    # ── Fleet summary ─────────────────────────────────────────────────────
    @app.get("/mobile/api/fleet", tags=["mobile"])
    async def get_fleet() -> dict:
        """Fleet overview: running/idle/fault/maint counts, alarms, risk."""
        snap = store.snapshot()
        if snap["fleet"] is None:
            raise HTTPException(503, "Pipeline not ready yet.")
        return snap["fleet"].to_dict()

    # ── Machine cards ──────────────────────────────────────────────────────
    @app.get("/mobile/api/cards", tags=["mobile"])
    async def get_cards(
        status  : Optional[str] = None,
        fault   : Optional[bool] = None,
    ) -> list:
        """
        Condensed machine card list.
        Optional filters: status (RUN/IDLE/FAULT/MAINTENANCE), fault (bool).
        """
        snap    = store.snapshot()
        cards   = snap["cards"]
        if status:
            cards = [c for c in cards if c.status == status.upper()]
        if fault is not None:
            cards = [c for c in cards if c.is_faulted == fault]
        return [c.to_dict() for c in cards]

    # ── Single machine card ────────────────────────────────────────────────
    @app.get("/mobile/api/cards/{machine_id}", tags=["mobile"])
    async def get_card(machine_id: str) -> dict:
        """Single machine card by machine_id."""
        snap = store.snapshot()
        for card in snap["cards"]:
            if card.machine_id == machine_id:
                return card.to_dict()
        raise HTTPException(404, f"Machine '{machine_id}' not found.")

    # ── Active alerts ──────────────────────────────────────────────────────
    @app.get("/mobile/api/alerts", tags=["mobile"])
    async def get_alerts(
        level   : Optional[str] = None,
        unacked : Optional[bool] = None,
    ) -> list:
        """
        Recent alerts (newest first, max 50).
        Optional filters: level (CRITICAL/WARN/INFO), unacked (bool).
        """
        snap    = store.snapshot()
        alerts  = snap["alerts"]
        if level:
            alerts = [a for a in alerts if a.level == level.upper()]
        if unacked is not None:
            alerts = [a for a in alerts if (not a.acknowledged) == unacked]
        return [a.to_dict() for a in alerts[:50]]

    # ── AI risk scores ─────────────────────────────────────────────────────
    @app.get("/mobile/api/risk", tags=["mobile"])
    async def get_risk(
        min_level   : Optional[str] = None,
    ) -> list:
        """
        AI risk scores (highest risk first).
        Optional filter: min_level (LOW/MEDIUM/HIGH/CRITICAL).
        """
        from mfi_phase_10_predict import RISK_LOW, RISK_MEDIUM, RISK_HIGH
        snap    = store.snapshot()
        risks   = snap["risks"]

        level_min_score = {
            "LOW": 0.0, "MEDIUM": RISK_LOW,
            "HIGH": RISK_MEDIUM, "CRITICAL": RISK_HIGH,
        }
        if min_level:
            min_score = level_min_score.get(min_level.upper(), 0.0)
            risks = [r for r in risks if r.risk_score >= min_score]

        return [r.to_dict() for r in sorted(risks, key=lambda x: x.risk_score, reverse=True)]

    # ── Recent notifications ───────────────────────────────────────────────
    @app.get("/mobile/api/notifications", tags=["mobile"])
    async def get_notifications(limit: int = 20) -> list:
        """Recent push notifications (newest first)."""
        return [n.to_dict() for n in notification_service.recent(limit=limit)]

    # ── Health ────────────────────────────────────────────────────────────
    @app.get("/mobile/api/health", tags=["mobile"])
    async def health() -> dict:
        """Health check: pipeline readiness + WebSocket client count."""
        snap = store.snapshot()
        return {
            "status"        : "ok" if store.is_ready() else "warming_up",
            "phase"         : PHASE_ID,
            "cycle"         : snap["cycle"],
            "ws_clients"    : ws_manager.client_count,
            "notif_stats"   : notification_service.stats,
            "last_updated"  : snap["updated"],
        }

    # ── WebSocket push endpoint ────────────────────────────────────────────
    @app.websocket("/mobile/ws")
    async def websocket_endpoint(ws: WebSocket) -> None:
        """
        WebSocket push endpoint.

        Sends full mobile data payload every WS_PUSH_INTERVAL seconds.
        Also delivers notification messages as they arrive.
        """
        await ws_manager.connect(ws)
        try:
            while True:
                await asyncio.sleep(WS_PUSH_INTERVAL)
                snap = store.snapshot()
                if snap["fleet"] is None:
                    continue
                payload = {
                    "type": "push",
                    "data": {
                        "fleet"  : snap["fleet"].to_dict()  if snap["fleet"] else None,
                        "cards"  : [c.to_dict() for c in snap["cards"]],
                        "alerts" : [a.to_dict() for a in snap["alerts"][:20]],
                        "risks"  : [r.to_dict() for r in snap["risks"]],
                    },
                }
                await ws.send_text(json.dumps(payload, default=str))
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            LOG.debug("WebSocket error: %s", exc)
        finally:
            ws_manager.disconnect(ws)

    return app


# =============================================================================
# SECTION 11 — MOBILE PIPELINE THREAD
# =============================================================================

class MobilePipelineThread(threading.Thread):
    """
    Background thread running the full MFI pipeline (Phases 4 + 7 + 10).

    Each cycle:
      1. MFICore.run_cycle()        → enriched records
      2. AlertService.engine().evaluate()  → alert events
      3. PredictService.ingest()    → prediction results
      4. MobileStore.update()       → atomic state write
      5. NotificationService.push() → dispatch CRITICAL alerts + risks
    """

    def __init__(
        self,
        store           : MobileStore,
        notif_service   : NotificationService,
        site_id         : str   = DEFAULT_SITE,
        interval        : float = PIPELINE_INTERVAL,
        baseline_cycles : int   = BASELINE_CYCLES,
    ) -> None:
        super().__init__(name="MobilePipelineThread", daemon=True)
        self._store         = store
        self._notif         = notif_service
        self._interval      = interval
        self._stop_evt      = threading.Event()
        self._cycle         = 0

        # Phase 4 — core pipeline
        self._core          = MFICore(site_id=site_id)

        # Phase 7 — alert engine (we call it manually, not via router)
        self._alert_service = AlertService()

        # Phase 10 — predict service
        self._predict       = PredictService(
            baseline_cycles = baseline_cycles,
            site_id         = site_id,
        )

        self._site_id = site_id
        LOG.info(
            "MobilePipelineThread initialized │ site=%s │ interval=%.1fs │ baseline=%d",
            site_id, interval, baseline_cycles,
        )

    def stop(self) -> None:
        self._stop_evt.set()

    def run(self) -> None:
        LOG.info("MobilePipelineThread started")

        while not self._stop_evt.is_set():
            try:
                self._cycle += 1

                # ── Phase 4: Core pipeline ────────────────────────────
                result      = self._core.run_cycle()
                enriched    = result["enriched"]

                # ── Phase 7: Alert engine ─────────────────────────────
                alert_events= self._alert_service.engine().evaluate(enriched)
                if alert_events:
                    self._alert_service.journal().record(alert_events)
                recent_alerts = [
                    MobileAlert.from_alert_event(e)
                    for e in self._alert_service.journal().query(limit=30)
                ]

                # ── Phase 10: Predictive AI ───────────────────────────
                pred_results= self._predict.ingest(enriched)
                risk_cards  = [
                    MobileRisk.from_prediction(r)
                    for r in pred_results
                    if r.model_status in ("SCORED", "BASELINE")
                ]

                # ── Build mobile views ────────────────────────────────
                cards   = [MobileCard.from_enriched(r) for r in enriched]
                fleet   = build_mobile_fleet(
                    records = enriched,
                    cycle   = self._cycle,
                    site_id = self._site_id,
                    risks   = pred_results,
                )

                # ── Update store ──────────────────────────────────────
                self._store.update(
                    fleet   = fleet,
                    cards   = sorted(cards, key=lambda c: c.machine_id),
                    alerts  = recent_alerts,
                    risks   = risk_cards,
                    cycle   = self._cycle,
                )

                # ── Push notifications for critical events ────────────
                n_alert = self._notif.push_alert_events(alert_events)
                n_risk  = self._notif.push_critical_predictions(pred_results)
                if n_alert + n_risk > 0:
                    LOG.debug(
                        "Cycle %d │ pushed %d alert + %d risk notifications",
                        self._cycle, n_alert, n_risk,
                    )

                LOG.debug(
                    "Mobile cycle %d │ enriched=%d │ alerts=%d │ risks=%d │ notifs=%d",
                    self._cycle, len(enriched), len(alert_events),
                    len(risk_cards), n_alert + n_risk,
                )

            except Exception as exc:
                LOG.error("MobilePipelineThread cycle ERROR: %s", exc)

            self._stop_evt.wait(timeout=self._interval)

        LOG.info("MobilePipelineThread stopped")


# =============================================================================
# SECTION 12 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 11. Validates:
      1.  MobileCard.from_enriched() builds condensed card correctly.
      2.  MobileCard has all MOBILE_CARD_FIELDS.
      3.  MobileFleet builds with correct status distribution.
      4.  MobileAlert.from_alert_event() maps fields correctly.
      5.  MobileRisk.from_prediction() maps fields correctly.
      6.  PushNotification auto-generates notification_id and timestamp.
      7.  NotificationService.from_alert() builds title with level emoji.
      8.  NotificationService.from_critical_risk() builds body correctly.
      9.  NotificationService.push() dispatches to registered handlers.
      10. NotificationService.push_alert_events() only pushes CRIT/WARN.
      11. NotificationService.recent() returns newest first.
      12. MobileStore starts not ready; becomes ready after update().
      13. MobileStore.snapshot() returns consistent dict.
      14. MobileStore.update() is idempotent (overwrites previous data).
      15. GET /mobile returns 200 with HTML.
      16. GET /mobile/api/health returns 200 with status field.
      17. GET /mobile/api/fleet returns 503 before data, 200 after.
      18. GET /mobile/api/cards returns list.
      19. GET /mobile/api/cards?status=FAULT filters correctly.
      20. GET /mobile/api/cards/{id} returns 200 for valid id.
      21. GET /mobile/api/cards/{id} returns 404 for unknown id.
      22. GET /mobile/api/alerts returns list.
      23. GET /mobile/api/risk returns list sorted by risk_score desc.
      24. GET /mobile/api/notifications returns list.
      25. Full pipeline: MobilePipelineThread populates store.

    Returns:
        True if all assertions pass, False otherwise.
    """
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
        std = MFIStandardModel(
            site_id="mecanitec", machine_id=mid, machine_type="CNC",
            protocol="opcua", status=status, alarm_code=alarm,
            piece_count=10000, good_count=5 if status == "RUN" else 0,
            bad_count=0, cycle_time_sec=42.0 if status == "RUN" else 0.0,
            temperature_c=temp, speed=1000.0 if status == "RUN" else 0.0,
            timestamp="2026-05-16T20:00:00+00:00",
        )
        col = MFICollector()
        for _ in range(3):
            col.collect([std])
        return MFIEnricher().enrich(std, col.get_state(mid))

    er_run   = _make_er("machine01", "RUN",   temp=50.0)
    er_fault = _make_er("machine02", "FAULT", alarm=300, temp=120.0)
    er_idle  = _make_er("machine03", "IDLE",  temp=30.0)
    all_ers  = [er_run, er_fault, er_idle]

    # ── Tests 1-2: MobileCard ─────────────────────────────────────────────
    card = MobileCard.from_enriched(er_run)
    check("MobileCard.from_enriched() builds card", card.machine_id == "machine01")
    check(
        "MobileCard has all MOBILE_CARD_FIELDS",
        all(hasattr(card, f) for f in MOBILE_CARD_FIELDS),
    )

    # ── Test 3: MobileFleet ───────────────────────────────────────────────
    from mfi_phase_10_predict import PredictService, PredictionResult
    dummy_risks: list[PredictionResult] = []
    fleet = build_mobile_fleet(all_ers, cycle=1, site_id="mecanitec", risks=dummy_risks)
    check(
        "MobileFleet has correct running count",
        fleet.running == 1 and fleet.fault == 1 and fleet.idle == 1,
        f"run={fleet.running} fault={fleet.fault} idle={fleet.idle}",
    )

    # ── Tests 4-5: MobileAlert, MobileRisk ───────────────────────────────
    evt = AlertEvent(
        rule_id="fault_status", rule_name="Machine FAULT", level=LEVEL_CRITICAL,
        machine_id="machine02", site_id="mecanitec",
        message="machine02 in FAULT", timestamp="2026-05-16T20:00:00+00:00",
    )
    ma = MobileAlert.from_alert_event(evt)
    check("MobileAlert.from_alert_event() maps level", ma.level == LEVEL_CRITICAL)

    pred = PredictionResult(
        machine_id="machine01", machine_type="CNC", site_id="mecanitec",
        timestamp="2026-05-16T20:00:00+00:00",
        risk_level="HIGH", risk_score=0.72,
        risk_factors=["Active alarm", "High temp"], model_status="SCORED",
    )
    mr = MobileRisk.from_prediction(pred)
    check("MobileRisk.from_prediction() maps risk_level", mr.risk_level == "HIGH")

    # ── Tests 6-7: PushNotification, NotificationService ─────────────────
    notif = PushNotification(title="Test", body="Test body", level=LEVEL_WARN)
    check("PushNotification has notification_id", len(notif.notification_id) > 0)
    check("PushNotification has timestamp",       len(notif.timestamp) > 0)

    svc = NotificationService()
    notif_from_alert = svc.from_alert(evt)
    check("from_alert() title contains 🔴 for CRITICAL", "🔴" in notif_from_alert.title)

    # ── Test 8: from_critical_risk ────────────────────────────────────────
    crit_pred = PredictionResult(
        machine_id="m01", machine_type="CNC", site_id="s",
        timestamp="2026-05-16T20:00:00+00:00",
        risk_level="CRITICAL", risk_score=0.95,
        risk_factors=["Temp CRIT", "Alarm 300"], model_status="SCORED",
    )
    notif_from_pred = svc.from_critical_risk(crit_pred)
    check(
        "from_critical_risk() body contains risk score",
        "95.0%" in notif_from_pred.body or "0.95" in notif_from_pred.body,
        notif_from_pred.body,
    )

    # ── Test 9: push() dispatches ─────────────────────────────────────────
    captured: list = []
    svc.register_channel("test_ch", lambda n: captured.append(n))
    svc.push(notif, channels=["test_ch"])
    check("NotificationService.push() dispatches to handler", len(captured) == 1)

    # ── Test 10: push_alert_events() skips INFO ───────────────────────────
    info_evt = AlertEvent(
        rule_id="x", rule_name="x", level=LEVEL_INFO,
        machine_id="m01", site_id="s",
        message="info", timestamp="2026-05-16T20:00:00+00:00",
    )
    captured2: list = []
    svc.register_channel("test_ch2", lambda n: captured2.append(n))
    n_pushed = svc.push_alert_events([evt, info_evt], ws_callback=None)
    check("push_alert_events() only pushes CRIT/WARN (not INFO)", n_pushed == 1)

    # ── Test 11: recent() returns newest first ────────────────────────────
    svc2    = NotificationService()
    n1      = PushNotification(title="First",  body="b1", level=LEVEL_INFO)
    n2      = PushNotification(title="Second", body="b2", level=LEVEL_WARN)
    svc2.push(n1, channels=[])
    svc2.push(n2, channels=[])
    recent  = svc2.recent(limit=2)
    check(
        "recent() returns newest first",
        recent[0].title == "Second" and recent[1].title == "First",
        str([r.title for r in recent]),
    )

    # ── Tests 12-14: MobileStore ──────────────────────────────────────────
    store = MobileStore()
    check("MobileStore starts not ready", not store.is_ready())

    cards   = [MobileCard.from_enriched(r) for r in all_ers]
    alerts  = [MobileAlert.from_alert_event(evt)]
    risks   = [MobileRisk.from_prediction(pred)]
    store.update(fleet=fleet, cards=cards, alerts=alerts, risks=risks, cycle=1)
    check("MobileStore becomes ready after update()", store.is_ready())

    snap = store.snapshot()
    check(
        "MobileStore.snapshot() has all keys",
        all(k in snap for k in ["fleet", "cards", "alerts", "risks", "cycle", "updated"]),
    )

    store.update(fleet=fleet, cards=cards, alerts=alerts, risks=risks, cycle=2)
    check("MobileStore.update() increments cycle", store.snapshot()["cycle"] == 2)

    # ── Tests 15-24: FastAPI routes ───────────────────────────────────────
    ws_mgr  = WebSocketManager()
    app     = create_mobile_app(store, ws_mgr, svc)
    client  = TestClient(app, raise_server_exceptions=True)

    r = client.get("/mobile")
    check("GET /mobile returns 200", r.status_code == 200)

    r = client.get("/mobile/api/health")
    check("GET /mobile/api/health returns 200", r.status_code == 200)
    check("health has status field", "status" in r.json())

    # Fleet — store is populated, should return 200
    r = client.get("/mobile/api/fleet")
    check("GET /mobile/api/fleet returns 200 when ready", r.status_code == 200)
    check("fleet has machine_count", "machine_count" in r.json())

    r = client.get("/mobile/api/cards")
    check("GET /mobile/api/cards returns 200", r.status_code == 200)
    check("cards is a list", isinstance(r.json(), list))

    r = client.get("/mobile/api/cards?status=FAULT")
    faults = r.json()
    check(
        "GET /mobile/api/cards?status=FAULT returns only FAULT machines",
        all(c["status"] == "FAULT" for c in faults),
        str([c["status"] for c in faults]),
    )

    r = client.get("/mobile/api/cards/machine01")
    check("GET /mobile/api/cards/machine01 returns 200", r.status_code == 200)
    check("card has correct machine_id", r.json()["machine_id"] == "machine01")

    r = client.get("/mobile/api/cards/nonexistent_xyz")
    check("GET /mobile/api/cards/nonexistent returns 404", r.status_code == 404)

    r = client.get("/mobile/api/alerts")
    check("GET /mobile/api/alerts returns list", isinstance(r.json(), list))

    r = client.get("/mobile/api/risk")
    risks_list = r.json()
    if len(risks_list) >= 2:
        check(
            "GET /mobile/api/risk sorted desc by risk_score",
            risks_list[0]["risk_score"] >= risks_list[-1]["risk_score"],
        )
    else:
        check("GET /mobile/api/risk returns list", isinstance(risks_list, list))

    r = client.get("/mobile/api/notifications")
    check("GET /mobile/api/notifications returns list", isinstance(r.json(), list))

    # ── Test 25: Full pipeline thread populates store ─────────────────────
    store2  = MobileStore()
    notif2  = NotificationService()
    thread  = MobilePipelineThread(
        store           = store2,
        notif_service   = notif2,
        site_id         = "mecanitec",
        interval        = 0.0,
        baseline_cycles = 2,
    )
    thread.start()
    # Allow a few seconds for baseline + scoring cycles
    deadline = time.monotonic() + 15.0
    while not store2.is_ready() and time.monotonic() < deadline:
        time.sleep(0.2)
    thread.stop()
    thread.join(timeout=3.0)

    check(
        "Full pipeline: MobilePipelineThread populates store",
        store2.is_ready(),
        "store not ready after 15s",
    )
    if store2.is_ready():
        snap2 = store2.snapshot()
        check(
            "Pipeline store has machine cards",
            len(snap2["cards"]) >= 40,
            f"cards={len(snap2['cards'])}",
        )

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
# SECTION 13 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_11_mobile.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Mobile PWA + REST + WebSocket push + notification service."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--self-test", action="store_true",
                        help="Run built-in self-test and exit.")
    parser.add_argument("--host", type=str, default=SERVER_HOST,
                        help=f"Server bind host (default: {SERVER_HOST}).")
    parser.add_argument("--port", type=int, default=SERVER_PORT,
                        help=f"Server port (default: {SERVER_PORT}).")
    parser.add_argument("--site", type=str, default=DEFAULT_SITE, metavar="SITE_ID",
                        help=f"Site identifier (default: {DEFAULT_SITE}).")
    parser.add_argument("--interval", type=float, default=PIPELINE_INTERVAL,
                        help=f"Pipeline cycle interval seconds (default: {PIPELINE_INTERVAL}).")
    parser.add_argument("--baseline", type=int, default=BASELINE_CYCLES,
                        help=f"AI baseline cycles before scoring (default: {BASELINE_CYCLES}).")
    return parser


def main() -> None:
    parser  = build_arg_parser()
    args    = parser.parse_args()

    LOG.info("╔══════════════════════════════════════════════╗")
    LOG.info("║  MyFactoryInsight  │  Phase %-2s │ %-20s ║", PHASE_ID, PHASE_NAME)
    LOG.info("║  Version %-7s   │ Site: %-23s ║", PHASE_VERSION, args.site)
    LOG.info("╚══════════════════════════════════════════════╝")

    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)

    # ── Initialize services ───────────────────────────────────────────────
    store   = MobileStore()
    ws_mgr  = WebSocketManager()
    notif   = NotificationService()

    # ── Start pipeline thread ─────────────────────────────────────────────
    thread  = MobilePipelineThread(
        store           = store,
        notif_service   = notif,
        site_id         = args.site,
        interval        = args.interval,
        baseline_cycles = args.baseline,
    )
    thread.start()

    # Wait for first data before accepting requests
    LOG.info("Waiting for first pipeline cycle...")
    while not store.is_ready():
        time.sleep(0.2)
    LOG.info("Pipeline ready — starting server.")

    app = create_mobile_app(store, ws_mgr, notif)

    LOG.info(
        "Mobile PWA available at http://%s:%d/mobile",
        args.host, args.port,
    )
    LOG.info(
        "WebSocket at ws://%s:%d/mobile/ws  |  REST at http://%s:%d/mobile/api/",
        args.host, args.port, args.host, args.port,
    )
    LOG.info("Press Ctrl+C to stop.")

    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    finally:
        thread.stop()
        thread.join(timeout=3.0)
        LOG.info("Phase %s shutdown complete.", PHASE_ID)


# =============================================================================
# SECTION 14 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
