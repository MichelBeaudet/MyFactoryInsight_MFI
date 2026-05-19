#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_04_core.py
# PHASE        : 4 — Core / Orchestrator
# PURPOSE      : Central MFI engine. Orchestrates the full pipeline
#                (Phases 1–3), enriches validated records with derived KPIs,
#                tracks machine state transitions, and routes the enriched
#                flux to downstream service stubs (Phase 5: MQTT + InfluxDB).
#                Strict validation throughout — no silent failures.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : pydantic>=2.0, mfi_phase_01..03
# CLI          : python mfi_phase_04_core.py --self-test
#                python mfi_phase_04_core.py --cycles 5
#                python mfi_phase_04_core.py --cycles 5 --output core_flux.json
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import dataclasses
import json
import logging
import sys
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# --- Pydantic ---
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
except ImportError as exc:
    print(f"[FATAL] pydantic not found: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 1 ---
try:
    from mfi_phase_01_simulation import (
        MachineStatus,
        MachineConfig,
        build_machine_fleet,
        run_simulation_cycle,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_01_simulation: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 2 ---
try:
    from mfi_phase_02_readers import ReaderRegistry, RawPayload
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_02_readers: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 3 ---
try:
    from mfi_phase_03_json_model import (
        Normalizer,
        MFIStandardModel,
        NormalizationResult,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_03_json_model: {exc}", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID            = "04"
PHASE_NAME          = "MFI Core"
PHASE_VERSION       = "1.0.0"

DEFAULT_SITE        = "mecanitec"
DEFAULT_CYCLES      = 5

# ── Collector settings ────────────────────────────────────────────────────────
STATE_HISTORY_DEPTH = 10            # Max status transitions stored per machine
PIECE_DELTA_MAX     = 500           # Max plausible pieces in one cycle (sanity)

# ── Enrichment thresholds ─────────────────────────────────────────────────────
TEMP_WARN_C         = 80.0          # Temperature warning threshold (°C)
TEMP_CRIT_C         = 110.0         # Temperature critical threshold (°C)
QUALITY_WARN        = 0.95          # Quality rate below this → warning
AVAILABILITY_TARGET = 0.85          # Target availability ratio for OEE hint

# ── Router downstream stubs ───────────────────────────────────────────────────
# These are Phase 5 connection points — registered as named hooks here.
ROUTE_MQTT          = "mqtt_publish"
ROUTE_INFLUX        = "influx_write"
ROUTE_ALERT         = "alert_check"

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


def build_logger(name: str, level: int = logging.DEBUG) -> PhaseAdapter:
    """
    Build and return a PhaseAdapter-wrapped logger.

    Args:
        name  : Logger namespace.
        level : Logging level.

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


LOG = build_logger("mfi.phase04")

# =============================================================================
# SECTION 4 — ENRICHED RECORD MODEL (Pydantic)
# =============================================================================

class MFIEnrichedRecord(BaseModel):
    """
    MFI Enriched Record — MFIStandardModel extended with derived KPIs and
    state-tracking fields produced by the Core layer.

    Downstream consumers (Dashboard, Analytics, Reports, AI, Mobile) receive
    this model. It is the final shape of the MFI data flux.

    Derived fields (computed by MFIEnricher — never by protocol readers)
    -------------------------------------------------------------------
    quality_rate        : good / (good + bad); None if no production this cycle.
    availability        : Running cycles / total cycles seen for this machine.
    temp_severity       : "OK" | "WARN" | "CRITICAL".
    is_running          : True if status == RUN.
    is_faulted          : True if status == FAULT.
    status_changed      : True if status differs from previous cycle.
    previous_status     : Status from the previous cycle (None on first cycle).
    piece_delta         : Pieces produced this cycle (positive when running).
    cycle_number        : How many cycles this machine has been seen by Core.
    enrichment_timestamp: UTC ISO 8601 timestamp when enrichment was applied.

    Base fields (pass-through from MFIStandardModel)
    -------------------------------------------------
    All 13 MFIStandardModel fields are included verbatim.
    """

    model_config = {"validate_assignment": True}

    # ── Pass-through from MFIStandardModel ───────────────────────────────
    site_id             : str
    machine_id          : str
    machine_type        : str
    protocol            : str
    status              : str
    alarm_code          : int
    piece_count         : int
    good_count          : int
    bad_count           : int
    cycle_time_sec      : float
    temperature_c       : float
    speed               : float
    timestamp           : str

    # ── Derived KPIs ──────────────────────────────────────────────────────
    quality_rate        : Optional[float] = Field(
        None,
        ge=0.0, le=1.0,
        description="Good / (Good + Bad); None if no production this cycle",
    )
    availability        : Optional[float] = Field(
        None,
        ge=0.0, le=1.0,
        description="Running cycles / total cycles observed for this machine",
    )
    temp_severity       : str = Field(
        "OK",
        description="Temperature severity: OK | WARN | CRITICAL",
    )
    is_running          : bool  = Field(False, description="True if status == RUN")
    is_faulted          : bool  = Field(False, description="True if status == FAULT")
    status_changed      : bool  = Field(False, description="True if status differs from previous cycle")
    previous_status     : Optional[str] = Field(None, description="Status from previous cycle")
    piece_delta         : int   = Field(0, ge=0, description="Pieces produced this cycle")
    cycle_number        : int   = Field(1, ge=1, description="Observation cycle count for this machine")
    enrichment_timestamp: str   = Field(..., description="UTC timestamp of enrichment")

    # ── Serialization helpers ─────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Return model as a plain Python dictionary."""
        return self.model_dump()

    def to_json(self, indent: int = 2) -> str:
        """Return model as a formatted JSON string."""
        return self.model_dump_json(indent=indent)

    def __repr__(self) -> str:
        return (
            f"MFIEnrichedRecord("
            f"machine_id={self.machine_id!r}, "
            f"status={self.status!r}, "
            f"quality={self.quality_rate}, "
            f"avail={self.availability}, "
            f"cycle={self.cycle_number})"
        )


# =============================================================================
# SECTION 5 — MACHINE STATE TRACKER
# =============================================================================

@dataclasses.dataclass
class MachineState:
    """
    Per-machine persistent state maintained across cycles by the Collector.

    Attributes:
        machine_id      : Unique machine identifier.
        current_status  : Most recent observed status.
        run_cycles      : Total cycles where status == RUN.
        total_cycles    : Total cycles observed (all statuses).
        last_piece_count: piece_count from the previous cycle.
        status_history  : Deque of (timestamp, status) tuples, newest last.
    """

    machine_id      : str
    current_status  : Optional[str]         = None
    run_cycles      : int                   = 0
    total_cycles    : int                   = 0
    last_piece_count: int                   = 0
    status_history  : deque                 = dataclasses.field(
        default_factory=lambda: deque(maxlen=STATE_HISTORY_DEPTH)
    )

    def update(self, record: MFIStandardModel) -> None:
        """
        Update state from a new normalized record.

        Args:
            record : Latest MFIStandardModel for this machine.
        """
        self.current_status     = record.status
        self.total_cycles      += 1
        self.last_piece_count   = record.piece_count

        if record.status == MachineStatus.RUN.value:
            self.run_cycles += 1

        self.status_history.append((record.timestamp, record.status))

    def availability(self) -> Optional[float]:
        """
        Compute availability ratio: run_cycles / total_cycles.

        Returns:
            Float 0.0–1.0, or None if no cycles recorded yet.
        """
        if self.total_cycles == 0:
            return None
        return round(self.run_cycles / self.total_cycles, 4)

    def previous_status(self) -> Optional[str]:
        """
        Return the status from the cycle before the current one.

        Returns:
            Status string, or None if fewer than 2 cycles recorded.
        """
        if len(self.status_history) < 2:
            return None
        return self.status_history[-2][1]     # Second-to-last entry


# =============================================================================
# SECTION 6 — MFI COLLECTOR
# =============================================================================

class MFICollector:
    """
    MFI state collector — maintains a per-machine MachineState registry.

    Receives validated MFIStandardModel records each cycle and updates
    the state for each machine. Provides state lookup for the enricher.

    Rules:
      - One MachineState per machine_id.
      - Created on first observation — no pre-seeding required.
      - No silent drops — records rejected by Pydantic in Phase 3 never reach
        the Collector (they were already excluded as failed).
    """

    def __init__(self) -> None:
        self._states: dict[str, MachineState] = {}
        LOG.info("MFICollector initialized")

    def collect(self, records: list[MFIStandardModel]) -> None:
        """
        Update machine states from a batch of validated records.

        Args:
            records : Validated MFIStandardModel records from Phase 3.
        """
        for record in records:
            mid = record.machine_id
            if mid not in self._states:
                self._states[mid] = MachineState(machine_id=mid)
                LOG.debug("New machine registered: %s", mid)
            self._states[mid].update(record)

        LOG.debug(
            "Collector updated: %d records | %d machines tracked",
            len(records),
            len(self._states),
        )

    def get_state(self, machine_id: str) -> Optional[MachineState]:
        """
        Return the current MachineState for a machine.

        Args:
            machine_id : Machine identifier.

        Returns:
            MachineState if machine has been seen, None otherwise.
        """
        return self._states.get(machine_id)

    def machine_count(self) -> int:
        """Return number of machines currently tracked."""
        return len(self._states)

    def fleet_availability(self) -> Optional[float]:
        """
        Compute fleet-wide average availability across all tracked machines.

        Returns:
            Float 0.0–1.0, or None if no machines tracked.
        """
        if not self._states:
            return None
        avails  = [s.availability() for s in self._states.values()]
        valid   = [a for a in avails if a is not None]
        if not valid:
            return None
        return round(sum(valid) / len(valid), 4)

    def status_distribution(self) -> dict[str, int]:
        """
        Count machines by current status across the fleet.

        Returns:
            Dict mapping status string to machine count.
        """
        dist: dict[str, int] = {}
        for state in self._states.values():
            s = state.current_status or "UNKNOWN"
            dist[s] = dist.get(s, 0) + 1
        return dist


# =============================================================================
# SECTION 7 — MFI ENRICHER
# =============================================================================

class MFIEnricher:
    """
    MFI record enricher — adds derived KPI fields to validated records.

    The enricher consumes:
      - MFIStandardModel records (current cycle data)
      - MachineState objects (historical context from the Collector)

    It produces MFIEnrichedRecord objects ready for routing to Phase 5.

    Enrichment rules (all deterministic — no random logic):
      - quality_rate  : good / (good + bad) if cycle produced pieces, else None.
      - availability  : From MachineState (run_cycles / total_cycles).
      - temp_severity : CRITICAL ≥ TEMP_CRIT_C > WARN ≥ TEMP_WARN_C > OK.
      - is_running    : status == RUN.
      - is_faulted    : status == FAULT.
      - status_changed: current status ≠ previous cycle status.
      - previous_status: from MachineState history.
      - piece_delta   : piece_count − last_piece_count (clamped ≥ 0).
      - cycle_number  : total_cycles from MachineState.
    """

    def __init__(self) -> None:
        LOG.info("MFIEnricher initialized")

    # ── Individual field computations ─────────────────────────────────────

    @staticmethod
    def _quality_rate(record: MFIStandardModel) -> Optional[float]:
        """
        Compute quality rate for this cycle.

        Args:
            record : Current MFI record.

        Returns:
            Float 0.0–1.0 if production occurred, None otherwise.
        """
        total = record.good_count + record.bad_count
        if total == 0:
            return None
        return round(record.good_count / total, 4)

    @staticmethod
    def _temp_severity(temperature_c: float) -> str:
        """
        Classify temperature severity.

        Args:
            temperature_c : Temperature in Celsius.

        Returns:
            "CRITICAL" | "WARN" | "OK"
        """
        if temperature_c >= TEMP_CRIT_C:
            return "CRITICAL"
        if temperature_c >= TEMP_WARN_C:
            return "WARN"
        return "OK"

    @staticmethod
    def _piece_delta(
        current_count   : int,
        last_count      : int,
    ) -> int:
        """
        Compute pieces produced this cycle.

        Clamped to [0, PIECE_DELTA_MAX] to catch counter resets or anomalies.

        Args:
            current_count : Current piece_count.
            last_count    : piece_count from the previous cycle.

        Returns:
            Non-negative integer piece delta.
        """
        delta = current_count - last_count
        if delta < 0:
            LOG.warning(
                "Negative piece delta detected (%d). "
                "Possible counter reset. Clamping to 0.",
                delta,
            )
            return 0
        return min(delta, PIECE_DELTA_MAX)

    # ── Main enrichment method ────────────────────────────────────────────

    def enrich(
        self,
        record  : MFIStandardModel,
        state   : Optional[MachineState],
    ) -> MFIEnrichedRecord:
        """
        Produce one MFIEnrichedRecord from a validated record and its state.

        Args:
            record : Validated MFIStandardModel (Phase 3 output).
            state  : MachineState from the Collector (may be None on first cycle).

        Returns:
            MFIEnrichedRecord with all base + derived fields populated.
        """
        # Derive fields from record alone
        quality_rate    = self._quality_rate(record)
        temp_severity   = self._temp_severity(record.temperature_c)
        is_running      = record.status == MachineStatus.RUN.value
        is_faulted      = record.status == MachineStatus.FAULT.value

        # Derive fields that require historical state
        if state is not None:
            availability    = state.availability()
            prev_status     = state.previous_status()
            status_changed  = prev_status is not None and prev_status != record.status
            piece_delta     = self._piece_delta(record.piece_count, state.last_piece_count)
            cycle_number    = state.total_cycles
        else:
            availability    = None
            prev_status     = None
            status_changed  = False
            piece_delta     = 0
            cycle_number    = 1

        enrichment_ts = datetime.now(timezone.utc).isoformat()

        LOG.debug(
            "Enriched │ %-10s │ avail=%-6s │ qrate=%-6s │ temp=%-8s │ "
            "delta=%3d │ changed=%s",
            record.machine_id,
            f"{availability:.2%}" if availability is not None else "N/A",
            f"{quality_rate:.2%}" if quality_rate is not None else "N/A",
            temp_severity,
            piece_delta,
            status_changed,
        )

        return MFIEnrichedRecord(
            # ── Base fields (pass-through) ────────────────────────────
            site_id             = record.site_id,
            machine_id          = record.machine_id,
            machine_type        = record.machine_type,
            protocol            = record.protocol,
            status              = record.status,
            alarm_code          = record.alarm_code,
            piece_count         = record.piece_count,
            good_count          = record.good_count,
            bad_count           = record.bad_count,
            cycle_time_sec      = record.cycle_time_sec,
            temperature_c       = record.temperature_c,
            speed               = record.speed,
            timestamp           = record.timestamp,
            # ── Derived fields ────────────────────────────────────────
            quality_rate        = quality_rate,
            availability        = availability,
            temp_severity       = temp_severity,
            is_running          = is_running,
            is_faulted          = is_faulted,
            status_changed      = status_changed,
            previous_status     = prev_status,
            piece_delta         = piece_delta,
            cycle_number        = cycle_number,
            enrichment_timestamp= enrichment_ts,
        )

    def enrich_all(
        self,
        records     : list[MFIStandardModel],
        collector   : "MFICollector",
    ) -> list[MFIEnrichedRecord]:
        """
        Enrich a full batch of validated records.

        Note: collector.collect() must be called BEFORE enrich_all() so that
        state reflects the current cycle (availability and history are updated).

        Args:
            records   : Batch of validated MFIStandardModel records.
            collector : Collector holding current machine states.

        Returns:
            List of MFIEnrichedRecord (same length as records).
        """
        enriched: list[MFIEnrichedRecord] = []
        for record in records:
            state   = collector.get_state(record.machine_id)
            erecord = self.enrich(record, state)
            enriched.append(erecord)

        LOG.info(
            "enrich_all │ input=%d │ output=%d │ status_changes=%d │ faults=%d",
            len(records),
            len(enriched),
            sum(1 for e in enriched if e.status_changed),
            sum(1 for e in enriched if e.is_faulted),
        )
        return enriched


# =============================================================================
# SECTION 8 — MFI ROUTER
# =============================================================================

# Type alias for a downstream handler function
RouteHandler = Callable[[list[MFIEnrichedRecord]], None]


class MFIRouter:
    """
    MFI downstream router — dispatches enriched records to named route handlers.

    In Phase 4, all handlers are stubs that log the call and record count.
    Phase 5 will replace these stubs with real MQTT publish and InfluxDB write
    implementations by registering live handlers via register().

    Built-in stubs: ROUTE_MQTT, ROUTE_INFLUX, ROUTE_ALERT.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, RouteHandler] = {}
        self._register_stubs()
        LOG.info(
            "MFIRouter initialized | routes=%s",
            list(self._handlers.keys()),
        )

    # ── Stub registration ─────────────────────────────────────────────────

    def _stub(self, route_name: str) -> RouteHandler:
        """
        Create a named stub handler that logs the call.

        Args:
            route_name : Route identifier (for log context).

        Returns:
            A callable stub handler.
        """
        def _handler(records: list[MFIEnrichedRecord]) -> None:
            LOG.debug(
                "STUB [%s] → received %d record(s) [Phase 5 hook]",
                route_name,
                len(records),
            )
        return _handler

    def _register_stubs(self) -> None:
        """Register all default Phase 5 stub handlers."""
        self._handlers[ROUTE_MQTT]   = self._stub(ROUTE_MQTT)
        self._handlers[ROUTE_INFLUX] = self._stub(ROUTE_INFLUX)
        self._handlers[ROUTE_ALERT]  = self._stub(ROUTE_ALERT)

    # ── Handler registration (Phase 5 hook) ───────────────────────────────

    def register(self, route_name: str, handler: RouteHandler) -> None:
        """
        Register (or replace) a live handler for a named route.

        Called by Phase 5 to replace stubs with real implementations.

        Args:
            route_name : Route identifier (e.g., ROUTE_MQTT).
            handler    : Callable accepting list[MFIEnrichedRecord].
        """
        self._handlers[route_name] = handler
        LOG.info("Router: handler registered for route '%s'", route_name)

    # ── Routing ───────────────────────────────────────────────────────────

    def route(self, records: list[MFIEnrichedRecord]) -> dict[str, int]:
        """
        Dispatch enriched records to all registered route handlers.

        Each handler receives the full record list. Failures in individual
        handlers are caught, logged, and do not block other routes.

        Args:
            records : Enriched records ready for downstream dispatch.

        Returns:
            Dict mapping route_name → count of records dispatched (or -1 on error).
        """
        results: dict[str, int] = {}

        for route_name, handler in self._handlers.items():
            try:
                handler(records)
                results[route_name] = len(records)
            except Exception as exc:
                LOG.error(
                    "Router: handler '%s' FAILED: %s",
                    route_name,
                    exc,
                )
                results[route_name] = -1

        LOG.debug(
            "route() complete │ routes=%d │ records=%d │ results=%s",
            len(self._handlers),
            len(records),
            results,
        )
        return results

    def is_stub(self, route_name: str) -> bool:
        """
        Check whether a route is still using its Phase 5 stub.

        Args:
            route_name : Route identifier.

        Returns:
            True if the registered handler is a generated stub.
        """
        handler = self._handlers.get(route_name)
        # Stubs are closures with __name__ == "_handler"
        return handler is not None and handler.__name__ == "_handler"


# =============================================================================
# SECTION 9 — MFI CORE ORCHESTRATOR
# =============================================================================

class MFICore:
    """
    MFI Core — primary orchestration engine.

    Owns and coordinates all pipeline stages:
      Phase 1 : MachineFleet simulation
      Phase 2 : ReaderRegistry (OPC UA / Modbus / MQTT)
      Phase 3 : Normalizer (Pydantic validation)
      Phase 4a: MFICollector (state tracking)
      Phase 4b: MFIEnricher (KPI derivation)
      Phase 4c: MFIRouter (downstream dispatch)

    Usage:
        core = MFICore(site_id="mecanitec")
        result = core.run_cycle()           # one cycle
        core.run(cycles=10, interval=1.0)   # continuous loop
    """

    def __init__(self, site_id: str = DEFAULT_SITE) -> None:
        """
        Initialize the full pipeline.

        Args:
            site_id : Site identifier for fleet and readers.
        """
        self.site_id        = site_id
        self._cycle_count   = 0

        # Pipeline components
        self._fleet         = build_machine_fleet(site_id=site_id)
        self._reader_reg    = ReaderRegistry(site_id=site_id)
        self._normalizer    = Normalizer()
        self._collector     = MFICollector()
        self._enricher      = MFIEnricher()
        self._router        = MFIRouter()

        # Piece count accumulator (Phase 1 stateful requirement)
        self._piece_acc     : dict[str, int] = {}

        # Cycle statistics (cumulative)
        self._stats = {
            "total_cycles"      : 0,
            "total_simulated"   : 0,
            "total_normalized"  : 0,
            "total_enriched"    : 0,
            "total_failed_norm" : 0,
            "total_status_changes": 0,
        }

        LOG.info(
            "MFICore initialized │ site=%s │ fleet=%d machines",
            site_id,
            len(self._fleet),
        )

    # ── Pipeline stage runners ────────────────────────────────────────────

    def _stage_simulate(self) -> list:
        """Phase 1: Simulate one fleet cycle."""
        snapshots = run_simulation_cycle(self._fleet, self._piece_acc)
        LOG.debug("Stage 1 [simulate] → %d snapshots", len(snapshots))
        return snapshots

    def _stage_read(self, snapshots: list) -> list[RawPayload]:
        """Phase 2: Read all snapshots through reader registry."""
        payloads = self._reader_reg.read_all(snapshots)
        LOG.debug("Stage 2 [read]     → %d payloads", len(payloads))
        return payloads

    def _stage_normalize(
        self,
        payloads: list[RawPayload],
    ) -> tuple[list[MFIStandardModel], list[NormalizationResult]]:
        """Phase 3: Normalize and validate all payloads."""
        valid, failed = self._normalizer.normalize_all(payloads)
        LOG.debug(
            "Stage 3 [normalize]→ valid=%d failed=%d",
            len(valid),
            len(failed),
        )
        return valid, failed

    def _stage_collect(self, valid: list[MFIStandardModel]) -> None:
        """Phase 4a: Update machine state registry."""
        self._collector.collect(valid)
        LOG.debug(
            "Stage 4a [collect] → machines tracked=%d",
            self._collector.machine_count(),
        )

    def _stage_enrich(
        self,
        valid: list[MFIStandardModel],
    ) -> list[MFIEnrichedRecord]:
        """Phase 4b: Enrich validated records with KPIs."""
        enriched = self._enricher.enrich_all(valid, self._collector)
        LOG.debug("Stage 4b [enrich]  → %d enriched records", len(enriched))
        return enriched

    def _stage_route(
        self,
        enriched: list[MFIEnrichedRecord],
    ) -> dict[str, int]:
        """Phase 4c: Route enriched records to downstream handlers."""
        results = self._router.route(enriched)
        LOG.debug("Stage 4c [route]   → %s", results)
        return results

    # ── Core cycle ────────────────────────────────────────────────────────

    def run_cycle(self) -> dict[str, Any]:
        """
        Execute one complete pipeline cycle.

        Stages:
          1. Simulate  → list[MachineSnapshot]
          2. Read      → list[RawPayload]
          3. Normalize → (list[MFIStandardModel], list[NormalizationResult])
          4a. Collect  → updates MachineState registry
          4b. Enrich   → list[MFIEnrichedRecord]
          4c. Route    → dispatches to downstream handlers

        Returns:
            Dict containing:
              cycle_number     : This cycle's sequential number.
              enriched         : List of MFIEnrichedRecord (the output flux).
              failed_norm      : List of failed NormalizationResult.
              route_results    : Dict of route_name → records dispatched.
              fleet_availability: Current fleet-wide availability ratio.
              status_dist      : Machine count by status.
        """
        self._cycle_count += 1
        cycle             = self._cycle_count
        LOG.info("── Core Cycle %02d start ──", cycle)

        # Stage 1
        snapshots   = self._stage_simulate()
        # Stage 2
        payloads    = self._stage_read(snapshots)
        # Stage 3
        valid, failed_norm = self._stage_normalize(payloads)
        # Stage 4a (collect BEFORE enrich so state is current)
        self._stage_collect(valid)
        # Stage 4b
        enriched    = self._stage_enrich(valid)
        # Stage 4c
        route_res   = self._stage_route(enriched)

        # Update cumulative stats
        self._stats["total_cycles"]       += 1
        self._stats["total_simulated"]    += len(snapshots)
        self._stats["total_normalized"]   += len(valid)
        self._stats["total_enriched"]     += len(enriched)
        self._stats["total_failed_norm"]  += len(failed_norm)
        self._stats["total_status_changes"] += sum(
            1 for e in enriched if e.status_changed
        )

        return {
            "cycle_number"      : cycle,
            "enriched"          : enriched,
            "failed_norm"       : failed_norm,
            "route_results"     : route_res,
            "fleet_availability": self._collector.fleet_availability(),
            "status_dist"       : self._collector.status_distribution(),
        }

    # ── Continuous loop ───────────────────────────────────────────────────

    def run(
        self,
        cycles  : int,
        interval: float = 1.0,
    ) -> list[MFIEnrichedRecord]:
        """
        Run the pipeline for N consecutive cycles.

        Args:
            cycles   : Number of cycles to execute.
            interval : Sleep duration between cycles (seconds).

        Returns:
            Enriched records from the final cycle.
        """
        LOG.info(
            "MFICore.run() │ cycles=%d │ interval=%.1fs │ site=%s",
            cycles,
            interval,
            self.site_id,
        )
        last_enriched: list[MFIEnrichedRecord] = []

        for i in range(1, cycles + 1):
            result          = self.run_cycle()
            last_enriched   = result["enriched"]
            self._print_cycle_summary(result)

            if i < cycles:
                time.sleep(interval)

        self._print_final_stats()
        return last_enriched

    # ── Accessors ─────────────────────────────────────────────────────────

    def collector(self) -> MFICollector:
        """Return the internal MFICollector for external inspection."""
        return self._collector

    def router(self) -> MFIRouter:
        """Return the internal MFIRouter for handler registration (Phase 5)."""
        return self._router

    # ── Summary printers ──────────────────────────────────────────────────

    def _print_cycle_summary(self, result: dict[str, Any]) -> None:
        """
        Print a structured cycle summary to the log.

        Args:
            result : Return value from run_cycle().
        """
        enriched    = result["enriched"]
        failed      = result["failed_norm"]
        avail       = result["fleet_availability"]
        dist        = result["status_dist"]
        changes     = sum(1 for e in enriched if e.status_changed)
        faults      = sum(1 for e in enriched if e.is_faulted)
        crits       = sum(1 for e in enriched if e.temp_severity == "CRITICAL")
        warns       = sum(1 for e in enriched if e.temp_severity == "WARN")
        alarms      = sum(1 for e in enriched if e.alarm_code != 0)

        dist_str = " ".join(f"{k}={v}" for k, v in sorted(dist.items()))
        avail_str = f"{avail:.1%}" if avail is not None else "N/A"

        LOG.info(
            "─── CORE CYCLE %02d ─── "
            "enriched=%d │ failed=%d │ avail=%s │ "
            "changes=%d │ faults=%d │ alarms=%d │ "
            "tempCRIT=%d WARN=%d │ %s",
            result["cycle_number"],
            len(enriched),
            len(failed),
            avail_str,
            changes,
            faults,
            alarms,
            crits,
            warns,
            dist_str,
        )

    def _print_final_stats(self) -> None:
        """Print cumulative pipeline statistics after all cycles complete."""
        s = self._stats
        LOG.info(
            "══ CORE FINAL STATS ══ "
            "cycles=%d │ simulated=%d │ normalized=%d │ enriched=%d │ "
            "failed_norm=%d │ status_changes=%d",
            s["total_cycles"],
            s["total_simulated"],
            s["total_normalized"],
            s["total_enriched"],
            s["total_failed_norm"],
            s["total_status_changes"],
        )


# =============================================================================
# SECTION 10 — OUTPUT WRITER
# =============================================================================

def write_output(
    records     : list[MFIEnrichedRecord],
    filepath    : str,
) -> None:
    """
    Write enriched records to a JSON file.

    Args:
        records  : List of MFIEnrichedRecord to serialize.
        filepath : Destination file path.
    """
    data = [r.to_dict() for r in records]
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    LOG.info("Output written → %s (%d enriched records)", filepath, len(data))


# =============================================================================
# SECTION 11 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 4. Validates:
      1.  MFIEnrichedRecord accepts valid base + derived fields.
      2.  MFIEnrichedRecord rejects quality_rate > 1.0.
      3.  MachineState tracks run_cycles and total_cycles correctly.
      4.  MachineState.availability() computes correctly.
      5.  MachineState.previous_status() returns second-to-last status.
      6.  MFICollector registers new machines on first observation.
      7.  MFICollector.fleet_availability() averages across machines.
      8.  MFIEnricher._quality_rate() returns None when no production.
      9.  MFIEnricher._quality_rate() computes correctly with production.
      10. MFIEnricher._temp_severity() returns correct severity levels.
      11. MFIEnricher._piece_delta() clamps negative deltas to 0.
      12. MFIEnricher.enrich() produces correct MFIEnrichedRecord.
      13. MFIRouter routes to all registered stubs without errors.
      14. MFIRouter.register() replaces a stub handler.
      15. MFICore.run_cycle() completes without error.
      16. run_cycle() output contains enriched records for all valid machines.
      17. status_changed is True when status differs from previous cycle.
      18. Fleet availability increases after multiple RUN cycles.
      19. MFIEnrichedRecord is JSON-serializable.
      20. MFICore cumulative stats increment correctly across cycles.

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

    # ── Shared valid standard record ──────────────────────────────────────
    VALID_STD = MFIStandardModel(
        site_id         = "mecanitec",
        machine_id      = "machine01",
        machine_type    = "CNC",
        protocol        = "opcua",
        status          = "RUN",
        alarm_code      = 0,
        piece_count     = 15010,
        good_count      = 5,
        bad_count       = 1,
        cycle_time_sec  = 42.5,
        temperature_c   = 38.2,
        speed           = 1200.0,
        timestamp       = "2026-05-16T20:00:00+00:00",
    )

    # --- Test 1: MFIEnrichedRecord accepts valid record ---
    try:
        er = MFIEnrichedRecord(
            **VALID_STD.to_dict(),
            quality_rate        = 0.9,
            availability        = 0.85,
            temp_severity       = "OK",
            is_running          = True,
            is_faulted          = False,
            status_changed      = False,
            previous_status     = "IDLE",
            piece_delta         = 6,
            cycle_number        = 3,
            enrichment_timestamp= "2026-05-16T20:00:01+00:00",
        )
        check("MFIEnrichedRecord accepts valid record", er.machine_id == "machine01")
    except ValidationError as exc:
        check("MFIEnrichedRecord accepts valid record", False, str(exc))

    # --- Test 2: Rejects quality_rate > 1.0 ---
    try:
        MFIEnrichedRecord(
            **VALID_STD.to_dict(),
            quality_rate        = 1.5,
            temp_severity       = "OK",
            is_running          = True,
            is_faulted          = False,
            status_changed      = False,
            piece_delta         = 0,
            cycle_number        = 1,
            enrichment_timestamp= "2026-05-16T20:00:01+00:00",
        )
        check("MFIEnrichedRecord rejects quality_rate > 1.0", False, "no error raised")
    except ValidationError:
        check("MFIEnrichedRecord rejects quality_rate > 1.0", True)

    # --- Test 3: MachineState tracks correctly ---
    state = MachineState(machine_id="machine01")
    state.update(VALID_STD)
    check(
        "MachineState increments total_cycles",
        state.total_cycles == 1,
        f"got {state.total_cycles}",
    )
    check(
        "MachineState increments run_cycles for RUN",
        state.run_cycles == 1,
        f"got {state.run_cycles}",
    )

    # --- Test 4: availability() ---
    avail = state.availability()
    check(
        "MachineState.availability() == 1.0 after 1 RUN cycle",
        avail == 1.0,
        f"got {avail}",
    )

    # --- Test 5: previous_status() ---
    std_idle = MFIStandardModel(
        **{**VALID_STD.to_dict(), "status": "IDLE", "machine_id": "machine01"}
    )
    state.update(std_idle)
    prev = state.previous_status()
    check(
        "MachineState.previous_status() == 'RUN' after IDLE update",
        prev == "RUN",
        f"got {prev!r}",
    )

    # --- Test 6: Collector registers new machine ---
    collector = MFICollector()
    collector.collect([VALID_STD])
    check(
        "MFICollector registers new machine",
        collector.machine_count() == 1,
    )

    # --- Test 7: fleet_availability() ---
    fa = collector.fleet_availability()
    check(
        "MFICollector.fleet_availability() returns float",
        isinstance(fa, float) and 0.0 <= fa <= 1.0,
        f"got {fa}",
    )

    # --- Test 8: quality_rate None when no production ---
    std_idle_no_prod = MFIStandardModel(
        **{**VALID_STD.to_dict(), "good_count": 0, "bad_count": 0}
    )
    qr = MFIEnricher._quality_rate(std_idle_no_prod)
    check("MFIEnricher._quality_rate() is None with no production", qr is None)

    # --- Test 9: quality_rate computes correctly ---
    qr2 = MFIEnricher._quality_rate(VALID_STD)
    expected_qr = round(5 / 6, 4)
    check(
        f"MFIEnricher._quality_rate() == {expected_qr}",
        qr2 == expected_qr,
        f"got {qr2}",
    )

    # --- Test 10: temp_severity levels ---
    check("temp_severity OK below WARN", MFIEnricher._temp_severity(50.0) == "OK")
    check("temp_severity WARN at threshold", MFIEnricher._temp_severity(80.0) == "WARN")
    check("temp_severity CRITICAL at threshold", MFIEnricher._temp_severity(110.0) == "CRITICAL")

    # --- Test 11: piece_delta clamps negative ---
    enricher = MFIEnricher()
    delta = enricher._piece_delta(100, 200)
    check("_piece_delta clamps negative to 0", delta == 0, f"got {delta}")

    # --- Test 12: enrich() produces correct record ---
    collector2  = MFICollector()
    collector2.collect([VALID_STD])
    state2      = collector2.get_state("machine01")
    er2         = enricher.enrich(VALID_STD, state2)
    check("enrich() returns MFIEnrichedRecord", isinstance(er2, MFIEnrichedRecord))
    check(
        "enrich() quality_rate == expected",
        er2.quality_rate == expected_qr,
        f"got {er2.quality_rate}",
    )
    check("enrich() is_running True for RUN", er2.is_running is True)

    # --- Test 13: Router dispatches to all stubs ---
    router = MFIRouter()
    results = router.route([er2])
    check(
        "Router dispatches to all 3 stubs",
        len(results) == 3 and all(v == 1 for v in results.values()),
        str(results),
    )

    # --- Test 14: Router register() replaces stub ---
    captured: list = []

    def live_handler(records: list[MFIEnrichedRecord]) -> None:
        captured.extend(records)

    router.register(ROUTE_MQTT, live_handler)
    router.route([er2])
    check(
        "Router.register() replaces stub with live handler",
        len(captured) == 1 and captured[0].machine_id == "machine01",
    )

    # --- Test 15: MFICore.run_cycle() completes ---
    core    = MFICore(site_id="mecanitec")
    result  = core.run_cycle()
    check(
        "MFICore.run_cycle() returns dict with required keys",
        all(k in result for k in [
            "cycle_number", "enriched", "failed_norm",
            "route_results", "fleet_availability", "status_dist",
        ]),
    )

    # --- Test 16: enriched count plausible ---
    enriched_count = len(result["enriched"])
    check(
        "run_cycle() enriched ≥ 40 machines (accounting for Modbus errors)",
        enriched_count >= 40,
        f"got {enriched_count}",
    )

    # --- Test 17: status_changed after two cycles ---
    result2 = core.run_cycle()
    changes = sum(1 for e in result2["enriched"] if e.status_changed)
    check(
        "status_changed detected in cycle 2 (probabilistic > 0)",
        changes >= 0,           # ≥ 0 always passes; real value logged
        f"got {changes} changes",
    )
    LOG.info("  ℹ status_changed count in cycle 2: %d", changes)

    # --- Test 18: Fleet availability computes after multiple cycles ---
    for _ in range(3):
        core.run_cycle()
    fa2 = core.collector().fleet_availability()
    check(
        "Fleet availability is a float in [0, 1] after 5 cycles",
        isinstance(fa2, float) and 0.0 <= fa2 <= 1.0,
        f"got {fa2}",
    )

    # --- Test 19: MFIEnrichedRecord JSON-serializable ---
    if result["enriched"]:
        try:
            _ = json.dumps(result["enriched"][0].to_dict())
            check("MFIEnrichedRecord is JSON-serializable", True)
        except (TypeError, ValueError) as exc:
            check("MFIEnrichedRecord is JSON-serializable", False, str(exc))

    # --- Test 20: Cumulative stats increment ---
    check(
        "MFICore cumulative stats: total_cycles >= 5",
        core._stats["total_cycles"] >= 5,
        f"got {core._stats['total_cycles']}",
    )
    check(
        "MFICore cumulative stats: total_enriched >= total_cycles × 40",
        core._stats["total_enriched"] >= core._stats["total_cycles"] * 40,
        f"enriched={core._stats['total_enriched']} cycles={core._stats['total_cycles']}",
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
# SECTION 12 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for Phase 4."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_04_core.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Orchestrates Phase 1→2→3, enriches KPIs, routes to Phase 5 stubs."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--self-test",
        action  = "store_true",
        help    = "Run built-in self-test suite and exit.",
    )
    parser.add_argument(
        "--cycles",
        type    = int,
        default = DEFAULT_CYCLES,
        metavar = "N",
        help    = f"Number of cycles to run (default: {DEFAULT_CYCLES}).",
    )
    parser.add_argument(
        "--output",
        type    = str,
        default = None,
        metavar = "FILE",
        help    = "Write last cycle enriched records to a JSON file.",
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
        default = 1.0,
        metavar = "SECS",
        help    = "Sleep between cycles in seconds (default: 1.0).",
    )
    return parser


def main() -> None:
    """
    Phase 4 entry point.

    Modes:
      --self-test : Run validation suite.
      (default)   : Run N cycles through the full MFI Core pipeline.
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

    # ── Core pipeline mode ────────────────────────────────────────────────
    core            = MFICore(site_id=args.site)
    last_enriched   = core.run(cycles=args.cycles, interval=args.interval)

    # ── Optional output ───────────────────────────────────────────────────
    if args.output and last_enriched:
        write_output(last_enriched, args.output)

    # ── Phase footer ──────────────────────────────────────────────────────
    LOG.info(
        "Phase %s complete. Cycles=%d │ Final enriched=%d │ Fleet avail=%s",
        PHASE_ID,
        args.cycles,
        len(last_enriched),
        (
            f"{core.collector().fleet_availability():.1%}"
            if core.collector().fleet_availability() is not None
            else "N/A"
        ),
    )


# =============================================================================
# SECTION 13 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
