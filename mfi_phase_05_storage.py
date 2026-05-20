#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_05_storage.py
# PHASE        : 5 — MQTT Publisher + InfluxDB Writer
# PURPOSE      : Publish MFIEnrichedRecord flux to a local Mosquitto broker
#                (paho-mqtt 2.x) and write time-series points to a local
#                InfluxDB instance (influxdb-client). Both services register
#                their live handlers into the Phase 4 MFIRouter, replacing the
#                Phase 4 stubs. Operates in dry_run mode when no live services
#                are present (self-test always uses dry_run).
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : paho-mqtt>=2.0, influxdb-client>=1.x,
#                pydantic>=2.0, mfi_phase_01..04
# CLI          : python mfi_phase_05_storage.py --self-test
#                python mfi_phase_05_storage.py --cycles 5
#                python mfi_phase_05_storage.py --cycles 5 --dry-run
#                python mfi_phase_05_storage.py --cycles 5 --mqtt-only
#                python mfi_phase_05_storage.py --cycles 5 --influx-only
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

# --- paho-mqtt 2.x ---
try:
    import paho.mqtt.client as mqtt
except ImportError as exc:
    print(
        f"[FATAL] paho-mqtt not found: {exc}\n"
        "Install: pip install paho-mqtt --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- influxdb-client ---
try:
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS
    from influxdb_client.client.exceptions import InfluxDBError
except ImportError as exc:
    print(
        f"[FATAL] influxdb-client not found: {exc}\n"
        "Install: pip install influxdb-client --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Phase 4 (pulls in Phases 1-3 transitively) ---
try:
    from mfi_phase_04_core import (
        MFICore,
        MFIEnrichedRecord,
        MFIRouter,
        ROUTE_MQTT,
        ROUTE_INFLUX,
        ROUTE_ALERT,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_04_core: {exc}", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID            = "05"
PHASE_NAME          = "MQTT + InfluxDB Storage"
PHASE_VERSION       = "1.0.0"

DEFAULT_SITE        = "mecanitec"
DEFAULT_CYCLES      = 5

# ── MQTT broker settings ──────────────────────────────────────────────────────
MQTT_BROKER_HOST    = "localhost"
MQTT_BROKER_PORT    = 1883
MQTT_KEEPALIVE_SEC  = 60
MQTT_QOS_DATA       = 1             # At-least-once for enriched records
MQTT_QOS_FLEET      = 0             # Best-effort for fleet summary
MQTT_CLIENT_ID      = "mfi-phase05-publisher"
MQTT_CONNECT_TIMEOUT= 5.0           # seconds

# MQTT topic templates
MQTT_TOPIC_ENRICHED = "mfi/{site}/{machine}/enriched"
MQTT_TOPIC_FLEET    = "mfi/{site}/fleet/summary"

# ── InfluxDB settings ─────────────────────────────────────────────────────────
INFLUX_URL          = "http://localhost:8086"
INFLUX_TOKEN        = "mfi-dev-token"       # Default dev token
INFLUX_ORG          = "mecanitec"
INFLUX_BUCKET       = "mfi"
INFLUX_MEASUREMENT  = "machine_metrics"
INFLUX_TIMEOUT_MS   = 500                   # Write timeout (ms) — kept short to avoid blocking pipeline
INFLUX_RETRIES      = 3                     # Retry attempts on write failure

# ── InfluxDB tag fields (low cardinality — go to tags not fields) ─────────────
INFLUX_TAG_FIELDS: tuple[str, ...] = (
    "site_id", "machine_id", "machine_type", "protocol",
    "status", "temp_severity",
)

# ── InfluxDB numeric field names (go to fields, not tags) ────────────────────
INFLUX_NUMERIC_FIELDS: tuple[str, ...] = (
    "alarm_code", "piece_count", "good_count", "bad_count",
    "cycle_time_sec", "temperature_c", "speed",
    "piece_delta", "cycle_number",
)

INFLUX_OPTIONAL_FLOAT_FIELDS: tuple[str, ...] = (
    "quality_rate", "availability",
)

INFLUX_BOOL_AS_INT_FIELDS: tuple[str, ...] = (
    "is_running", "is_faulted", "status_changed",
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


LOG = build_logger("mfi.phase05")

# =============================================================================
# SECTION 4 — MQTT PUBLISHER
# =============================================================================

class MFIMQTTPublisher:
    """
    MQTT publisher for MFI enriched records.

    Uses paho-mqtt 2.x API. Connects to a local Mosquitto broker and
    publishes one message per machine per cycle to the enriched topic,
    plus one fleet summary message per cycle.

    Operates in dry_run mode when no broker is available or when
    explicitly requested. In dry_run, all payloads are constructed and
    validated but not transmitted.

    Topics published:
      mfi/{site_id}/{machine_id}/enriched  — Full MFIEnrichedRecord JSON
      mfi/{site_id}/fleet/summary          — Cycle-level fleet KPI summary
    """

    def __init__(
        self,
        host        : str   = MQTT_BROKER_HOST,
        port        : int   = MQTT_BROKER_PORT,
        client_id   : str   = MQTT_CLIENT_ID,
        dry_run     : bool  = False,
    ) -> None:
        """
        Initialize the MQTT publisher.

        Args:
            host      : Broker hostname or IP.
            port      : Broker port (default 1883).
            client_id : MQTT client identifier.
            dry_run   : If True, build payloads but do not connect or publish.
        """
        self.host       = host
        self.port       = port
        self.dry_run    = dry_run
        self._connected = False
        self._published = 0         # Cumulative message count
        self._errors    = 0         # Cumulative error count

        # paho 2.x requires CallbackAPIVersion
        self._client = mqtt.Client(
            callback_api_version    = mqtt.CallbackAPIVersion.VERSION2,
            client_id               = client_id,
            clean_session           = True,
        )
        self._client.on_connect     = self._on_connect
        self._client.on_disconnect  = self._on_disconnect
        self._client.on_publish     = self._on_publish

        LOG.info(
            "MFIMQTTPublisher initialized │ broker=%s:%d │ dry_run=%s",
            host, port, dry_run,
        )

    # ── paho callbacks ────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        """Called when broker connection is established or fails."""
        if reason_code == 0:
            self._connected = True
            LOG.info("MQTT connected │ broker=%s:%d", self.host, self.port)
        else:
            self._connected = False
            LOG.error("MQTT connect FAILED │ reason_code=%s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        """Called when broker connection is lost."""
        self._connected = False
        if reason_code != 0:
            LOG.warning("MQTT unexpected disconnect │ reason_code=%s", reason_code)
        else:
            LOG.info("MQTT disconnected cleanly")

    def _on_publish(self, client, userdata, mid, reason_code, properties) -> None:
        """Called when a message publish is acknowledged by the broker."""
        LOG.debug("MQTT publish ACK │ mid=%d │ rc=%s", mid, reason_code)

    # ── Connection management ─────────────────────────────────────────────

    def connect(self) -> bool:
        """
        Attempt to connect to the MQTT broker.

        Returns:
            True if connected, False on failure (non-fatal in dry_run context).
        """
        if self.dry_run:
            LOG.info("MQTT dry_run=True │ skipping broker connection")
            return False

        try:
            self._client.connect(
                host        = self.host,
                port        = self.port,
                keepalive   = MQTT_KEEPALIVE_SEC,
            )
            self._client.loop_start()
            # Brief wait for on_connect callback
            deadline = time.monotonic() + MQTT_CONNECT_TIMEOUT
            while not self._connected and time.monotonic() < deadline:
                time.sleep(0.05)

            if not self._connected:
                LOG.error(
                    "MQTT connect timeout after %.1fs │ broker=%s:%d",
                    MQTT_CONNECT_TIMEOUT, self.host, self.port,
                )
                return False
            return True

        except (OSError, ConnectionRefusedError) as exc:
            LOG.error("MQTT connect ERROR │ %s:%d │ %s", self.host, self.port, exc)
            return False

    def disconnect(self) -> None:
        """Cleanly disconnect from the broker and stop the network loop."""
        if self._connected:
            self._client.loop_stop()
            self._client.disconnect()
            LOG.info("MQTT publisher disconnected")

    # ── Payload builders ──────────────────────────────────────────────────

    @staticmethod
    def _build_enriched_payload(record: MFIEnrichedRecord) -> bytes:
        """
        Serialize one MFIEnrichedRecord to JSON bytes for MQTT publish.

        Args:
            record : Enriched record to serialize.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        return json.dumps(record.to_dict(), default=str).encode("utf-8")

    @staticmethod
    def _build_fleet_payload(
        records     : list[MFIEnrichedRecord],
        cycle_num   : int,
        site_id     : str,
    ) -> bytes:
        """
        Build a fleet-level summary JSON payload for the cycle.

        Args:
            records   : All enriched records for this cycle.
            cycle_num : Cycle sequence number.
            site_id   : Site identifier.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        status_dist: dict[str, int] = {}
        total_alarms    = 0
        total_crits     = 0
        running_count   = 0

        for r in records:
            status_dist[r.status] = status_dist.get(r.status, 0) + 1
            if r.alarm_code != 0:
                total_alarms += 1
            if r.temp_severity == "CRITICAL":
                total_crits += 1
            if r.is_running:
                running_count += 1

        avail = round(running_count / len(records), 4) if records else 0.0

        summary = {
            "site_id"           : site_id,
            "cycle_number"      : cycle_num,
            "machine_count"     : len(records),
            "availability"      : avail,
            "status_distribution": status_dist,
            "active_alarms"     : total_alarms,
            "critical_temps"    : total_crits,
            "timestamp"         : datetime.now(timezone.utc).isoformat(),
        }
        return json.dumps(summary).encode("utf-8")

    # ── Publish methods ───────────────────────────────────────────────────

    def _publish_one(self, topic: str, payload: bytes, qos: int) -> bool:
        """
        Publish one MQTT message. In dry_run, logs and returns True.

        Args:
            topic   : MQTT topic string.
            payload : Message payload bytes.
            qos     : Quality of service level.

        Returns:
            True on success (or dry_run), False on publish error.
        """
        if self.dry_run:
            LOG.debug(
                "DRY-RUN MQTT │ topic=%s │ qos=%d │ bytes=%d",
                topic, qos, len(payload),
            )
            self._published += 1
            return True

        if not self._connected:
            self._errors += 1
            if self._errors == 1:
                LOG.warning(
                    "MQTT not connected — publish skipped. "
                    "Further skip warnings suppressed."
                )
            return False

        info = self._client.publish(topic=topic, payload=payload, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            LOG.error(
                "MQTT publish FAILED │ rc=%d │ topic=%s",
                info.rc, topic,
            )
            self._errors += 1
            return False

        self._published += 1
        return True

    def publish_batch(
        self,
        records     : list[MFIEnrichedRecord],
        cycle_num   : int = 0,
    ) -> dict[str, int]:
        """
        Publish all enriched records for one cycle plus a fleet summary.

        Args:
            records   : Enriched records from Phase 4.
            cycle_num : Cycle sequence number (for fleet summary).

        Returns:
            Dict with "published", "skipped", "errors" counts.
        """
        published   = 0
        skipped     = 0
        errors      = 0

        for record in records:
            topic   = MQTT_TOPIC_ENRICHED.format(
                site    = record.site_id,
                machine = record.machine_id,
            )
            payload = self._build_enriched_payload(record)
            ok = self._publish_one(topic, payload, MQTT_QOS_DATA)
            if ok:
                published += 1
            else:
                errors += 1

        # Fleet summary — published once per cycle
        if records:
            site_id     = records[0].site_id
            fleet_topic = MQTT_TOPIC_FLEET.format(site=site_id)
            fleet_payload = self._build_fleet_payload(records, cycle_num, site_id)
            ok = self._publish_one(fleet_topic, fleet_payload, MQTT_QOS_FLEET)
            if ok:
                published += 1
            else:
                errors += 1

        LOG.info(
            "MQTT publish_batch │ records=%d │ published=%d │ errors=%d │ dry_run=%s",
            len(records), published, errors, self.dry_run,
        )
        return {"published": published, "skipped": skipped, "errors": errors}

    @property
    def stats(self) -> dict[str, Any]:
        """Return cumulative publisher statistics."""
        return {
            "connected" : self._connected,
            "published" : self._published,
            "errors"    : self._errors,
            "dry_run"   : self.dry_run,
        }


# =============================================================================
# SECTION 5 — INFLUXDB WRITER
# =============================================================================

class MFIInfluxWriter:
    """
    InfluxDB time-series writer for MFI enriched records.

    Uses influxdb-client (Python SDK). Each MFIEnrichedRecord is converted
    to one InfluxDB Point with:
      - Measurement : INFLUX_MEASUREMENT ("machine_metrics")
      - Tags        : site_id, machine_id, machine_type, protocol,
                      status, temp_severity
      - Fields      : all numeric and boolean KPIs
      - Timestamp   : record.timestamp (nanosecond precision)

    Operates in dry_run mode when no InfluxDB instance is available.
    In dry_run, Points are constructed and line-protocol is validated
    but no network write is attempted.
    """

    def __init__(
        self,
        url     : str   = INFLUX_URL,
        token   : str   = INFLUX_TOKEN,
        org     : str   = INFLUX_ORG,
        bucket  : str   = INFLUX_BUCKET,
        dry_run : bool  = False,
    ) -> None:
        """
        Initialize the InfluxDB writer.

        Args:
            url     : InfluxDB server URL.
            token   : Authentication token.
            org     : InfluxDB organization name.
            bucket  : Target bucket name.
            dry_run : If True, build Points but do not write.
        """
        self.url        = url
        self.org        = org
        self.bucket     = bucket
        self.dry_run    = dry_run
        self._written   = 0
        self._errors    = 0
        self._client    : Optional[InfluxDBClient]  = None
        self._write_api                             = None

        LOG.info(
            "MFIInfluxWriter initialized │ url=%s │ org=%s │ bucket=%s │ dry_run=%s",
            url, org, bucket, dry_run,
        )

    # ── Connection management ─────────────────────────────────────────────

    def connect(self) -> bool:
        """
        Initialize InfluxDB client and write API.

        In dry_run mode, client is NOT instantiated (avoids network dependency).

        Returns:
            True if ready, False if initialization failed.
        """
        if self.dry_run:
            LOG.info("InfluxDB dry_run=True │ skipping client initialization")
            return False

        try:
            self._client = InfluxDBClient(
                url     = self.url,
                token   = INFLUX_TOKEN,
                org     = self.org,
                timeout = INFLUX_TIMEOUT_MS,
            )
            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
            LOG.info("InfluxDB client initialized │ url=%s │ org=%s", self.url, self.org)
            return True

        except Exception as exc:
            LOG.error("InfluxDB init ERROR │ %s", exc)
            return False

    def close(self) -> None:
        """Close the InfluxDB client connection."""
        if self._client:
            self._client.close()
            LOG.info("InfluxDB client closed")

    # ── Point builder ─────────────────────────────────────────────────────

    @staticmethod
    def build_point(record: MFIEnrichedRecord) -> Point:
        """
        Convert one MFIEnrichedRecord to an InfluxDB Point.

        Tag strategy  : Low-cardinality string fields → tags (indexed).
        Field strategy: All numeric and boolean KPIs → fields (stored values).
        Timestamp     : Parsed from record.timestamp (UTC, nanosecond resolution).

        Args:
            record : MFIEnrichedRecord from Phase 4.

        Returns:
            influxdb_client.Point ready for write_api.write().

        Raises:
            ValueError : If record.timestamp cannot be parsed.
        """
        # Parse timestamp
        ts_str  = record.timestamp.replace("Z", "+00:00")
        try:
            ts  = datetime.fromisoformat(ts_str)
        except ValueError as exc:
            raise ValueError(
                f"Cannot parse timestamp '{record.timestamp}': {exc}"
            ) from exc

        # Start point with measurement name
        point = Point(INFLUX_MEASUREMENT)

        # ── Tags (low-cardinality string fields) ──────────────────────
        point = (point
            .tag("site_id",      record.site_id)
            .tag("machine_id",   record.machine_id)
            .tag("machine_type", record.machine_type)
            .tag("protocol",     record.protocol)
            .tag("status",       record.status)
            .tag("temp_severity",record.temp_severity)
        )

        # ── Integer / float fields ────────────────────────────────────
        for field_name in INFLUX_NUMERIC_FIELDS:
            value = getattr(record, field_name, None)
            if value is not None:
                point = point.field(field_name, value)

        # ── Optional float fields (may be None — skip if None) ────────
        for field_name in INFLUX_OPTIONAL_FLOAT_FIELDS:
            value = getattr(record, field_name, None)
            if value is not None:
                point = point.field(field_name, float(value))

        # ── Boolean fields stored as int (0/1) ───────────────────────
        for field_name in INFLUX_BOOL_AS_INT_FIELDS:
            value = getattr(record, field_name, None)
            if value is not None:
                point = point.field(field_name, int(value))

        # ── Timestamp ─────────────────────────────────────────────────
        point = point.time(ts)

        return point

    # ── Write methods ─────────────────────────────────────────────────────

    def write_batch(
        self,
        records: list[MFIEnrichedRecord],
    ) -> dict[str, int]:
        """
        Convert and write a batch of enriched records to InfluxDB.

        In dry_run mode, Points are built and line-protocol logged but
        no write call is made.

        Args:
            records : Enriched records from Phase 4.

        Returns:
            Dict with "written", "skipped", "errors" counts.
        """
        written = 0
        skipped = 0
        errors  = 0

        # Build all Points first (validates structure before any write)
        points: list[Point] = []
        for record in records:
            try:
                pt = self.build_point(record)
                points.append(pt)
            except (ValueError, AttributeError) as exc:
                LOG.error(
                    "InfluxDB Point build FAILED │ machine=%s │ %s",
                    record.machine_id, exc,
                )
                errors += 1

        if self.dry_run:
            for pt in points:
                lp = pt.to_line_protocol()
                LOG.debug("DRY-RUN InfluxDB │ %s", lp[:120] if lp else "(empty)")
                written += 1
        else:
            if self._write_api is None:
                LOG.error(
                    "InfluxDB write_api not initialized. "
                    "Call connect() first or use dry_run=True."
                )
                return {"written": 0, "skipped": len(points), "errors": 1}

            # Circuit breaker: skip all writes after first connection failure
            if self._errors > 0:
                return {"written": 0, "skipped": len(points), "errors": 0}

            _connection_failed = False
            for pt in points:
                if _connection_failed:
                    skipped += 1
                    continue
                for attempt in range(1, INFLUX_RETRIES + 1):
                    try:
                        self._write_api.write(
                            bucket  = self.bucket,
                            org     = self.org,
                            record  = pt,
                        )
                        written += 1
                        self._written += 1
                        break
                    except InfluxDBError as exc:
                        LOG.error(
                            "InfluxDB write FAILED │ attempt=%d/%d │ %s",
                            attempt, INFLUX_RETRIES, exc,
                        )
                        if attempt == INFLUX_RETRIES:
                            errors += 1
                            self._errors += 1
                    except Exception as exc:
                        self._errors += 1
                        errors += 1
                        _connection_failed = True
                        if self._errors == 1:
                            LOG.error(
                                "InfluxDB connection failed │ %s │ ", exc,
                            )
                            LOG.warning(
                                "InfluxDB unreachable — further errors suppressed. "
                                "Use --no-storage to disable entirely."
                            )
                        break

        LOG.info(
            "InfluxDB write_batch │ records=%d │ written=%d │ errors=%d │ dry_run=%s",
            len(records), written, errors, self.dry_run,
        )
        return {"written": written, "skipped": skipped, "errors": errors}

    @property
    def stats(self) -> dict[str, Any]:
        """Return cumulative writer statistics."""
        return {
            "written"   : self._written,
            "errors"    : self._errors,
            "dry_run"   : self.dry_run,
        }


# =============================================================================
# SECTION 6 — MFI STORAGE SERVICE
# =============================================================================

class MFIStorageService:
    """
    MFI Storage Service — coordinates MQTT and InfluxDB, registers live
    handlers into the Phase 4 MFIRouter.

    Replaces Phase 4 stubs with:
      ROUTE_MQTT   → MFIMQTTPublisher.publish_batch()
      ROUTE_INFLUX → MFIInfluxWriter.write_batch()
      ROUTE_ALERT  → alert_stub (unchanged — Phase 7 will replace)

    Usage:
        service = MFIStorageService(dry_run=True)
        service.connect()
        service.register_handlers(core.router())
        core.run(cycles=5)
        service.close()
    """

    def __init__(
        self,
        mqtt_host   : str   = MQTT_BROKER_HOST,
        mqtt_port   : int   = MQTT_BROKER_PORT,
        influx_url  : str   = INFLUX_URL,
        influx_org  : str   = INFLUX_ORG,
        influx_bucket: str  = INFLUX_BUCKET,
        dry_run     : bool  = False,
        enable_mqtt : bool  = True,
        enable_influx: bool = True,
    ) -> None:
        """
        Initialize both storage backends.

        Args:
            mqtt_host      : MQTT broker hostname.
            mqtt_port      : MQTT broker port.
            influx_url     : InfluxDB URL.
            influx_org     : InfluxDB org name.
            influx_bucket  : InfluxDB bucket name.
            dry_run        : If True, both backends run without transmitting.
            enable_mqtt    : If False, MQTT handler is not registered.
            enable_influx  : If False, InfluxDB handler is not registered.
        """
        self.dry_run        = dry_run
        self.enable_mqtt    = enable_mqtt
        self.enable_influx  = enable_influx
        self._cycle_counter = 0

        self._mqtt      = MFIMQTTPublisher(
            host    = mqtt_host,
            port    = mqtt_port,
            dry_run = dry_run,
        ) if enable_mqtt else None

        self._influx    = MFIInfluxWriter(
            url     = influx_url,
            org     = influx_org,
            bucket  = influx_bucket,
            dry_run = dry_run,
        ) if enable_influx else None

        LOG.info(
            "MFIStorageService initialized │ dry_run=%s │ mqtt=%s │ influx=%s",
            dry_run, enable_mqtt, enable_influx,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def connect(self) -> dict[str, bool]:
        """
        Establish connections for both backends.

        Returns:
            Dict mapping service name → connected (bool).
        """
        results: dict[str, bool] = {}

        if self._mqtt:
            results["mqtt"]     = self._mqtt.connect()
        if self._influx:
            results["influx"]   = self._influx.connect()

        LOG.info("StorageService.connect() │ results=%s", results)
        return results

    def close(self) -> None:
        """Close all backend connections."""
        if self._mqtt:
            self._mqtt.disconnect()
        if self._influx:
            self._influx.close()
        LOG.info("StorageService closed")

    # ── Handler registration ───────────────────────────────────────────────

    def register_handlers(self, router: MFIRouter) -> None:
        """
        Replace Phase 4 stubs with live storage handlers in the MFIRouter.

        Args:
            router : MFIRouter from MFICore (via core.router()).
        """
        if self._mqtt:
            def mqtt_handler(records: list[MFIEnrichedRecord]) -> None:
                self._cycle_counter += 1
                self._mqtt.publish_batch(records, cycle_num=self._cycle_counter)

            router.register(ROUTE_MQTT, mqtt_handler)
            LOG.info("MFIMQTTPublisher registered as ROUTE_MQTT handler")

        if self._influx:
            def influx_handler(records: list[MFIEnrichedRecord]) -> None:
                self._influx.write_batch(records)

            router.register(ROUTE_INFLUX, influx_handler)
            LOG.info("MFIInfluxWriter registered as ROUTE_INFLUX handler")

    # ── Stats ──────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Return combined statistics from both backends."""
        return {
            "mqtt"  : self._mqtt.stats    if self._mqtt   else None,
            "influx": self._influx.stats  if self._influx else None,
        }


# =============================================================================
# SECTION 7 — CYCLE SUMMARY
# =============================================================================

def print_storage_summary(
    cycle_num   : int,
    mqtt_result : Optional[dict],
    influx_result: Optional[dict],
) -> None:
    """
    Print storage operation summary for one cycle.

    Args:
        cycle_num     : Current cycle number.
        mqtt_result   : publish_batch() result dict or None.
        influx_result : write_batch() result dict or None.
    """
    mqtt_str    = (
        f"mqtt=published:{mqtt_result['published']} err:{mqtt_result['errors']}"
        if mqtt_result else "mqtt=disabled"
    )
    influx_str  = (
        f"influx=written:{influx_result['written']} err:{influx_result['errors']}"
        if influx_result else "influx=disabled"
    )
    LOG.info(
        "─── STORAGE CYCLE %02d ─── %s │ %s",
        cycle_num, mqtt_str, influx_str,
    )


# =============================================================================
# SECTION 8 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 5. All tests use dry_run=True (no live services needed).

    Validates:
      1.  MFIMQTTPublisher initializes without broker.
      2.  _build_enriched_payload() produces valid JSON bytes.
      3.  _build_fleet_payload() produces valid JSON with required keys.
      4.  publish_batch() in dry_run processes all records without error.
      5.  publish_batch() returns correct counts (published == records + 1 fleet).
      6.  MFIInfluxWriter initializes without InfluxDB server.
      7.  build_point() produces a Point with correct tags.
      8.  build_point() line protocol contains site_id tag.
      9.  build_point() includes numeric fields (temperature_c, piece_count).
      10. build_point() stores booleans as int (0/1).
      11. build_point() skips None optional fields (quality_rate).
      12. write_batch() in dry_run processes all records without error.
      13. write_batch() returns correct written count.
      14. MFIStorageService initializes with both backends.
      15. register_handlers() replaces Phase 4 stubs with live handlers.
      16. Full pipeline: MFICore → StorageService dry_run processes 50 machines.
      17. StorageService.stats() returns mqtt and influx sub-dicts.
      18. MQTT topic format is correct for enriched records.
      19. MFIInfluxWriter.build_point() raises ValueError on bad timestamp.
      20. Cumulative stats update after multi-cycle run.

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

    # ── Build one enriched record for unit tests ──────────────────────────
    from mfi_phase_03_json_model import MFIStandardModel
    from mfi_phase_04_core import MFIEnricher, MFICollector

    std = MFIStandardModel(
        site_id         = "mecanitec",
        machine_id      = "machine01",
        machine_type    = "CNC",
        protocol        = "opcua",
        status          = "RUN",
        alarm_code      = 0,
        piece_count     = 15010,
        good_count      = 6,
        bad_count       = 0,
        cycle_time_sec  = 42.5,
        temperature_c   = 38.2,
        speed           = 1200.0,
        timestamp       = "2026-05-16T20:00:00+00:00",
    )

    collector = MFICollector()
    collector.collect([std])
    enricher  = MFIEnricher()
    er: MFIEnrichedRecord = enricher.enrich(std, collector.get_state("machine01"))

    # ── MQTT unit tests ───────────────────────────────────────────────────

    # Test 1: Publisher init
    pub = MFIMQTTPublisher(dry_run=True)
    check("MFIMQTTPublisher initializes (dry_run)", pub.dry_run is True)

    # Test 2: Enriched payload valid JSON
    payload_bytes = MFIMQTTPublisher._build_enriched_payload(er)
    try:
        decoded = json.loads(payload_bytes)
        check(
            "_build_enriched_payload() produces valid JSON",
            "machine_id" in decoded and decoded["machine_id"] == "machine01",
        )
    except json.JSONDecodeError as exc:
        check("_build_enriched_payload() produces valid JSON", False, str(exc))

    # Test 3: Fleet payload has required keys
    fleet_bytes = MFIMQTTPublisher._build_fleet_payload([er], cycle_num=1, site_id="mecanitec")
    try:
        fleet = json.loads(fleet_bytes)
        required_fleet_keys = [
            "site_id", "cycle_number", "machine_count",
            "availability", "status_distribution", "active_alarms", "timestamp",
        ]
        keys_ok = all(k in fleet for k in required_fleet_keys)
        check("_build_fleet_payload() has all required keys", keys_ok, str(list(fleet.keys())))
    except json.JSONDecodeError as exc:
        check("_build_fleet_payload() has all required keys", False, str(exc))

    # Test 4: publish_batch dry_run no errors
    result = pub.publish_batch([er], cycle_num=1)
    check("publish_batch() dry_run runs without error", result["errors"] == 0)

    # Test 5: publish_batch count = records + 1 fleet message
    check(
        "publish_batch() published == records + 1 fleet",
        result["published"] == 2,          # 1 machine + 1 fleet
        f"got {result['published']}",
    )

    # ── InfluxDB unit tests ───────────────────────────────────────────────

    # Test 6: Writer init
    writer = MFIInfluxWriter(dry_run=True)
    check("MFIInfluxWriter initializes (dry_run)", writer.dry_run is True)

    # Test 7: build_point() produces Point with correct tags
    pt = MFIInfluxWriter.build_point(er)
    lp = pt.to_line_protocol()
    check(
        "build_point() line protocol contains measurement name",
        lp.startswith(INFLUX_MEASUREMENT),
        f"got: {lp[:60]}",
    )

    # Test 8: Line protocol contains site_id tag
    check(
        "build_point() line protocol contains site_id tag",
        "site_id=mecanitec" in lp,
        f"got: {lp[:120]}",
    )

    # Test 9: Line protocol contains numeric fields
    check(
        "build_point() contains temperature_c field",
        "temperature_c=" in lp,
        f"got: {lp[:120]}",
    )
    check(
        "build_point() contains piece_count field",
        "piece_count=" in lp,
        f"got: {lp[:120]}",
    )

    # Test 10: Boolean fields stored as int
    check(
        "build_point() stores is_running as int",
        "is_running=1i" in lp or "is_running=1" in lp,
        f"got: {lp}",
    )

    # Test 11: None quality_rate is absent from line protocol
    # er.quality_rate is None (good=6 bad=0 → quality=1.0 actually, so use idle record)
    std_idle = MFIStandardModel(
        **{**std.to_dict(), "status": "IDLE", "good_count": 0, "bad_count": 0}
    )
    collector_t11 = MFICollector()
    collector_t11.collect([std_idle])
    er_idle = enricher.enrich(std_idle, collector_t11.get_state("machine01"))
    pt_idle = MFIInfluxWriter.build_point(er_idle)
    lp_idle = pt_idle.to_line_protocol()
    # quality_rate is None for idle with no production → should be absent
    check(
        "build_point() skips None quality_rate field",
        "quality_rate" not in lp_idle,
        f"quality_rate in lp: {'quality_rate' in lp_idle} | lp={lp_idle[:120]}",
    )

    # Test 12: write_batch dry_run no errors
    wr = writer.write_batch([er])
    check("write_batch() dry_run runs without error", wr["errors"] == 0)

    # Test 13: write_batch written count
    check(
        "write_batch() written == 1",
        wr["written"] == 1,
        f"got {wr['written']}",
    )

    # Test 14: StorageService init
    service = MFIStorageService(dry_run=True)
    check(
        "MFIStorageService initializes with both backends",
        service._mqtt is not None and service._influx is not None,
    )

    # Test 15: register_handlers() replaces stubs
    core = MFICore(site_id="mecanitec")
    service.register_handlers(core.router())
    check(
        "register_handlers() replaces ROUTE_MQTT stub",
        not core.router().is_stub(ROUTE_MQTT),
    )
    check(
        "register_handlers() replaces ROUTE_INFLUX stub",
        not core.router().is_stub(ROUTE_INFLUX),
    )

    # Test 16: Full pipeline — one cycle, 50 machines
    result_cycle = core.run_cycle()
    enriched_count = len(result_cycle["enriched"])
    check(
        "Full pipeline dry_run: enriched ≥ 40 machines",
        enriched_count >= 40,
        f"got {enriched_count}",
    )

    # Test 17: StorageService stats returns sub-dicts
    st = service.stats()
    check(
        "StorageService.stats() returns 'mqtt' and 'influx' sub-dicts",
        "mqtt" in st and "influx" in st,
        str(st),
    )

    # Test 18: MQTT topic format
    topic = MQTT_TOPIC_ENRICHED.format(site=er.site_id, machine=er.machine_id)
    check(
        "MQTT enriched topic format is correct",
        topic == "mfi/mecanitec/machine01/enriched",
        f"got {topic!r}",
    )

    # Test 19: build_point raises ValueError on bad timestamp
    bad_er = er.model_copy(update={"timestamp": "not-a-timestamp"})
    try:
        MFIInfluxWriter.build_point(bad_er)
        check("build_point() raises ValueError on bad timestamp", False, "no error")
    except ValueError:
        check("build_point() raises ValueError on bad timestamp", True)

    # Test 20: Cumulative stats after multi-cycle run
    service2 = MFIStorageService(dry_run=True)
    core2    = MFICore(site_id="mecanitec")
    service2.register_handlers(core2.router())
    core2.run(cycles=3, interval=0.0)
    st2 = service2.stats()
    mqtt_published = st2["mqtt"]["published"] if st2["mqtt"] else 0
    check(
        "MQTT cumulative published > 0 after 3 cycles",
        mqtt_published > 0,
        f"got {mqtt_published}",
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
# SECTION 9 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for Phase 5."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_05_storage.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Publishes enriched records to MQTT broker and InfluxDB."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--self-test",
        action  = "store_true",
        help    = "Run built-in self-test suite (always dry_run). Exit 0=OK, 1=FAIL.",
    )
    parser.add_argument(
        "--cycles",
        type    = int,
        default = DEFAULT_CYCLES,
        metavar = "N",
        help    = f"Number of pipeline cycles (default: {DEFAULT_CYCLES}).",
    )
    parser.add_argument(
        "--dry-run",
        action  = "store_true",
        help    = "Build payloads and points but do not transmit to broker/DB.",
    )
    parser.add_argument(
        "--mqtt-only",
        action  = "store_true",
        help    = "Enable MQTT publishing only (disable InfluxDB).",
    )
    parser.add_argument(
        "--influx-only",
        action  = "store_true",
        help    = "Enable InfluxDB writing only (disable MQTT).",
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
        help    = "Sleep between cycles (default: 1.0).",
    )
    parser.add_argument(
        "--mqtt-host",
        type    = str,
        default = MQTT_BROKER_HOST,
        help    = f"MQTT broker host (default: {MQTT_BROKER_HOST}).",
    )
    parser.add_argument(
        "--influx-url",
        type    = str,
        default = INFLUX_URL,
        help    = f"InfluxDB URL (default: {INFLUX_URL}).",
    )
    return parser


def main() -> None:
    """
    Phase 5 entry point.

    Modes:
      --self-test   : Run validation suite (dry_run forced).
      --dry-run     : Full pipeline, payloads built but not transmitted.
      (default)     : Full pipeline with live MQTT + InfluxDB.
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

    # ── Resolve enable flags ──────────────────────────────────────────────
    enable_mqtt     = not args.influx_only
    enable_influx   = not args.mqtt_only
    dry_run         = args.dry_run

    LOG.info(
        "Starting storage service │ mqtt=%s │ influx=%s │ dry_run=%s │ cycles=%d",
        enable_mqtt, enable_influx, dry_run, args.cycles,
    )

    # ── Initialize storage service ────────────────────────────────────────
    service = MFIStorageService(
        mqtt_host       = args.mqtt_host,
        influx_url      = args.influx_url,
        dry_run         = dry_run,
        enable_mqtt     = enable_mqtt,
        enable_influx   = enable_influx,
    )
    conn_results = service.connect()
    LOG.info("Connection results: %s", conn_results)

    # ── Initialize core and register handlers ─────────────────────────────
    core = MFICore(site_id=args.site)
    service.register_handlers(core.router())

    # ── Run pipeline ──────────────────────────────────────────────────────
    try:
        core.run(cycles=args.cycles, interval=args.interval)
    finally:
        service.close()

    # ── Final stats ───────────────────────────────────────────────────────
    st = service.stats()
    LOG.info("Phase %s complete │ stats=%s", PHASE_ID, st)


# =============================================================================
# SECTION 10 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()