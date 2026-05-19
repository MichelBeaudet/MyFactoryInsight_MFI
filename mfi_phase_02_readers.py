#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_02_readers.py
# PHASE        : 2 — Industrial Readers
# PURPOSE      : Simulate protocol-specific readers (OPC UA, Modbus TCP, MQTT).
#                Each reader ingests a MachineSnapshot from Phase 1 and wraps
#                it in an authentic protocol-flavored raw payload.
#                These raw payloads are what Phase 3 (normalizer) will consume.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : stdlib only + mfi_phase_01_simulation (Phase 1)
# CLI          : python mfi_phase_02_readers.py --self-test
#                python mfi_phase_02_readers.py --cycles 3
#                python mfi_phase_02_readers.py --cycles 3 --output readers.json
#                python mfi_phase_02_readers.py --protocol opcua --cycles 2
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import abc
import argparse
import json
import logging
import math
import struct
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

# --- Phase 1 dependency ---
try:
    from mfi_phase_01_simulation import (
        MachineConfig,
        MachineSnapshot,
        MachineStatus,
        build_machine_fleet,
        run_simulation_cycle,
        build_logger as _p1_build_logger,
    )
except ImportError as exc:
    print(
        f"[FATAL] Cannot import mfi_phase_01_simulation: {exc}\n"
        "Ensure mfi_phase_01_simulation.py is in the same directory.",
        file=sys.stderr,
    )
    sys.exit(1)

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID            = "02"
PHASE_NAME          = "Industrial Readers"
PHASE_VERSION       = "1.0.0"

DEFAULT_SITE        = "mecanitec"
DEFAULT_CYCLES      = 3
DEFAULT_PROTOCOL    = "all"          # "all" | "opcua" | "modbus" | "mqtt"

# ── OPC UA constants ──────────────────────────────────────────────────────────
OPCUA_NAMESPACE     = 2             # ns=2 (vendor namespace)
OPCUA_SERVER_LAG_MS = 5             # Simulated server processing lag (ms)

OPCUA_STATUS_CODES: dict[str, str] = {
    "Good"          : "0x00000000",
    "Bad"           : "0x80000000",
    "Uncertain"     : "0x40000000",
    "BadNoData"     : "0x809B0000",
    "BadTimeout"    : "0x800A0000",
}

# OPC UA node layout per machine: node_name → data_type
OPCUA_NODE_MAP: dict[str, str] = {
    "Status"        : "String",
    "AlarmCode"     : "Int32",
    "PieceCount"    : "Int64",
    "GoodCount"     : "Int32",
    "BadCount"      : "Int32",
    "CycleTimeSec"  : "Float",
    "TemperatureC"  : "Float",
    "Speed"         : "Float",
    "MachineType"   : "String",
    "Protocol"      : "String",
    "SiteId"        : "String",
}

# ── Modbus TCP constants ──────────────────────────────────────────────────────
MODBUS_FUNCTION_CODE    = 3         # FC03 = Read Holding Registers
MODBUS_PROTOCOL_ID      = 0         # Always 0 for Modbus TCP
MODBUS_TRANSACTION_START= 1         # Transaction ID counter seed

# Modbus register map: field_name → (start_address, count, scale_factor)
# All values stored as 16-bit unsigned integers (0–65535).
# scale_factor: divide raw register value to get real value.
MODBUS_REGISTER_MAP: dict[str, tuple[int, int, float]] = {
    # address  count  scale
    "status"        : (40001, 1,  1.0),   # encoded: RUN=1 IDLE=2 FAULT=3 MAINT=4
    "alarm_code"    : (40002, 1,  1.0),
    "piece_count"   : (40003, 2,  1.0),   # 32-bit across 2 registers (high/low)
    "good_count"    : (40005, 2,  1.0),
    "bad_count"     : (40007, 1,  1.0),
    "cycle_time"    : (40008, 1,  10.0),  # stored × 10, read ÷ 10 → seconds
    "temperature"   : (40009, 1,  10.0),  # stored × 10, read ÷ 10 → °C
    "speed"         : (40010, 1,  10.0),  # stored × 10, read ÷ 10 → unit
}

MODBUS_STATUS_ENCODING: dict[str, int] = {
    "RUN"           : 1,
    "IDLE"          : 2,
    "FAULT"         : 3,
    "MAINTENANCE"   : 4,
}

# ── MQTT constants ────────────────────────────────────────────────────────────
MQTT_QOS_DEFAULT    = 1             # At-least-once delivery
MQTT_RETAINED       = False         # No retained messages for live data
MQTT_CLIENT_PREFIX  = "mfi-reader" # Client ID prefix

# Topic structure: mfi/{site_id}/{machine_id}/{subtopic}
MQTT_TOPIC_DATA     = "mfi/{site}/{machine}/data"
MQTT_TOPIC_STATUS   = "mfi/{site}/{machine}/status"
MQTT_TOPIC_ALARM    = "mfi/{site}/{machine}/alarm"
MQTT_TOPIC_METRICS  = "mfi/{site}/{machine}/metrics"

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
    Build and return a PhaseAdapter-wrapped logger for Phase 2.

    Args:
        name  : Logger namespace.
        level : Logging level (default DEBUG).

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


LOG = build_logger("mfi.phase02")

# =============================================================================
# SECTION 4 — RAW PAYLOAD DATA MODEL
# =============================================================================

class RawPayload:
    """
    Protocol-agnostic envelope for a raw reader output.

    Attributes:
        protocol    : Protocol name ("opcua" | "modbus" | "mqtt").
        machine_id  : Source machine identifier.
        site_id     : Source site identifier.
        timestamp   : ISO 8601 UTC acquisition timestamp.
        raw         : Protocol-specific dictionary payload.
    """

    __slots__ = ("protocol", "machine_id", "site_id", "timestamp", "raw")

    def __init__(
        self,
        protocol    : str,
        machine_id  : str,
        site_id     : str,
        timestamp   : str,
        raw         : dict[str, Any],
    ) -> None:
        self.protocol   = protocol
        self.machine_id = machine_id
        self.site_id    = site_id
        self.timestamp  = timestamp
        self.raw        = raw

    def to_dict(self) -> dict[str, Any]:
        """Return envelope as a plain dictionary."""
        return {
            "protocol"   : self.protocol,
            "machine_id" : self.machine_id,
            "site_id"    : self.site_id,
            "timestamp"  : self.timestamp,
            "raw"        : self.raw,
        }

    def to_json(self, indent: int = 2) -> str:
        """Return envelope as a formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def __repr__(self) -> str:
        return (
            f"RawPayload(protocol={self.protocol!r}, "
            f"machine_id={self.machine_id!r}, "
            f"ts={self.timestamp!r})"
        )


# =============================================================================
# SECTION 5 — ABSTRACT READER BASE
# =============================================================================

class ReaderBase(abc.ABC):
    """
    Abstract base for all MFI protocol readers.

    Subclasses must implement `read()`.
    The reader contract:
      - Takes a MachineSnapshot (simulated or real via adapter).
      - Returns a RawPayload containing protocol-native data structure.
      - NEVER produces MFI standard JSON directly — that is Phase 3's job.
      - NEVER raises silent exceptions — logs and re-raises.
    """

    PROTOCOL_NAME: str = ""     # Overridden by each subclass

    def __init__(self, site_id: str = DEFAULT_SITE) -> None:
        """
        Args:
            site_id : Site context for topic/node generation.
        """
        self.site_id    = site_id
        self._logger    = build_logger(f"mfi.reader.{self.PROTOCOL_NAME}")

    @abc.abstractmethod
    def read(self, snapshot: MachineSnapshot) -> RawPayload:
        """
        Convert a MachineSnapshot into a protocol-native RawPayload.

        Args:
            snapshot : Machine state produced by Phase 1 simulator.

        Returns:
            RawPayload with protocol-flavored `raw` dict.

        Raises:
            ValueError : If snapshot fields are invalid or missing.
        """
        ...

    def _now_utc(self) -> str:
        """Return current UTC time as ISO 8601 string."""
        return datetime.now(timezone.utc).isoformat()

    def _server_timestamp(self, lag_ms: int = 0) -> str:
        """
        Return server-side timestamp offset by lag_ms milliseconds.

        Args:
            lag_ms : Simulated processing lag in milliseconds.

        Returns:
            ISO 8601 UTC timestamp string.
        """
        ts = datetime.now(timezone.utc) + timedelta(milliseconds=lag_ms)
        return ts.isoformat()


# =============================================================================
# SECTION 6 — OPC UA READER
# =============================================================================

class OPCUAReader(ReaderBase):
    """
    Simulated OPC UA reader.

    Produces OPC UA DataValue payloads mimicking what asyncua would return
    from a real OPC UA server. Each machine field is exposed as a distinct
    node within namespace ns=2.

    Node ID format: ns=2;s={machine_id}.{NodeName}
    Example        : ns=2;s=machine01.Status

    Payload structure mirrors OPC UA DataValue specification:
      - Value         : The typed value
      - DataType      : OPC UA data type name
      - StatusCode    : Good / Bad / Uncertain + hex code
      - SourceTimestamp : When the PLC/device set the value
      - ServerTimestamp : When the OPC UA server received the value
    """

    PROTOCOL_NAME = "opcua"

    def __init__(self, site_id: str = DEFAULT_SITE) -> None:
        super().__init__(site_id)

    # ── Node ID builder ───────────────────────────────────────────────────

    def _node_id(self, machine_id: str, node_name: str) -> str:
        """
        Build an OPC UA NodeIdentifier string.

        Args:
            machine_id : Machine identifier.
            node_name  : Field name (e.g., "Status", "TemperatureC").

        Returns:
            String like "ns=2;s=machine01.Status"
        """
        return f"ns={OPCUA_NAMESPACE};s={machine_id}.{node_name}"

    # ── Status code resolver ──────────────────────────────────────────────

    def _status_code(self, snapshot: MachineSnapshot) -> tuple[str, str]:
        """
        Determine OPC UA StatusCode from machine status.

        - FAULT       → Uncertain (data may be unreliable)
        - MAINTENANCE → Good (machine is reachable, just offline)
        - Others      → Good

        Args:
            snapshot : Machine snapshot.

        Returns:
            (status_name, hex_code) tuple.
        """
        if snapshot.status == MachineStatus.FAULT.value:
            name = "Uncertain"
        else:
            name = "Good"
        return name, OPCUA_STATUS_CODES[name]

    # ── DataValue builder ─────────────────────────────────────────────────

    def _data_value(
        self,
        machine_id      : str,
        node_name       : str,
        data_type       : str,
        value           : Any,
        status_name     : str,
        status_hex      : str,
        source_ts       : str,
        server_ts       : str,
    ) -> dict[str, Any]:
        """
        Build one OPC UA DataValue dict.

        Args:
            machine_id  : For node ID construction.
            node_name   : OPC UA node field name.
            data_type   : OPC UA data type string.
            value       : The typed value for this node.
            status_name : Status code name (Good/Uncertain/Bad).
            status_hex  : Hex representation of status code.
            source_ts   : Source (PLC) timestamp.
            server_ts   : Server (OPC UA server) timestamp.

        Returns:
            Dict representing one OPC UA DataValue.
        """
        return {
            "NodeId"          : self._node_id(machine_id, node_name),
            "DisplayName"     : node_name,
            "DataType"        : data_type,
            "Value"           : value,
            "StatusCode"      : {"Name": status_name, "Code": status_hex},
            "SourceTimestamp" : source_ts,
            "ServerTimestamp" : server_ts,
        }

    # ── Main read ─────────────────────────────────────────────────────────

    def read(self, snapshot: MachineSnapshot) -> RawPayload:
        """
        Produce OPC UA DataValue payload from machine snapshot.

        Returns a RawPayload whose `raw` dict contains:
          - endpoint    : Simulated OPC UA server endpoint URL
          - session_id  : Simulated session identifier
          - nodes       : List of DataValue dicts (one per field)

        Args:
            snapshot : Machine state from Phase 1.

        Returns:
            RawPayload with OPC UA-flavored raw dict.
        """
        source_ts   = snapshot.timestamp
        server_ts   = self._server_timestamp(lag_ms=OPCUA_SERVER_LAG_MS)
        status_name, status_hex = self._status_code(snapshot)

        # --- Map snapshot fields to OPC UA typed nodes ---
        field_values: dict[str, tuple[str, Any]] = {
            "Status"        : ("String",  snapshot.status),
            "AlarmCode"     : ("Int32",   snapshot.alarm_code),
            "PieceCount"    : ("Int64",   snapshot.piece_count),
            "GoodCount"     : ("Int32",   snapshot.good_count),
            "BadCount"      : ("Int32",   snapshot.bad_count),
            "CycleTimeSec"  : ("Float",   snapshot.cycle_time_sec),
            "TemperatureC"  : ("Float",   snapshot.temperature_c),
            "Speed"         : ("Float",   snapshot.speed),
            "MachineType"   : ("String",  snapshot.machine_type),
            "Protocol"      : ("String",  snapshot.protocol),
            "SiteId"        : ("String",  snapshot.site_id),
        }

        nodes = [
            self._data_value(
                machine_id  = snapshot.machine_id,
                node_name   = node_name,
                data_type   = dtype,
                value       = val,
                status_name = status_name,
                status_hex  = status_hex,
                source_ts   = source_ts,
                server_ts   = server_ts,
            )
            for node_name, (dtype, val) in field_values.items()
        ]

        raw = {
            "endpoint"   : f"opc.tcp://sim-{snapshot.machine_id}.local:4840",
            "session_id" : f"ses-{snapshot.machine_id}-{OPCUA_NAMESPACE}",
            "namespace"  : OPCUA_NAMESPACE,
            "node_count" : len(nodes),
            "nodes"      : nodes,
        }

        self._logger.debug(
            "OPC UA read │ %-10s │ status=%s │ nodes=%d │ sc=%s",
            snapshot.machine_id,
            snapshot.status,
            len(nodes),
            status_name,
        )

        return RawPayload(
            protocol    = self.PROTOCOL_NAME,
            machine_id  = snapshot.machine_id,
            site_id     = snapshot.site_id,
            timestamp   = server_ts,
            raw         = raw,
        )


# =============================================================================
# SECTION 7 — MODBUS TCP READER
# =============================================================================

class ModbusTCPReader(ReaderBase):
    """
    Simulated Modbus TCP reader.

    Produces Modbus FC03 (Read Holding Registers) response payloads
    mimicking what pymodbus would return from a real PLC/device.

    Each machine field is encoded as one or two 16-bit holding registers
    per MODBUS_REGISTER_MAP. 32-bit values (piece counts) use two consecutive
    registers (high word / low word split).

    Payload structure:
      - unit_id       : Modbus unit (slave) ID
      - transaction_id: Modbus TCP transaction identifier
      - function_code : Always 3 (FC03 Read Holding Registers)
      - ip            : Simulated device IP address
      - port          : 502 (standard Modbus TCP port)
      - register_blocks: List of register read results
    """

    PROTOCOL_NAME   = "modbus"
    _tx_counter     = 0    # Class-level transaction ID counter

    def __init__(self, site_id: str = DEFAULT_SITE) -> None:
        super().__init__(site_id)

    # ── Transaction ID ────────────────────────────────────────────────────

    @classmethod
    def _next_tx_id(cls) -> int:
        """Increment and return the next Modbus TCP transaction ID (0–65535)."""
        cls._tx_counter = (cls._tx_counter + 1) % 65536
        return cls._tx_counter

    # ── Value encoders ────────────────────────────────────────────────────

    @staticmethod
    def _encode_16(value: float, scale: float) -> list[int]:
        """
        Encode a scalar value into one 16-bit register (clamped 0–65535).

        Args:
            value : Real-world value.
            scale : Divide factor (stored = value × scale, clamped).

        Returns:
            Single-element list with 16-bit int.
        """
        raw = int(round(value * scale))
        raw = max(0, min(65535, raw))
        return [raw]

    @staticmethod
    def _encode_32(value: int) -> list[int]:
        """
        Encode a 32-bit integer into two 16-bit registers [high, low].

        Args:
            value : Integer value (0–4 294 967 295).

        Returns:
            Two-element list [high_word, low_word].
        """
        value = max(0, min(0xFFFF_FFFF, value))
        high  = (value >> 16) & 0xFFFF
        low   = value & 0xFFFF
        return [high, low]

    # ── Register block builder ────────────────────────────────────────────

    def _build_register_block(
        self,
        field   : str,
        address : int,
        count   : int,
        scale   : float,
        value   : Any,
    ) -> dict[str, Any]:
        """
        Build one Modbus register read result block.

        Args:
            field   : Field name (for readability in payload).
            address : Starting register address.
            count   : Number of registers read.
            scale   : Scale factor used for encoding.
            value   : Raw value encoded into registers.

        Returns:
            Dict with address, count, registers (raw ints), and decoded value.
        """
        if field == "status":
            raw_regs = [MODBUS_STATUS_ENCODING.get(str(value), 0)]
        elif count == 2:
            raw_regs = self._encode_32(int(value))
        else:
            raw_regs = self._encode_16(float(value), scale)

        return {
            "field"          : field,
            "start_address"  : address,
            "register_count" : count,
            "raw_registers"  : raw_regs,
            "scale_factor"   : scale,
        }

    # ── Main read ─────────────────────────────────────────────────────────

    def read(self, snapshot: MachineSnapshot) -> RawPayload:
        """
        Produce Modbus TCP FC03 payload from machine snapshot.

        Returns a RawPayload whose `raw` dict contains:
          - unit_id          : Slave unit ID (derived from machine index)
          - transaction_id   : Auto-incremented Modbus TCP TID
          - function_code    : 3 (FC03)
          - ip / port        : Simulated device endpoint
          - register_blocks  : One block per field in MODBUS_REGISTER_MAP
          - error            : None if Good, error string if FAULT

        Args:
            snapshot : Machine state from Phase 1.

        Returns:
            RawPayload with Modbus TCP-flavored raw dict.
        """
        tx_id   = self._next_tx_id()
        # Derive a unit_id (1–247 range) from machine_id suffix
        unit_id = (int(snapshot.machine_id.replace("machine", "")) % 247) + 1
        # Simulate IP: 192.168.10.{unit_id}
        ip      = f"192.168.10.{unit_id}"

        # Error simulation: FAULT machines may return Modbus exception
        modbus_error: Optional[str] = None
        if snapshot.status == MachineStatus.FAULT.value:
            import random
            if random.random() < 0.15:
                modbus_error = "ExceptionCode: 0x04 (SERVER_DEVICE_FAILURE)"

        # --- Build register blocks for each mapped field ---
        field_values: dict[str, Any] = {
            "status"        : snapshot.status,
            "alarm_code"    : snapshot.alarm_code,
            "piece_count"   : snapshot.piece_count,
            "good_count"    : snapshot.good_count,
            "bad_count"     : snapshot.bad_count,
            "cycle_time"    : snapshot.cycle_time_sec,
            "temperature"   : snapshot.temperature_c,
            "speed"         : snapshot.speed,
        }

        register_blocks = []
        for field, (address, count, scale) in MODBUS_REGISTER_MAP.items():
            block = self._build_register_block(
                field   = field,
                address = address,
                count   = count,
                scale   = scale,
                value   = field_values[field],
            )
            register_blocks.append(block)

        raw = {
            "unit_id"           : unit_id,
            "transaction_id"    : tx_id,
            "protocol_id"       : MODBUS_PROTOCOL_ID,
            "function_code"     : MODBUS_FUNCTION_CODE,
            "function_name"     : "READ_HOLDING_REGISTERS",
            "ip"                : ip,
            "port"              : 502,
            "register_blocks"   : register_blocks,
            "error"             : modbus_error,
        }

        self._logger.debug(
            "Modbus read │ %-10s │ unit=%3d │ tx=%5d │ ip=%-15s │ error=%s",
            snapshot.machine_id,
            unit_id,
            tx_id,
            ip,
            modbus_error or "None",
        )

        return RawPayload(
            protocol    = self.PROTOCOL_NAME,
            machine_id  = snapshot.machine_id,
            site_id     = snapshot.site_id,
            timestamp   = self._now_utc(),
            raw         = raw,
        )


# =============================================================================
# SECTION 8 — MQTT READER
# =============================================================================

class MQTTReader(ReaderBase):
    """
    Simulated MQTT reader.

    Produces MQTT message payloads mimicking what paho-mqtt would deliver
    from a real MQTT broker subscription.

    Each machine publishes to four sub-topics:
      mfi/{site}/{machine}/data     — Full normalized snapshot as JSON payload
      mfi/{site}/{machine}/status   — Status string only
      mfi/{site}/{machine}/alarm    — Alarm code + description
      mfi/{site}/{machine}/metrics  — Numeric KPIs only

    Payload structure:
      - topic       : Full MQTT topic string
      - qos         : Quality of service level (0/1/2)
      - retained    : Whether broker retains message
      - client_id   : Simulated publisher client ID
      - messages    : List of MQTT message dicts (one per sub-topic)
    """

    PROTOCOL_NAME = "mqtt"

    # Alarm code descriptions (mirrors Phase 1 ALARM_CODES)
    ALARM_DESCRIPTIONS: dict[int, str] = {
        0   : "NO_ALARM",
        100 : "OVERTEMP",
        101 : "OVERSPEED",
        102 : "MOTOR_FAULT",
        200 : "COMM_ERROR",
        201 : "SENSOR_FAIL",
        300 : "E_STOP",
        301 : "DOOR_OPEN",
        400 : "MAINTENANCE_DUE",
        500 : "POWER_FLUCTUATION",
    }

    def __init__(self, site_id: str = DEFAULT_SITE) -> None:
        super().__init__(site_id)

    # ── Topic builder ─────────────────────────────────────────────────────

    def _topic(self, template: str, snapshot: MachineSnapshot) -> str:
        """
        Build an MQTT topic string from a template.

        Args:
            template : Topic template with {site} and {machine} placeholders.
            snapshot : Machine snapshot for substitution.

        Returns:
            Fully qualified MQTT topic string.
        """
        return template.format(
            site    = snapshot.site_id,
            machine = snapshot.machine_id,
        )

    # ── Message builders ──────────────────────────────────────────────────

    def _msg_data(self, snapshot: MachineSnapshot, ts: str) -> dict[str, Any]:
        """
        Build the /data sub-topic MQTT message payload.
        Contains the full machine snapshot as a flat JSON dict.

        Args:
            snapshot : Machine snapshot.
            ts       : Acquisition timestamp.

        Returns:
            Dict with topic, qos, retained, payload.
        """
        return {
            "topic"   : self._topic(MQTT_TOPIC_DATA, snapshot),
            "qos"     : MQTT_QOS_DEFAULT,
            "retained": MQTT_RETAINED,
            "payload" : {
                "site_id"       : snapshot.site_id,
                "machine_id"    : snapshot.machine_id,
                "machine_type"  : snapshot.machine_type,
                "protocol"      : snapshot.protocol,
                "status"        : snapshot.status,
                "alarm_code"    : snapshot.alarm_code,
                "piece_count"   : snapshot.piece_count,
                "good_count"    : snapshot.good_count,
                "bad_count"     : snapshot.bad_count,
                "cycle_time_sec": snapshot.cycle_time_sec,
                "temperature_c" : snapshot.temperature_c,
                "speed"         : snapshot.speed,
                "timestamp"     : ts,
            },
        }

    def _msg_status(self, snapshot: MachineSnapshot) -> dict[str, Any]:
        """
        Build the /status sub-topic MQTT message payload.
        Lightweight — status string only.

        Args:
            snapshot : Machine snapshot.

        Returns:
            Dict with topic, qos, retained, payload.
        """
        return {
            "topic"   : self._topic(MQTT_TOPIC_STATUS, snapshot),
            "qos"     : MQTT_QOS_DEFAULT,
            "retained": True,       # Status IS retained (last known state)
            "payload" : {
                "machine_id" : snapshot.machine_id,
                "status"     : snapshot.status,
            },
        }

    def _msg_alarm(self, snapshot: MachineSnapshot) -> dict[str, Any]:
        """
        Build the /alarm sub-topic MQTT message payload.
        Alarm code + human-readable description.

        Args:
            snapshot : Machine snapshot.

        Returns:
            Dict with topic, qos, retained, payload.
        """
        return {
            "topic"   : self._topic(MQTT_TOPIC_ALARM, snapshot),
            "qos"     : 2,          # Exactly-once for alarms
            "retained": False,
            "payload" : {
                "machine_id"  : snapshot.machine_id,
                "alarm_code"  : snapshot.alarm_code,
                "alarm_desc"  : self.ALARM_DESCRIPTIONS.get(
                    snapshot.alarm_code, "UNKNOWN"
                ),
                "severity"    : (
                    "CRITICAL" if snapshot.alarm_code >= 300
                    else "WARNING" if snapshot.alarm_code > 0
                    else "NONE"
                ),
            },
        }

    def _msg_metrics(self, snapshot: MachineSnapshot) -> dict[str, Any]:
        """
        Build the /metrics sub-topic MQTT message payload.
        Numeric KPIs only — for lightweight downstream consumers.

        Args:
            snapshot : Machine snapshot.

        Returns:
            Dict with topic, qos, retained, payload.
        """
        return {
            "topic"   : self._topic(MQTT_TOPIC_METRICS, snapshot),
            "qos"     : 0,          # Best-effort for high-frequency metrics
            "retained": False,
            "payload" : {
                "machine_id"    : snapshot.machine_id,
                "piece_count"   : snapshot.piece_count,
                "good_count"    : snapshot.good_count,
                "bad_count"     : snapshot.bad_count,
                "cycle_time_sec": snapshot.cycle_time_sec,
                "temperature_c" : snapshot.temperature_c,
                "speed"         : snapshot.speed,
            },
        }

    # ── Main read ─────────────────────────────────────────────────────────

    def read(self, snapshot: MachineSnapshot) -> RawPayload:
        """
        Produce MQTT message payloads from machine snapshot.

        Returns a RawPayload whose `raw` dict contains:
          - broker      : Simulated broker address
          - client_id   : Simulated MQTT client identifier
          - messages    : List of 4 MQTT message dicts (data/status/alarm/metrics)

        Args:
            snapshot : Machine state from Phase 1.

        Returns:
            RawPayload with MQTT-flavored raw dict.
        """
        ts          = snapshot.timestamp
        client_id   = f"{MQTT_CLIENT_PREFIX}-{snapshot.site_id}-{snapshot.machine_id}"

        messages = [
            self._msg_data(snapshot, ts),
            self._msg_status(snapshot),
            self._msg_alarm(snapshot),
            self._msg_metrics(snapshot),
        ]

        raw = {
            "broker"    : "mqtt://localhost:1883",
            "client_id" : client_id,
            "message_count" : len(messages),
            "messages"  : messages,
        }

        self._logger.debug(
            "MQTT read  │ %-10s │ status=%s │ alarm=%3d │ msgs=%d │ topics=%s",
            snapshot.machine_id,
            snapshot.status,
            snapshot.alarm_code,
            len(messages),
            [m["topic"] for m in messages],
        )

        return RawPayload(
            protocol    = self.PROTOCOL_NAME,
            machine_id  = snapshot.machine_id,
            site_id     = snapshot.site_id,
            timestamp   = ts,
            raw         = raw,
        )


# =============================================================================
# SECTION 9 — READER REGISTRY
# =============================================================================

class ReaderRegistry:
    """
    Central registry mapping protocol names to reader instances.

    Readers are instantiated once at registry creation and reused.
    Only registered protocols are accepted; unknown protocols raise ValueError.

    Usage:
        registry = ReaderRegistry(site_id="mecanitec")
        payload  = registry.read(snapshot)    # auto-dispatch by protocol
    """

    SUPPORTED_PROTOCOLS = ("opcua", "modbus", "mqtt")

    def __init__(self, site_id: str = DEFAULT_SITE) -> None:
        """
        Instantiate all registered readers.

        Args:
            site_id : Site context passed to each reader.
        """
        self._readers: dict[str, ReaderBase] = {
            "opcua"  : OPCUAReader(site_id),
            "modbus" : ModbusTCPReader(site_id),
            "mqtt"   : MQTTReader(site_id),
        }
        LOG.info(
            "ReaderRegistry initialized | site=%s | protocols=%s",
            site_id,
            list(self._readers.keys()),
        )

    def read(self, snapshot: MachineSnapshot) -> RawPayload:
        """
        Dispatch a snapshot to the correct reader based on its protocol.

        Args:
            snapshot : Machine snapshot containing protocol field.

        Returns:
            RawPayload from the matching reader.

        Raises:
            ValueError : If snapshot.protocol is not registered.
        """
        protocol = snapshot.protocol.lower()
        reader   = self._readers.get(protocol)

        if reader is None:
            raise ValueError(
                f"Unsupported protocol '{protocol}' for machine "
                f"'{snapshot.machine_id}'. "
                f"Registered: {list(self._readers.keys())}"
            )

        return reader.read(snapshot)

    def read_all(
        self,
        snapshots: list[MachineSnapshot],
    ) -> list[RawPayload]:
        """
        Read all snapshots and return one RawPayload per machine.

        Failed reads are logged and skipped (no silent swallowing —
        error is logged with full context before continuing).

        Args:
            snapshots : List of machine snapshots (from Phase 1 cycle).

        Returns:
            List of RawPayload (may be shorter than snapshots if errors occur).
        """
        payloads: list[RawPayload] = []
        errors  : int = 0

        for snap in snapshots:
            try:
                payload = self.read(snap)
                payloads.append(payload)
            except Exception as exc:
                LOG.error(
                    "Read FAILED │ machine=%s │ protocol=%s │ error=%s",
                    snap.machine_id,
                    snap.protocol,
                    exc,
                )
                errors += 1

        LOG.info(
            "read_all complete │ total=%d │ ok=%d │ errors=%d",
            len(snapshots),
            len(payloads),
            errors,
        )
        return payloads

    def get_reader(self, protocol: str) -> ReaderBase:
        """
        Return a specific reader by protocol name.

        Args:
            protocol : Protocol name string.

        Returns:
            Matching ReaderBase instance.

        Raises:
            ValueError : If protocol not registered.
        """
        reader = self._readers.get(protocol.lower())
        if reader is None:
            raise ValueError(
                f"Protocol '{protocol}' not registered. "
                f"Available: {list(self._readers.keys())}"
            )
        return reader


# =============================================================================
# SECTION 10 — CYCLE SUMMARY
# =============================================================================

def print_reader_summary(
    cycle_num : int,
    payloads  : list[RawPayload],
) -> None:
    """
    Print a concise summary of one reader cycle.

    Args:
        cycle_num : Current cycle number (1-based).
        payloads  : All RawPayloads produced this cycle.
    """
    proto_counts: dict[str, int] = {}
    for p in payloads:
        proto_counts[p.protocol] = proto_counts.get(p.protocol, 0) + 1

    LOG.info(
        "─── READER CYCLE %02d ─── total=%d │ %s",
        cycle_num,
        len(payloads),
        " │ ".join(f"{k}={v}" for k, v in sorted(proto_counts.items())),
    )


# =============================================================================
# SECTION 11 — OUTPUT WRITER
# =============================================================================

def write_output(payloads: list[RawPayload], filepath: str) -> None:
    """
    Write all RawPayloads to a JSON file.

    Args:
        payloads : List of RawPayload to serialize.
        filepath : Destination file path.
    """
    data = [p.to_dict() for p in payloads]
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    LOG.info("Output written → %s (%d payloads)", filepath, len(data))


# =============================================================================
# SECTION 12 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 2. Validates:
      1.  ReaderRegistry initializes with 3 protocols.
      2.  OPCUAReader produces payload with correct structure.
      3.  OPCUAReader nodes count matches OPCUA_NODE_MAP.
      4.  ModbusTCPReader produces payload with register_blocks.
      5.  ModbusTCPReader register block count matches MODBUS_REGISTER_MAP.
      6.  ModbusTCPReader 32-bit encoding is correct (piece_count high/low).
      7.  MQTTReader produces 4 messages per machine.
      8.  MQTTReader topic format is correct.
      9.  All protocols produce valid JSON-serializable output.
      10. ReaderRegistry auto-dispatches by snapshot.protocol field.
      11. Unknown protocol raises ValueError.
      12. read_all produces one payload per machine in fleet.

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

    # --- Build one snapshot for testing ---
    fleet       = build_machine_fleet()
    acc: dict[str, int] = {}
    snapshots   = run_simulation_cycle(fleet, acc)

    # Force one snapshot of each protocol type
    def _force_snap(proto: str) -> MachineSnapshot:
        """Find (or patch) a snapshot with the given protocol."""
        for s in snapshots:
            if s.protocol == proto:
                return s
        # Patch the first snapshot's protocol field for test isolation
        s = snapshots[0]
        import dataclasses
        return dataclasses.replace(s, protocol=proto)

    snap_opcua  = _force_snap("opcua")
    snap_modbus = _force_snap("modbus")
    snap_mqtt   = _force_snap("mqtt")

    # --- Test 1: Registry init ---
    registry = ReaderRegistry()
    check(
        "ReaderRegistry has 3 protocols",
        len(registry.SUPPORTED_PROTOCOLS) == 3,
    )

    # --- Test 2: OPC UA payload structure ---
    pl_opcua = OPCUAReader().read(snap_opcua)
    check("OPCUAReader returns RawPayload", isinstance(pl_opcua, RawPayload))
    check(
        "OPCUAReader protocol field == 'opcua'",
        pl_opcua.protocol == "opcua",
    )

    # --- Test 3: OPC UA node count ---
    node_count = pl_opcua.raw.get("node_count", 0)
    check(
        f"OPCUAReader node_count == {len(OPCUA_NODE_MAP)}",
        node_count == len(OPCUA_NODE_MAP),
        f"got {node_count}",
    )

    # --- Test 4: Modbus payload structure ---
    pl_modbus = ModbusTCPReader().read(snap_modbus)
    check("ModbusTCPReader returns RawPayload", isinstance(pl_modbus, RawPayload))
    check(
        "ModbusTCPReader has register_blocks",
        "register_blocks" in pl_modbus.raw,
    )

    # --- Test 5: Modbus register block count ---
    rb_count = len(pl_modbus.raw.get("register_blocks", []))
    check(
        f"Modbus register_blocks count == {len(MODBUS_REGISTER_MAP)}",
        rb_count == len(MODBUS_REGISTER_MAP),
        f"got {rb_count}",
    )

    # --- Test 6: Modbus 32-bit encoding ---
    piece_block = next(
        (b for b in pl_modbus.raw["register_blocks"] if b["field"] == "piece_count"),
        None,
    )
    if piece_block:
        regs    = piece_block["raw_registers"]
        decoded = (regs[0] << 16) | regs[1]
        check(
            "Modbus 32-bit piece_count encodes/decodes correctly",
            decoded == snap_modbus.piece_count,
            f"encoded={regs} decoded={decoded} expected={snap_modbus.piece_count}",
        )
    else:
        check("Modbus piece_count block exists", False, "block not found")

    # --- Test 7: MQTT message count ---
    pl_mqtt = MQTTReader().read(snap_mqtt)
    msg_count = pl_mqtt.raw.get("message_count", 0)
    check(
        "MQTTReader produces 4 messages",
        msg_count == 4,
        f"got {msg_count}",
    )

    # --- Test 8: MQTT topic format ---
    messages = pl_mqtt.raw.get("messages", [])
    topics   = [m["topic"] for m in messages]
    expected_prefix = f"mfi/{snap_mqtt.site_id}/{snap_mqtt.machine_id}/"
    topics_ok = all(t.startswith(expected_prefix) for t in topics)
    check(
        f"MQTT topics start with '{expected_prefix}'",
        topics_ok,
        str(topics),
    )

    # --- Test 9: JSON serializable ---
    for proto, pl in [("opcua", pl_opcua), ("modbus", pl_modbus), ("mqtt", pl_mqtt)]:
        try:
            _ = json.dumps(pl.to_dict())
            serial_ok = True
        except (TypeError, ValueError) as exc:
            serial_ok = False
        check(f"{proto.upper()} payload is JSON-serializable", serial_ok)

    # --- Test 10: Registry auto-dispatch ---
    dispatched = registry.read(snap_opcua)
    check(
        "Registry dispatches opcua → OPCUAReader",
        dispatched.protocol == "opcua",
    )

    # --- Test 11: Unknown protocol raises ValueError ---
    import dataclasses as dc
    bad_snap = dc.replace(snap_opcua, protocol="profinet")
    try:
        registry.read(bad_snap)
        check("Unknown protocol raises ValueError", False, "no exception raised")
    except ValueError:
        check("Unknown protocol raises ValueError", True)

    # --- Test 12: read_all produces one payload per machine ---
    all_payloads = registry.read_all(snapshots)
    check(
        "read_all produces one payload per machine",
        len(all_payloads) == len(snapshots),
        f"got {len(all_payloads)} expected {len(snapshots)}",
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
    """Build and return the CLI argument parser for Phase 2."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_02_readers.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Simulate OPC UA, Modbus TCP, and MQTT readers over Phase 1 fleet."
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
        help    = f"Number of simulation cycles to run (default: {DEFAULT_CYCLES}).",
    )
    parser.add_argument(
        "--protocol",
        type    = str,
        default = DEFAULT_PROTOCOL,
        choices = ["all", "opcua", "modbus", "mqtt"],
        help    = "Filter output to a specific protocol (default: all).",
    )
    parser.add_argument(
        "--output",
        type    = str,
        default = None,
        metavar = "FILE",
        help    = "Write last cycle raw payloads to a JSON file.",
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
    Phase 2 entry point.

    Modes:
      --self-test  : Run validation suite.
      (default)    : Run N simulation cycles through readers, print summaries.
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

    # ── Simulation + Reader mode ──────────────────────────────────────────
    LOG.info(
        "Starting reader loop: site=%s | protocol=%s | cycles=%d | interval=%.1fs",
        args.site,
        args.protocol,
        args.cycles,
        args.interval,
    )

    fleet       = build_machine_fleet(site_id=args.site)
    registry    = ReaderRegistry(site_id=args.site)
    acc: dict[str, int] = {}
    last_payloads: list[RawPayload] = []

    for cycle in range(1, args.cycles + 1):
        LOG.info("── Cycle %02d / %02d ──", cycle, args.cycles)

        snapshots   = run_simulation_cycle(fleet, acc)
        payloads    = registry.read_all(snapshots)

        # Filter by protocol if requested
        if args.protocol != "all":
            payloads = [p for p in payloads if p.protocol == args.protocol]

        print_reader_summary(cycle, payloads)
        last_payloads = payloads

        if cycle < args.cycles:
            time.sleep(args.interval)

    # ── Optional output ───────────────────────────────────────────────────
    if args.output:
        write_output(last_payloads, args.output)

    # ── Phase footer ──────────────────────────────────────────────────────
    LOG.info(
        "Phase %s complete. Cycles=%d | Last payloads=%d",
        PHASE_ID,
        args.cycles,
        len(last_payloads),
    )


# =============================================================================
# SECTION 14 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
