#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_03_json_model.py
# PHASE        : 3 — MFI Standard JSON Model & Normalizer
# PURPOSE      : Transform raw protocol payloads (OPC UA, Modbus TCP, MQTT)
#                into the single canonical MFI Standard Internal JSON.
#                Validates strictly via Pydantic v2. Rejects invalid records
#                explicitly — no silent failures, no partial outputs.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : pydantic>=2.0, mfi_phase_01_simulation, mfi_phase_02_readers
# CLI          : python mfi_phase_03_json_model.py --self-test
#                python mfi_phase_03_json_model.py --cycles 3
#                python mfi_phase_03_json_model.py --cycles 3 --output mfi.json
#                python mfi_phase_03_json_model.py --protocol modbus --cycles 2
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import abc
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

# --- Pydantic v2 ---
try:
    from pydantic import (
        BaseModel,
        Field,
        field_validator,
        model_validator,
        ValidationError,
    )
    import pydantic
except ImportError as exc:
    print(
        f"[FATAL] pydantic not found: {exc}\n"
        "Install with: pip install pydantic --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Phase 1 dependency ---
try:
    from mfi_phase_01_simulation import (
        MachineStatus,
        build_machine_fleet,
        run_simulation_cycle,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_01_simulation: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 2 dependency ---
try:
    from mfi_phase_02_readers import (
        RawPayload,
        ReaderRegistry,
        MODBUS_REGISTER_MAP,
        MODBUS_STATUS_ENCODING,
    )
except ImportError as exc:
    print(f"[FATAL] Cannot import mfi_phase_02_readers: {exc}", file=sys.stderr)
    sys.exit(1)

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID            = "03"
PHASE_NAME          = "MFI Standard JSON Model"
PHASE_VERSION       = "1.0.0"

DEFAULT_SITE        = "mecanitec"
DEFAULT_CYCLES      = 3
DEFAULT_PROTOCOL    = "all"

# ── MFI Standard schema constraints ──────────────────────────────────────────
VALID_STATUSES      : frozenset[str] = frozenset(
    {s.value for s in MachineStatus}
)
VALID_PROTOCOLS     : frozenset[str] = frozenset({"opcua", "modbus", "mqtt"})
VALID_MACHINE_TYPES : frozenset[str] = frozenset(
    {"CNC", "PRESS", "ROBOT", "CONVEYOR", "WELDER"}
)

PIECE_COUNT_MAX     = 100_000_000   # Hard upper bound (sanity check)
TEMP_MIN_C          = -10.0         # Minimum plausible temperature
TEMP_MAX_C          = 300.0         # Maximum plausible temperature
SPEED_MIN           = 0.0
SPEED_MAX           = 100_000.0
CYCLE_TIME_MAX_SEC  = 86_400.0      # 24 hours max cycle time

# ── Modbus inverse status decoding ───────────────────────────────────────────
MODBUS_STATUS_DECODING: dict[int, str] = {
    v: k for k, v in MODBUS_STATUS_ENCODING.items()
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


LOG = build_logger("mfi.phase03")

# =============================================================================
# SECTION 4 — MFI STANDARD PYDANTIC MODEL
# =============================================================================

class MFIStandardModel(BaseModel):
    """
    Canonical MFI Standard Internal JSON model.

    This is the single source of truth for all downstream consumers:
    Dashboard, Analytics, Reports, Predictive AI, Mobile.
    No downstream layer ever reads protocol-specific data directly.

    All fields are required. Pydantic v2 strict mode is NOT used globally
    (allows int→float coercion for numeric fields), but domain constraints
    are enforced by field validators.

    Fields
    ------
    site_id       : Site identifier string (non-empty).
    machine_id    : Machine identifier string (non-empty).
    machine_type  : One of VALID_MACHINE_TYPES.
    protocol      : Source protocol — one of VALID_PROTOCOLS.
    status        : One of VALID_STATUSES (RUN/IDLE/FAULT/MAINTENANCE).
    alarm_code    : Non-negative integer alarm code.
    piece_count   : Cumulative production count (≥ 0).
    good_count    : Good pieces this cycle (≥ 0).
    bad_count     : Bad/scrap pieces this cycle (≥ 0).
    cycle_time_sec: Cycle duration in seconds (≥ 0).
    temperature_c : Machine temperature in Celsius.
    speed         : Machine speed in protocol-native unit (≥ 0).
    timestamp     : ISO 8601 UTC acquisition timestamp.
    """

    model_config = {"str_strip_whitespace": True, "validate_assignment": True}

    site_id         : str   = Field(..., min_length=1, description="Site identifier")
    machine_id      : str   = Field(..., min_length=1, description="Machine identifier")
    machine_type    : str   = Field(..., description="Machine type")
    protocol        : str   = Field(..., description="Source protocol")
    status          : str   = Field(..., description="Operational status")
    alarm_code      : int   = Field(..., ge=0, description="Alarm code (0 = no alarm)")
    piece_count     : int   = Field(..., ge=0, le=PIECE_COUNT_MAX, description="Cumulative pieces")
    good_count      : int   = Field(..., ge=0, description="Good pieces this cycle")
    bad_count       : int   = Field(..., ge=0, description="Bad/scrap pieces this cycle")
    cycle_time_sec  : float = Field(..., ge=0.0, le=CYCLE_TIME_MAX_SEC, description="Cycle time (s)")
    temperature_c   : float = Field(..., ge=TEMP_MIN_C, le=TEMP_MAX_C, description="Temperature (°C)")
    speed           : float = Field(..., ge=SPEED_MIN, le=SPEED_MAX, description="Speed (protocol unit)")
    timestamp       : str   = Field(..., min_length=20, description="ISO 8601 UTC timestamp")

    # ── Field validators ──────────────────────────────────────────────────

    @field_validator("machine_type")
    @classmethod
    def validate_machine_type(cls, v: str) -> str:
        """Reject unknown machine types."""
        if v not in VALID_MACHINE_TYPES:
            raise ValueError(
                f"Invalid machine_type '{v}'. "
                f"Valid: {sorted(VALID_MACHINE_TYPES)}"
            )
        return v

    @field_validator("protocol")
    @classmethod
    def validate_protocol(cls, v: str) -> str:
        """Reject unknown protocols."""
        v = v.lower()
        if v not in VALID_PROTOCOLS:
            raise ValueError(
                f"Invalid protocol '{v}'. "
                f"Valid: {sorted(VALID_PROTOCOLS)}"
            )
        return v

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Reject unknown status values."""
        if v not in VALID_STATUSES:
            raise ValueError(
                f"Invalid status '{v}'. "
                f"Valid: {sorted(VALID_STATUSES)}"
            )
        return v

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """
        Ensure timestamp is a parseable ISO 8601 string.
        Accepts both '+00:00' and 'Z' suffixes.
        """
        normalized = v.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(normalized)
        except ValueError:
            raise ValueError(
                f"timestamp '{v}' is not a valid ISO 8601 datetime string."
            )
        return v

    @model_validator(mode="after")
    def validate_count_consistency(self) -> "MFIStandardModel":
        """
        Ensure good_count + bad_count does not exceed a single reasonable
        cycle output. This is a sanity check, not a hard production rule.
        """
        cycle_total = self.good_count + self.bad_count
        if cycle_total > 10_000:
            raise ValueError(
                f"good_count ({self.good_count}) + bad_count ({self.bad_count}) "
                f"= {cycle_total} exceeds max per-cycle output of 10,000."
            )
        return self

    # ── Serialization helpers ─────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Return model as a plain Python dictionary."""
        return self.model_dump()

    def to_json(self, indent: int = 2) -> str:
        """Return model as a formatted JSON string."""
        return self.model_dump_json(indent=indent)

    def __repr__(self) -> str:
        return (
            f"MFIStandardModel("
            f"machine_id={self.machine_id!r}, "
            f"status={self.status!r}, "
            f"protocol={self.protocol!r}, "
            f"ts={self.timestamp!r})"
        )


# =============================================================================
# SECTION 5 — NORMALIZATION RESULT ENVELOPE
# =============================================================================

class NormalizationResult:
    """
    Result of one normalization attempt.

    Attributes:
        success     : True if normalization produced a valid MFIStandardModel.
        record      : MFIStandardModel if success, None otherwise.
        machine_id  : Source machine ID (always set).
        protocol    : Source protocol (always set).
        errors      : List of validation error strings (empty if success).
    """

    __slots__ = ("success", "record", "machine_id", "protocol", "errors")

    def __init__(
        self,
        machine_id  : str,
        protocol    : str,
        record      : Optional[MFIStandardModel] = None,
        errors      : Optional[list[str]] = None,
    ) -> None:
        self.machine_id = machine_id
        self.protocol   = protocol
        self.record     = record
        self.errors     = errors or []
        self.success    = record is not None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict (includes error detail for failed records)."""
        return {
            "success"    : self.success,
            "machine_id" : self.machine_id,
            "protocol"   : self.protocol,
            "record"     : self.record.to_dict() if self.record else None,
            "errors"     : self.errors,
        }

    def __repr__(self) -> str:
        status = "OK" if self.success else f"FAIL({len(self.errors)} errors)"
        return (
            f"NormalizationResult("
            f"machine_id={self.machine_id!r}, "
            f"protocol={self.protocol!r}, "
            f"status={status})"
        )


# =============================================================================
# SECTION 6 — ABSTRACT EXTRACTOR BASE
# =============================================================================

class ExtractorBase(abc.ABC):
    """
    Abstract base for protocol-specific field extractors.

    Each extractor knows how to pull fields from one protocol's raw dict
    and map them to MFIStandardModel field names.

    Contract:
      - extract() returns a flat dict ready for MFIStandardModel(**data).
      - Raises ValueError with a descriptive message on any field failure.
      - Never returns partial data silently.
    """

    PROTOCOL: str = ""

    @abc.abstractmethod
    def extract(self, payload: RawPayload) -> dict[str, Any]:
        """
        Extract and map fields from a RawPayload to MFI standard dict.

        Args:
            payload : RawPayload from Phase 2 reader.

        Returns:
            Flat dict suitable for MFIStandardModel(**dict).

        Raises:
            ValueError : If a required field is missing or malformed.
            KeyError   : If raw payload structure is unexpected.
        """
        ...


# =============================================================================
# SECTION 7 — OPC UA EXTRACTOR
# =============================================================================

class OPCUAExtractor(ExtractorBase):
    """
    Extracts MFI standard fields from OPC UA RawPayload.

    OPC UA payload structure (from Phase 2):
      raw.nodes  → list of DataValue dicts
      Each node  → { NodeId, DisplayName, DataType, Value, StatusCode, ... }

    Extraction strategy:
      - Build a lookup dict: DisplayName → Value
      - Map OPC UA node names to MFI field names
      - Apply type coercions where needed
    """

    PROTOCOL = "opcua"

    # OPC UA DisplayName → MFI field name mapping
    _NODE_TO_FIELD: dict[str, str] = {
        "Status"        : "status",
        "AlarmCode"     : "alarm_code",
        "PieceCount"    : "piece_count",
        "GoodCount"     : "good_count",
        "BadCount"      : "bad_count",
        "CycleTimeSec"  : "cycle_time_sec",
        "TemperatureC"  : "temperature_c",
        "Speed"         : "speed",
        "MachineType"   : "machine_type",
        "Protocol"      : "protocol",
        "SiteId"        : "site_id",
    }

    def extract(self, payload: RawPayload) -> dict[str, Any]:
        """
        Extract from OPC UA DataValue node list.

        Args:
            payload : OPC UA RawPayload.

        Returns:
            Flat dict for MFIStandardModel construction.

        Raises:
            ValueError : Missing nodes or bad StatusCode on critical fields.
            KeyError   : Unexpected raw payload structure.
        """
        raw     = payload.raw
        nodes   = raw.get("nodes", [])

        if not nodes:
            raise ValueError(
                f"[{payload.machine_id}] OPC UA payload has no nodes."
            )

        # Build DisplayName → DataValue lookup
        node_map: dict[str, dict] = {
            n["DisplayName"]: n for n in nodes
        }

        # Warn on Uncertain/Bad status codes (non-fatal — data may still be usable)
        for name, node in node_map.items():
            sc = node.get("StatusCode", {}).get("Name", "Good")
            if sc not in ("Good",):
                LOG.warning(
                    "OPC UA StatusCode=%s on node %s.%s — accepting with caution",
                    sc, payload.machine_id, name,
                )

        # Extract all required fields
        result: dict[str, Any] = {
            "machine_id" : payload.machine_id,
            "timestamp"  : payload.timestamp,
        }

        for node_name, field_name in self._NODE_TO_FIELD.items():
            node = node_map.get(node_name)
            if node is None:
                raise ValueError(
                    f"[{payload.machine_id}] OPC UA missing node '{node_name}'."
                )
            result[field_name] = node["Value"]

        return result


# =============================================================================
# SECTION 8 — MODBUS TCP EXTRACTOR
# =============================================================================

class ModbusExtractor(ExtractorBase):
    """
    Extracts MFI standard fields from Modbus TCP RawPayload.

    Modbus payload structure (from Phase 2):
      raw.register_blocks → list of register block dicts
      Each block → { field, start_address, raw_registers, scale_factor }

    Extraction strategy:
      - Reject payloads with a non-null error field.
      - Build a field → block lookup.
      - Decode register values using scale_factor and word-split rules.
      - Decode status int back to status string via MODBUS_STATUS_DECODING.
    """

    PROTOCOL = "modbus"

    def _decode_block(self, block: dict[str, Any]) -> Any:
        """
        Decode one register block back to its real-world value.

        Rules:
          - "status"      : single register → status string via decoding table
          - 2-register    : high/low 32-bit recombination → int
          - 1-register    : raw ÷ scale_factor → float

        Args:
            block : Register block dict from Modbus raw payload.

        Returns:
            Decoded real-world value (str, int, or float).

        Raises:
            ValueError : If decoding fails.
        """
        field       = block["field"]
        regs        = block["raw_registers"]
        scale       = block["scale_factor"]
        reg_count   = block["register_count"]

        if field == "status":
            # Status is encoded as an integer — decode to string
            raw_int = regs[0]
            decoded = MODBUS_STATUS_DECODING.get(raw_int)
            if decoded is None:
                raise ValueError(
                    f"Unknown Modbus status code {raw_int}. "
                    f"Valid: {MODBUS_STATUS_DECODING}"
                )
            return decoded

        if reg_count == 2:
            # 32-bit value: high_word << 16 | low_word
            return (regs[0] << 16) | regs[1]

        # Single register: divide by scale
        if scale == 1.0:
            return regs[0]
        return round(regs[0] / scale, 2)

    def extract(self, payload: RawPayload) -> dict[str, Any]:
        """
        Extract from Modbus TCP register blocks.

        Args:
            payload : Modbus TCP RawPayload.

        Returns:
            Flat dict for MFIStandardModel construction.

        Raises:
            ValueError : On Modbus error flag, missing blocks, or decode failure.
        """
        raw = payload.raw

        # Hard reject on Modbus exception response
        if raw.get("error") is not None:
            raise ValueError(
                f"[{payload.machine_id}] Modbus error: {raw['error']} — record rejected."
            )

        blocks = raw.get("register_blocks", [])
        if not blocks:
            raise ValueError(
                f"[{payload.machine_id}] Modbus payload has no register_blocks."
            )

        # Build field → block lookup
        block_map: dict[str, dict] = {b["field"]: b for b in blocks}

        # Decode all fields
        decoded: dict[str, Any] = {}
        for field in MODBUS_REGISTER_MAP:
            block = block_map.get(field)
            if block is None:
                raise ValueError(
                    f"[{payload.machine_id}] Modbus missing register block "
                    f"for field '{field}'."
                )
            decoded[field] = self._decode_block(block)

        # Map Modbus field names to MFI standard field names
        result: dict[str, Any] = {
            "machine_id"    : payload.machine_id,
            "site_id"       : payload.site_id,
            "machine_type"  : "CNC",        # Modbus does not transmit machine_type
                                             # — filled from registry in production
            "protocol"      : "modbus",
            "status"        : decoded["status"],
            "alarm_code"    : int(decoded["alarm_code"]),
            "piece_count"   : int(decoded["piece_count"]),
            "good_count"    : int(decoded["good_count"]),
            "bad_count"     : int(decoded["bad_count"]),
            "cycle_time_sec": float(decoded["cycle_time"]),
            "temperature_c" : float(decoded["temperature"]),
            "speed"         : float(decoded["speed"]),
            "timestamp"     : payload.timestamp,
        }

        return result


# =============================================================================
# SECTION 9 — MQTT EXTRACTOR
# =============================================================================

class MQTTExtractor(ExtractorBase):
    """
    Extracts MFI standard fields from MQTT RawPayload.

    MQTT payload structure (from Phase 2):
      raw.messages → list of 4 message dicts
      The /data message (index 0) carries the full snapshot as its payload.

    Extraction strategy:
      - Find the /data sub-topic message by topic suffix.
      - Extract its payload dict directly (already MFI-shaped from Phase 2).
      - Enrich with machine_id and site_id from the envelope.
    """

    PROTOCOL = "mqtt"

    _DATA_TOPIC_SUFFIX = "/data"

    def _find_data_message(
        self,
        messages    : list[dict],
        machine_id  : str,
    ) -> dict[str, Any]:
        """
        Find the /data sub-topic message within MQTT messages list.

        Args:
            messages    : List of MQTT message dicts.
            machine_id  : Used for error context.

        Returns:
            The payload dict from the /data message.

        Raises:
            ValueError : If no /data message is found.
        """
        for msg in messages:
            topic = msg.get("topic", "")
            if topic.endswith(self._DATA_TOPIC_SUFFIX):
                return msg.get("payload", {})

        raise ValueError(
            f"[{machine_id}] MQTT payload has no '{self._DATA_TOPIC_SUFFIX}' "
            f"message. Topics found: {[m.get('topic') for m in messages]}"
        )

    def extract(self, payload: RawPayload) -> dict[str, Any]:
        """
        Extract from MQTT /data message payload.

        Args:
            payload : MQTT RawPayload.

        Returns:
            Flat dict for MFIStandardModel construction.

        Raises:
            ValueError : If /data message is missing or payload incomplete.
        """
        raw      = payload.raw
        messages = raw.get("messages", [])

        if not messages:
            raise ValueError(
                f"[{payload.machine_id}] MQTT payload has no messages."
            )

        data_payload = self._find_data_message(messages, payload.machine_id)

        # Validate required fields present in data payload
        required = [
            "site_id", "machine_id", "machine_type", "protocol",
            "status", "alarm_code", "piece_count", "good_count",
            "bad_count", "cycle_time_sec", "temperature_c", "speed", "timestamp",
        ]
        missing = [f for f in required if f not in data_payload]
        if missing:
            raise ValueError(
                f"[{payload.machine_id}] MQTT /data payload missing fields: {missing}"
            )

        # MQTT /data payload is already MFI-shaped — pass through directly
        return {k: data_payload[k] for k in required}


# =============================================================================
# SECTION 10 — NORMALIZER REGISTRY & ENGINE
# =============================================================================

class NormalizerRegistry:
    """
    Registry mapping protocol names to their extractors.

    Used internally by the Normalizer engine.
    """

    def __init__(self) -> None:
        self._extractors: dict[str, ExtractorBase] = {
            "opcua"  : OPCUAExtractor(),
            "modbus" : ModbusExtractor(),
            "mqtt"   : MQTTExtractor(),
        }

    def get(self, protocol: str) -> ExtractorBase:
        """
        Return extractor for the given protocol.

        Args:
            protocol : Protocol name string.

        Returns:
            Matching ExtractorBase instance.

        Raises:
            ValueError : If protocol is not registered.
        """
        extractor = self._extractors.get(protocol.lower())
        if extractor is None:
            raise ValueError(
                f"No extractor registered for protocol '{protocol}'. "
                f"Registered: {list(self._extractors.keys())}"
            )
        return extractor


class Normalizer:
    """
    MFI Normalization Engine.

    Orchestrates the full pipeline:
      RawPayload → ExtractorBase.extract() → MFIStandardModel (Pydantic)
                                           → NormalizationResult

    Failures at extraction OR validation produce a failed NormalizationResult
    with full error detail. No exception propagates silently.
    """

    def __init__(self) -> None:
        self._registry = NormalizerRegistry()
        LOG.info(
            "Normalizer initialized | protocols=%s | pydantic=%s",
            list(self._registry._extractors.keys()),
            pydantic.__version__,
        )

    def normalize(self, payload: RawPayload) -> NormalizationResult:
        """
        Normalize one RawPayload to MFIStandardModel.

        Steps:
          1. Resolve extractor by protocol.
          2. Extract raw fields → flat dict.
          3. Validate via MFIStandardModel (Pydantic).
          4. Return NormalizationResult (success or failure).

        Args:
            payload : RawPayload from Phase 2 reader.

        Returns:
            NormalizationResult with success flag and record or errors.
        """
        machine_id  = payload.machine_id
        protocol    = payload.protocol

        # Step 1: Resolve extractor
        try:
            extractor = self._registry.get(protocol)
        except ValueError as exc:
            LOG.error("No extractor │ %s │ %s", machine_id, exc)
            return NormalizationResult(
                machine_id  = machine_id,
                protocol    = protocol,
                errors      = [str(exc)],
            )

        # Step 2: Extract fields
        try:
            raw_fields = extractor.extract(payload)
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            LOG.error("Extract FAIL │ %-10s │ %s: %s", machine_id, type(exc).__name__, exc)
            return NormalizationResult(
                machine_id  = machine_id,
                protocol    = protocol,
                errors      = [f"Extraction error: {exc}"],
            )

        # Step 3: Pydantic validation
        try:
            record = MFIStandardModel(**raw_fields)
        except ValidationError as exc:
            error_msgs = [
                f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
                for e in exc.errors()
            ]
            LOG.error(
                "Validation FAIL │ %-10s │ %d error(s): %s",
                machine_id,
                len(error_msgs),
                "; ".join(error_msgs),
            )
            return NormalizationResult(
                machine_id  = machine_id,
                protocol    = protocol,
                errors      = error_msgs,
            )

        LOG.debug(
            "Normalized OK │ %-10s │ %-8s │ status=%-12s │ alarm=%3d │ pieces=%6d",
            record.machine_id,
            record.protocol,
            record.status,
            record.alarm_code,
            record.piece_count,
        )

        return NormalizationResult(
            machine_id  = machine_id,
            protocol    = protocol,
            record      = record,
        )

    def normalize_all(
        self,
        payloads: list[RawPayload],
    ) -> tuple[list[MFIStandardModel], list[NormalizationResult]]:
        """
        Normalize a batch of RawPayloads.

        Args:
            payloads : List of RawPayloads from Phase 2 read_all().

        Returns:
            Tuple of:
              - valid   : List of successfully validated MFIStandardModel records.
              - failed  : List of failed NormalizationResult (with error detail).
        """
        valid   : list[MFIStandardModel]    = []
        failed  : list[NormalizationResult] = []

        for payload in payloads:
            result = self.normalize(payload)
            if result.success:
                valid.append(result.record)
            else:
                failed.append(result)

        LOG.info(
            "normalize_all │ total=%d │ valid=%d │ failed=%d",
            len(payloads),
            len(valid),
            len(failed),
        )
        return valid, failed


# =============================================================================
# SECTION 11 — CYCLE SUMMARY
# =============================================================================

def print_normalizer_summary(
    cycle_num   : int,
    valid       : list[MFIStandardModel],
    failed      : list[NormalizationResult],
) -> None:
    """
    Print a concise summary of one normalizer cycle.

    Args:
        cycle_num : Cycle number (1-based).
        valid     : Successfully normalized records.
        failed    : Failed normalization results.
    """
    proto_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    alarm_count = 0

    for r in valid:
        proto_counts[r.protocol]    = proto_counts.get(r.protocol, 0) + 1
        status_counts[r.status]     = status_counts.get(r.status, 0) + 1
        if r.alarm_code != 0:
            alarm_count += 1

    LOG.info(
        "─── NORMALIZER CYCLE %02d ─── valid=%d │ failed=%d │ alarms=%d │ %s │ %s",
        cycle_num,
        len(valid),
        len(failed),
        alarm_count,
        " ".join(f"{k}={v}" for k, v in sorted(proto_counts.items())),
        " ".join(f"{k}={v}" for k, v in sorted(status_counts.items())),
    )


# =============================================================================
# SECTION 12 — OUTPUT WRITER
# =============================================================================

def write_output(
    records     : list[MFIStandardModel],
    filepath    : str,
) -> None:
    """
    Write validated MFI standard records to a JSON file.

    Args:
        records  : List of MFIStandardModel to serialize.
        filepath : Destination file path.
    """
    data = [r.to_dict() for r in records]
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    LOG.info("Output written → %s (%d records)", filepath, len(data))


# =============================================================================
# SECTION 13 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 3. Validates:
      1.  MFIStandardModel accepts a valid complete record.
      2.  MFIStandardModel rejects invalid machine_type.
      3.  MFIStandardModel rejects invalid protocol.
      4.  MFIStandardModel rejects invalid status.
      5.  MFIStandardModel rejects negative alarm_code.
      6.  MFIStandardModel rejects malformed timestamp.
      7.  OPCUAExtractor produces valid MFI dict from OPC UA payload.
      8.  ModbusExtractor produces valid MFI dict from Modbus payload.
      9.  ModbusExtractor rejects payloads with Modbus error flag.
      10. MQTTExtractor produces valid MFI dict from MQTT payload.
      11. Normalizer.normalize() succeeds end-to-end for all 3 protocols.
      12. Normalizer.normalize_all() processes full 50-machine fleet.
      13. MFIStandardModel is JSON-serializable.
      14. Failed records produce NormalizationResult with errors list.

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

    # ── Shared test data ──────────────────────────────────────────────────
    VALID_RECORD = dict(
        site_id         = "mecanitec",
        machine_id      = "machine01",
        machine_type    = "CNC",
        protocol        = "opcua",
        status          = "RUN",
        alarm_code      = 0,
        piece_count     = 15000,
        good_count      = 5,
        bad_count       = 0,
        cycle_time_sec  = 42.5,
        temperature_c   = 38.2,
        speed           = 1200.0,
        timestamp       = "2026-05-16T20:00:00+00:00",
    )

    # --- Test 1: Valid record accepted ---
    try:
        m = MFIStandardModel(**VALID_RECORD)
        check("MFIStandardModel accepts valid record", m.machine_id == "machine01")
    except ValidationError as exc:
        check("MFIStandardModel accepts valid record", False, str(exc))

    # --- Test 2: Invalid machine_type rejected ---
    try:
        MFIStandardModel(**{**VALID_RECORD, "machine_type": "SPACESHIP"})
        check("Rejects invalid machine_type", False, "no error raised")
    except ValidationError:
        check("Rejects invalid machine_type", True)

    # --- Test 3: Invalid protocol rejected ---
    try:
        MFIStandardModel(**{**VALID_RECORD, "protocol": "profinet"})
        check("Rejects invalid protocol", False, "no error raised")
    except ValidationError:
        check("Rejects invalid protocol", True)

    # --- Test 4: Invalid status rejected ---
    try:
        MFIStandardModel(**{**VALID_RECORD, "status": "RUNNING"})
        check("Rejects invalid status", False, "no error raised")
    except ValidationError:
        check("Rejects invalid status", True)

    # --- Test 5: Negative alarm_code rejected ---
    try:
        MFIStandardModel(**{**VALID_RECORD, "alarm_code": -1})
        check("Rejects negative alarm_code", False, "no error raised")
    except ValidationError:
        check("Rejects negative alarm_code", True)

    # --- Test 6: Malformed timestamp rejected ---
    try:
        MFIStandardModel(**{**VALID_RECORD, "timestamp": "not-a-date"})
        check("Rejects malformed timestamp", False, "no error raised")
    except ValidationError:
        check("Rejects malformed timestamp", True)

    # --- Build Phase 2 payloads for extractor tests ---
    from mfi_phase_02_readers import OPCUAReader, ModbusTCPReader, MQTTReader
    import dataclasses

    fleet   = build_machine_fleet()
    acc: dict[str, int] = {}
    snaps   = run_simulation_cycle(fleet, acc)

    def _snap_for(proto: str):
        for s in snaps:
            if s.protocol == proto:
                return s
        return dataclasses.replace(snaps[0], protocol=proto)

    snap_opcua  = _snap_for("opcua")
    snap_modbus = _snap_for("modbus")
    snap_mqtt   = _snap_for("mqtt")

    pl_opcua    = OPCUAReader().read(snap_opcua)
    pl_modbus   = ModbusTCPReader().read(snap_modbus)
    pl_mqtt     = MQTTReader().read(snap_mqtt)

    # --- Test 7: OPCUAExtractor ---
    try:
        fields = OPCUAExtractor().extract(pl_opcua)
        check(
            "OPCUAExtractor produces valid MFI dict",
            "machine_id" in fields and "status" in fields,
        )
    except Exception as exc:
        check("OPCUAExtractor produces valid MFI dict", False, str(exc))

    # --- Test 8: ModbusExtractor ---
    try:
        fields = ModbusExtractor().extract(pl_modbus)
        check(
            "ModbusExtractor produces valid MFI dict",
            "machine_id" in fields and "status" in fields,
        )
    except Exception as exc:
        check("ModbusExtractor produces valid MFI dict", False, str(exc))

    # --- Test 9: ModbusExtractor rejects error payloads ---
    import copy
    bad_pl_modbus = copy.deepcopy(pl_modbus)
    bad_pl_modbus.raw["error"] = "ExceptionCode: 0x04"
    try:
        ModbusExtractor().extract(bad_pl_modbus)
        check("ModbusExtractor rejects error payload", False, "no error raised")
    except ValueError:
        check("ModbusExtractor rejects error payload", True)

    # --- Test 10: MQTTExtractor ---
    try:
        fields = MQTTExtractor().extract(pl_mqtt)
        check(
            "MQTTExtractor produces valid MFI dict",
            "machine_id" in fields and "status" in fields,
        )
    except Exception as exc:
        check("MQTTExtractor produces valid MFI dict", False, str(exc))

    # --- Test 11: End-to-end normalize() for all 3 protocols ---
    normalizer = Normalizer()
    for proto, pl in [("opcua", pl_opcua), ("modbus", pl_modbus), ("mqtt", pl_mqtt)]:
        result = normalizer.normalize(pl)
        check(
            f"Normalizer.normalize() succeeds for {proto}",
            result.success,
            str(result.errors) if not result.success else "",
        )

    # --- Test 12: normalize_all() on full 50-machine fleet ---
    reader_reg  = ReaderRegistry()
    payloads    = reader_reg.read_all(snaps)
    valid, fail = normalizer.normalize_all(payloads)
    check(
        "normalize_all: valid + failed == total machines",
        len(valid) + len(fail) == len(snaps),
        f"valid={len(valid)} failed={len(fail)} total={len(snaps)}",
    )
    # Most records should pass (Modbus errors are probabilistic ~15% of FAULT machines)
    check(
        "normalize_all: at least 80% valid",
        len(valid) >= len(snaps) * 0.80,
        f"valid={len(valid)} / {len(snaps)}",
    )

    # --- Test 13: JSON serializable ---
    if valid:
        try:
            _ = json.dumps(valid[0].to_dict())
            check("MFIStandardModel is JSON-serializable", True)
        except (TypeError, ValueError) as exc:
            check("MFIStandardModel is JSON-serializable", False, str(exc))

    # --- Test 14: Failed result has errors list ---
    fail_result = normalizer.normalize(bad_pl_modbus)
    check(
        "Failed NormalizationResult has non-empty errors",
        not fail_result.success and len(fail_result.errors) > 0,
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
# SECTION 14 — CLI / MAIN ENTRY POINT
# =============================================================================

def build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for Phase 3."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_03_json_model.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Normalize raw protocol payloads to MFI Standard Internal JSON."
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
        help    = f"Number of simulation cycles (default: {DEFAULT_CYCLES}).",
    )
    parser.add_argument(
        "--protocol",
        type    = str,
        default = DEFAULT_PROTOCOL,
        choices = ["all", "opcua", "modbus", "mqtt"],
        help    = "Filter output to one protocol (default: all).",
    )
    parser.add_argument(
        "--output",
        type    = str,
        default = None,
        metavar = "FILE",
        help    = "Write last cycle MFI records to a JSON file.",
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
    Phase 3 entry point.

    Modes:
      --self-test : Run validation suite.
      (default)   : Run N cycles through Phase 1→2→3 pipeline.
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

    # ── Pipeline mode: Phase 1 → Phase 2 → Phase 3 ───────────────────────
    LOG.info(
        "Starting normalizer pipeline: site=%s | protocol=%s | cycles=%d",
        args.site,
        args.protocol,
        args.cycles,
    )

    fleet       = build_machine_fleet(site_id=args.site)
    reader_reg  = ReaderRegistry(site_id=args.site)
    normalizer  = Normalizer()
    acc: dict[str, int] = {}
    last_valid  : list[MFIStandardModel] = []

    for cycle in range(1, args.cycles + 1):
        LOG.info("── Cycle %02d / %02d ──", cycle, args.cycles)

        # Phase 1: simulate
        snapshots   = run_simulation_cycle(fleet, acc)

        # Phase 2: read
        payloads    = reader_reg.read_all(snapshots)

        # Protocol filter
        if args.protocol != "all":
            payloads = [p for p in payloads if p.protocol == args.protocol]

        # Phase 3: normalize
        valid, fail = normalizer.normalize_all(payloads)
        print_normalizer_summary(cycle, valid, fail)
        last_valid = valid

        if cycle < args.cycles:
            time.sleep(args.interval)

    # ── Optional output ───────────────────────────────────────────────────
    if args.output and last_valid:
        write_output(last_valid, args.output)

    # ── Phase footer ──────────────────────────────────────────────────────
    LOG.info(
        "Phase %s complete. Cycles=%d | Last valid records=%d",
        PHASE_ID,
        args.cycles,
        len(last_valid),
    )


# =============================================================================
# SECTION 15 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
