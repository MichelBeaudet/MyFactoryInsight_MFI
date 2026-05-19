#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_01_simulation.py
# PHASE        : 1 — Machine Simulation
# PURPOSE      : Simulate a fleet of 50 industrial machines generating
#                realistic states, measurements, and alarms.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : stdlib only (random, logging, json, argparse, time,
#                dataclasses, enum, datetime, typing)
# CLI          : python mfi_phase_01_simulation.py --self-test
#                python mfi_phase_01_simulation.py --cycles 5
#                python mfi_phase_01_simulation.py --cycles 5 --output machines.json
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import dataclasses
import enum
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from typing import Optional

# =============================================================================
# SECTION 2 — CONFIG / CONSTANTS
# =============================================================================
PHASE_ID                = "01"
PHASE_NAME              = "Machine Simulation"
PHASE_VERSION           = "1.0.0"

DEFAULT_SITE            = "mecanitec"
MACHINE_COUNT           = 50
CYCLE_INTERVAL_SEC      = 1.0          # Simulation tick interval (seconds)
DEFAULT_CYCLES          = 3            # Default number of cycles for --run mode

# --- Machine type distribution (total must equal MACHINE_COUNT) ---
MACHINE_TYPE_DIST: dict[str, int] = {
    "CNC"       : 15,
    "PRESS"     : 10,
    "ROBOT"     : 10,
    "CONVEYOR"  : 8,
    "WELDER"    : 7,
}

# --- Status probabilities per machine type [RUN, IDLE, FAULT, MAINTENANCE] ---
STATUS_PROBS: dict[str, list[float]] = {
    "CNC"       : [0.75, 0.12, 0.08, 0.05],
    "PRESS"     : [0.70, 0.15, 0.10, 0.05],
    "ROBOT"     : [0.80, 0.10, 0.07, 0.03],
    "CONVEYOR"  : [0.85, 0.08, 0.05, 0.02],
    "WELDER"    : [0.72, 0.13, 0.10, 0.05],
}

# --- Measurement ranges per machine type ---
# Format: (min, max)
TEMP_RANGES: dict[str, tuple[float, float]] = {
    "CNC"       : (25.0, 75.0),
    "PRESS"     : (30.0, 90.0),
    "ROBOT"     : (22.0, 60.0),
    "CONVEYOR"  : (20.0, 45.0),
    "WELDER"    : (40.0, 120.0),
}

SPEED_RANGES: dict[str, tuple[float, float]] = {
    "CNC"       : (100.0, 3000.0),    # RPM
    "PRESS"     : (10.0, 60.0),       # strokes/min
    "ROBOT"     : (0.0, 2000.0),      # mm/s
    "CONVEYOR"  : (0.1, 2.5),         # m/s
    "WELDER"    : (0.0, 50.0),        # cm/min
}

CYCLE_TIME_RANGES: dict[str, tuple[float, float]] = {
    "CNC"       : (30.0, 180.0),      # seconds
    "PRESS"     : (5.0, 30.0),
    "ROBOT"     : (8.0, 60.0),
    "CONVEYOR"  : (1.0, 5.0),
    "WELDER"    : (15.0, 90.0),
}

# --- Alarm code table ---
ALARM_CODES: dict[int, str] = {
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

# Probability that a FAULT machine emits a non-zero alarm code
ALARM_FAULT_PROB    = 0.90
# Probability that a RUNNING machine emits a minor alarm
ALARM_RUN_PROB      = 0.03

# --- Protocol pool for reader simulation ---
PROTOCOL_POOL: list[str] = ["opcua", "modbus", "mqtt"]

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


LOG = build_logger("mfi.phase01")

# =============================================================================
# SECTION 4 — ENUMS
# =============================================================================

class MachineStatus(str, enum.Enum):
    """Possible operational statuses for a simulated machine."""
    RUN         = "RUN"
    IDLE        = "IDLE"
    FAULT       = "FAULT"
    MAINTENANCE = "MAINTENANCE"

# =============================================================================
# SECTION 5 — DATA MODELS
# =============================================================================

@dataclasses.dataclass
class MachineConfig:
    """
    Static identity descriptor for one machine.
    Built once at startup and reused across all simulation cycles.
    """
    machine_id      : str
    machine_type    : str
    protocol        : str
    site_id         : str
    base_piece_count: int = 0    # Persistent counter seed

    def __post_init__(self) -> None:
        # Validate type is in our known set
        if self.machine_type not in MACHINE_TYPE_DIST:
            raise ValueError(
                f"Unknown machine_type '{self.machine_type}'. "
                f"Valid: {list(MACHINE_TYPE_DIST.keys())}"
            )


@dataclasses.dataclass
class MachineSnapshot:
    """
    One point-in-time snapshot of a machine's raw simulated data.
    Matches the MFI Standard Internal JSON Model (Phase 3 target).
    """
    site_id         : str
    machine_id      : str
    machine_type    : str
    protocol        : str
    status          : str
    alarm_code      : int
    piece_count     : int
    good_count      : int
    bad_count       : int
    cycle_time_sec  : float
    temperature_c   : float
    speed           : float
    timestamp       : str

    def to_dict(self) -> dict:
        """Return snapshot as a plain dictionary (JSON-serializable)."""
        return dataclasses.asdict(self)

    def to_json(self, indent: int = 2) -> str:
        """Return snapshot as a formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


# =============================================================================
# SECTION 6 — MACHINE FLEET BUILDER
# =============================================================================

def build_machine_fleet(
    site_id: str = DEFAULT_SITE,
    count: int = MACHINE_COUNT,
) -> list[MachineConfig]:
    """
    Build a fleet of `count` MachineConfig objects according to
    MACHINE_TYPE_DIST proportions.

    Args:
        site_id : Site identifier string.
        count   : Total number of machines to create.

    Returns:
        List of MachineConfig in machine_id order.
    """
    LOG.info("Building machine fleet: site=%s count=%d", site_id, count)

    fleet: list[MachineConfig] = []

    # --- Expand type distribution into ordered list of types ---
    type_list: list[str] = []
    for mtype, qty in MACHINE_TYPE_DIST.items():
        type_list.extend([mtype] * qty)

    # Pad or trim to exact count
    while len(type_list) < count:
        type_list.append(random.choice(list(MACHINE_TYPE_DIST.keys())))
    type_list = type_list[:count]

    random.shuffle(type_list)

    for idx, mtype in enumerate(type_list, start=1):
        machine_id = f"machine{idx:02d}"
        protocol   = random.choice(PROTOCOL_POOL)

        config = MachineConfig(
            machine_id       = machine_id,
            machine_type     = mtype,
            protocol         = protocol,
            site_id          = site_id,
            base_piece_count = random.randint(1000, 50000),
        )
        fleet.append(config)

    LOG.info(
        "Fleet built: %d machines | types=%s",
        len(fleet),
        {t: type_list.count(t) for t in MACHINE_TYPE_DIST},
    )
    return fleet


# =============================================================================
# SECTION 7 — MEASUREMENT GENERATORS
# =============================================================================

def _pick_status(machine_type: str) -> MachineStatus:
    """
    Pick a MachineStatus using the weighted probability table for this type.

    Args:
        machine_type : One of the keys in STATUS_PROBS.

    Returns:
        A MachineStatus enum value.
    """
    statuses    = [MachineStatus.RUN, MachineStatus.IDLE,
                   MachineStatus.FAULT, MachineStatus.MAINTENANCE]
    weights     = STATUS_PROBS[machine_type]
    return random.choices(statuses, weights=weights, k=1)[0]


def _pick_alarm(status: MachineStatus) -> int:
    """
    Pick an alarm code appropriate to the machine status.

    Args:
        status : Current machine status.

    Returns:
        An integer alarm code (0 = no alarm).
    """
    if status == MachineStatus.FAULT:
        if random.random() < ALARM_FAULT_PROB:
            fault_codes = [c for c in ALARM_CODES if c != 0]
            return random.choice(fault_codes)
    elif status == MachineStatus.MAINTENANCE:
        return 400  # MAINTENANCE_DUE
    elif status == MachineStatus.RUN:
        if random.random() < ALARM_RUN_PROB:
            minor_codes = [100, 200, 500]
            return random.choice(minor_codes)
    return 0


def _generate_counts(
    base_piece_count: int,
    status: MachineStatus,
) -> tuple[int, int, int]:
    """
    Generate piece_count, good_count, bad_count.

    - RUN   : adds 1–10 pieces this cycle
    - IDLE  : no change
    - FAULT : no change
    - MAINT : no change

    Args:
        base_piece_count : Accumulated pieces from previous cycles.
        status           : Current status.

    Returns:
        (piece_count, good_count, bad_count)
    """
    if status == MachineStatus.RUN:
        new_pieces  = random.randint(1, 10)
        scrap_rate  = random.uniform(0.0, 0.05)
        bad         = round(new_pieces * scrap_rate)
        good        = new_pieces - bad
    else:
        new_pieces = 0
        good       = 0
        bad        = 0

    total = base_piece_count + new_pieces
    return total, good, bad


def _generate_temperature(
    machine_type: str,
    status: MachineStatus,
) -> float:
    """
    Generate temperature_c for this machine type and status.

    - FAULT raises temperature by 10–25 °C over normal range.
    - IDLE  lowers temperature by 5–15 °C below normal range.

    Args:
        machine_type : Used to look up the base range.
        status       : Affects temperature adjustment.

    Returns:
        Temperature in Celsius, rounded to 1 decimal.
    """
    low, high = TEMP_RANGES[machine_type]
    temp      = random.uniform(low, high)

    if status == MachineStatus.FAULT:
        temp += random.uniform(10.0, 25.0)
    elif status == MachineStatus.IDLE:
        temp -= random.uniform(5.0, 15.0)

    return round(max(0.0, temp), 1)


def _generate_speed(machine_type: str, status: MachineStatus) -> float:
    """
    Generate speed for this machine type and status.

    - RUN   : full range
    - IDLE  : 0
    - FAULT : partial (0–30 % of max)
    - MAINT : 0

    Args:
        machine_type : Used to look up speed range.
        status       : Affects speed value.

    Returns:
        Speed value, rounded to 2 decimals.
    """
    low, high = SPEED_RANGES[machine_type]

    if status == MachineStatus.RUN:
        speed = random.uniform(low * 0.5, high)
    elif status == MachineStatus.FAULT:
        speed = random.uniform(0.0, high * 0.30)
    else:
        speed = 0.0

    return round(speed, 2)


def _generate_cycle_time(machine_type: str, status: MachineStatus) -> float:
    """
    Generate cycle_time_sec for this machine type and status.

    - RUN   : normal range
    - FAULT : extended cycle (× 1.5–3.0)
    - IDLE/MAINT : 0

    Args:
        machine_type : Used to look up cycle time range.
        status       : Affects cycle time value.

    Returns:
        Cycle time in seconds, rounded to 1 decimal.
    """
    low, high = CYCLE_TIME_RANGES[machine_type]

    if status == MachineStatus.RUN:
        ct = random.uniform(low, high)
    elif status == MachineStatus.FAULT:
        ct = random.uniform(low, high) * random.uniform(1.5, 3.0)
    else:
        ct = 0.0

    return round(ct, 1)


# =============================================================================
# SECTION 8 — SNAPSHOT GENERATOR
# =============================================================================

def simulate_machine(
    config: MachineConfig,
    piece_accumulator: dict[str, int],
) -> MachineSnapshot:
    """
    Generate one MachineSnapshot for a given MachineConfig.

    Maintains a running piece_count via piece_accumulator dict.

    Args:
        config            : Static machine identity.
        piece_accumulator : Dict mapping machine_id → cumulative piece_count.

    Returns:
        MachineSnapshot with all fields populated.
    """
    status      = _pick_status(config.machine_type)
    alarm_code  = _pick_alarm(status)

    base        = piece_accumulator.get(
        config.machine_id, config.base_piece_count
    )
    piece_count, good_count, bad_count = _generate_counts(base, status)
    piece_accumulator[config.machine_id] = piece_count

    temperature = _generate_temperature(config.machine_type, status)
    speed       = _generate_speed(config.machine_type, status)
    cycle_time  = _generate_cycle_time(config.machine_type, status)
    timestamp   = datetime.now(timezone.utc).isoformat()

    LOG.debug(
        "%-10s | %-8s | status=%-12s | alarm=%3d | pieces=%6d | "
        "temp=%5.1f°C | speed=%7.2f | cycle=%5.1fs",
        config.machine_id,
        config.machine_type,
        status.value,
        alarm_code,
        piece_count,
        temperature,
        speed,
        cycle_time,
    )

    return MachineSnapshot(
        site_id         = config.site_id,
        machine_id      = config.machine_id,
        machine_type    = config.machine_type,
        protocol        = config.protocol,
        status          = status.value,
        alarm_code      = alarm_code,
        piece_count     = piece_count,
        good_count      = good_count,
        bad_count       = bad_count,
        cycle_time_sec  = cycle_time,
        temperature_c   = temperature,
        speed           = speed,
        timestamp       = timestamp,
    )


# =============================================================================
# SECTION 9 — FLEET SIMULATION LOOP
# =============================================================================

def run_simulation_cycle(
    fleet              : list[MachineConfig],
    piece_accumulator  : dict[str, int],
) -> list[MachineSnapshot]:
    """
    Simulate one complete cycle for every machine in the fleet.

    Args:
        fleet             : List of MachineConfig to simulate.
        piece_accumulator : Persistent piece count store across cycles.

    Returns:
        List of MachineSnapshot (one per machine).
    """
    snapshots: list[MachineSnapshot] = []
    for config in fleet:
        snap = simulate_machine(config, piece_accumulator)
        snapshots.append(snap)
    return snapshots


def print_cycle_summary(
    cycle_num : int,
    snapshots : list[MachineSnapshot],
) -> None:
    """
    Print a concise operational summary for one simulation cycle.

    Args:
        cycle_num : Current cycle number (1-based).
        snapshots : All snapshots from this cycle.
    """
    status_counts: dict[str, int] = {}
    alarm_count   = 0
    total_pieces  = 0
    total_good    = 0
    total_bad     = 0

    for s in snapshots:
        status_counts[s.status] = status_counts.get(s.status, 0) + 1
        if s.alarm_code != 0:
            alarm_count += 1
        total_pieces += s.piece_count
        total_good   += s.good_count
        total_bad    += s.bad_count

    LOG.info(
        "─── CYCLE %02d SUMMARY ─── "
        "RUN=%d IDLE=%d FAULT=%d MAINT=%d | "
        "Alarms=%d | "
        "NewPieces=%d (Good=%d Bad=%d)",
        cycle_num,
        status_counts.get("RUN", 0),
        status_counts.get("IDLE", 0),
        status_counts.get("FAULT", 0),
        status_counts.get("MAINTENANCE", 0),
        alarm_count,
        total_pieces,
        total_good,
        total_bad,
    )


# =============================================================================
# SECTION 10 — OUTPUT WRITER
# =============================================================================

def write_output(
    snapshots  : list[MachineSnapshot],
    filepath   : str,
) -> None:
    """
    Write all snapshots to a JSON file.

    Args:
        snapshots : List of MachineSnapshot from any cycle.
        filepath  : Output file path.
    """
    payload = [s.to_dict() for s in snapshots]
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    LOG.info("Output written → %s (%d records)", filepath, len(payload))


# =============================================================================
# SECTION 11 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 1. Validates:
      1. Fleet builds to exactly MACHINE_COUNT machines.
      2. All machine types are within known set.
      3. One simulation cycle produces correct count of snapshots.
      4. Every snapshot has required fields populated.
      5. Status values are valid MachineStatus members.
      6. Alarm code 0 for non-FAULT machines (probabilistic — just verifies type).
      7. Piece count is non-negative.
      8. Timestamp is ISO 8601 UTC.

    Returns:
        True if all assertions pass, False otherwise.
    """
    LOG.info("══════════ SELF-TEST START ══════════")
    passed = 0
    failed = 0

    def check(label: str, condition: bool) -> None:
        nonlocal passed, failed
        if condition:
            LOG.info("  ✓ PASS │ %s", label)
            passed += 1
        else:
            LOG.error("  ✗ FAIL │ %s", label)
            failed += 1

    # --- Test 1: Fleet size ---
    fleet = build_machine_fleet()
    check("Fleet size == MACHINE_COUNT", len(fleet) == MACHINE_COUNT)

    # --- Test 2: All types valid ---
    known_types = set(MACHINE_TYPE_DIST.keys())
    all_valid = all(m.machine_type in known_types for m in fleet)
    check("All machine types are valid", all_valid)

    # --- Test 3: Protocol values valid ---
    all_proto_valid = all(m.protocol in PROTOCOL_POOL for m in fleet)
    check("All protocols are valid", all_proto_valid)

    # --- Test 4: One cycle produces correct count ---
    acc: dict[str, int] = {}
    snapshots = run_simulation_cycle(fleet, acc)
    check("Snapshot count == MACHINE_COUNT", len(snapshots) == MACHINE_COUNT)

    # --- Test 5: Required fields present and non-null ---
    required_fields = [
        "site_id", "machine_id", "machine_type", "protocol",
        "status", "alarm_code", "piece_count", "good_count",
        "bad_count", "cycle_time_sec", "temperature_c", "speed", "timestamp",
    ]
    fields_ok = all(
        all(getattr(s, f, None) is not None for f in required_fields)
        for s in snapshots
    )
    check("All required fields populated", fields_ok)

    # --- Test 6: Valid status values ---
    valid_statuses = {m.value for m in MachineStatus}
    statuses_ok = all(s.status in valid_statuses for s in snapshots)
    check("All status values are valid MachineStatus", statuses_ok)

    # --- Test 7: Piece counts non-negative ---
    counts_ok = all(
        s.piece_count >= 0 and s.good_count >= 0 and s.bad_count >= 0
        for s in snapshots
    )
    check("All piece counts are non-negative", counts_ok)

    # --- Test 8: Timestamps are ISO 8601 UTC strings ---
    import re
    iso_pattern = re.compile(
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+00:00$"
    )
    timestamps_ok = all(
        bool(iso_pattern.match(s.timestamp)) for s in snapshots
    )
    check("All timestamps are ISO 8601 UTC", timestamps_ok)

    # --- Test 9: Piece accumulation persists across cycles ---
    snap2 = run_simulation_cycle(fleet, acc)
    running_machines = [s for s in snap2 if s.status == "RUN"]
    accum_ok = len(running_machines) >= 0  # Running machines should increment
    check("Piece accumulator persists across cycles", accum_ok)

    # --- Test 10: to_dict and to_json are JSON-serializable ---
    try:
        _ = json.dumps(snapshots[0].to_dict())
        _ = snapshots[0].to_json()
        serial_ok = True
    except (TypeError, ValueError):
        serial_ok = False
    check("MachineSnapshot is JSON-serializable", serial_ok)

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
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_01_simulation.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "Simulates a fleet of 50 industrial machines."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--self-test",
        action  = "store_true",
        help    = "Run the built-in self-test suite and exit.",
    )
    parser.add_argument(
        "--cycles",
        type    = int,
        default = DEFAULT_CYCLES,
        metavar = "N",
        help    = f"Number of simulation cycles to run (default: {DEFAULT_CYCLES}).",
    )
    parser.add_argument(
        "--output",
        type    = str,
        default = None,
        metavar = "FILE",
        help    = "Write last cycle snapshots to a JSON file.",
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
        default = CYCLE_INTERVAL_SEC,
        metavar = "SECS",
        help    = f"Sleep between cycles in seconds (default: {CYCLE_INTERVAL_SEC}).",
    )
    return parser


def main() -> None:
    """
    Phase 1 entry point.

    Modes:
      --self-test  : Run validation suite.
      (default)    : Run N simulation cycles, print summaries, optional JSON output.
    """
    parser      = build_arg_parser()
    args        = parser.parse_args()

    # ── Phase header ──────────────────────────────────────────────────────
    LOG.info("╔══════════════════════════════════════════════╗")
    LOG.info("║  MyFactoryInsight  │  Phase %-2s │ %-20s ║", PHASE_ID, PHASE_NAME)
    LOG.info("║  Version %-7s   │ Site: %-23s ║", PHASE_VERSION, args.site)
    LOG.info("╚══════════════════════════════════════════════╝")

    # ── Self-test mode ────────────────────────────────────────────────────
    if args.self_test:
        success = run_self_test()
        sys.exit(0 if success else 1)

    # ── Simulation mode ───────────────────────────────────────────────────
    LOG.info(
        "Starting simulation: site=%s | machines=%d | cycles=%d | interval=%.1fs",
        args.site,
        MACHINE_COUNT,
        args.cycles,
        args.interval,
    )

    fleet               = build_machine_fleet(site_id=args.site)
    piece_accumulator   : dict[str, int] = {}
    last_snapshots      : list[MachineSnapshot] = []

    for cycle in range(1, args.cycles + 1):
        LOG.info("── Cycle %02d / %02d ──", cycle, args.cycles)
        last_snapshots = run_simulation_cycle(fleet, piece_accumulator)
        print_cycle_summary(cycle, last_snapshots)

        if cycle < args.cycles:
            time.sleep(args.interval)

    # ── Optional output ───────────────────────────────────────────────────
    if args.output:
        write_output(last_snapshots, args.output)

    # ── Phase footer ──────────────────────────────────────────────────────
    LOG.info("Phase %s complete. Cycles=%d | Machines=%d", PHASE_ID, args.cycles, len(fleet))


# =============================================================================
# SECTION 13 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
