#!/usr/bin/env python3
# =============================================================================
# PROJECT      : MyFactoryInsight (MFI)
# FILE         : mfi_phase_10_predict.py
# PHASE        : 10 — Predictive AI
# PURPOSE      : Detect anomalies and predict maintenance risk from MFI enriched
#                records. Two ML models run per machine type:
#                  1. IsolationForest   — unsupervised anomaly detection
#                  2. LocalOutlierFactor (novelty) — density-based outlier score
#                Both are wrapped in RobustScaler pipelines. An explainer
#                converts raw scores into human-readable risk factors.
#                Models are trained on baseline "healthy" data (first N cycles)
#                and score every subsequent cycle's records.
# AUTHOR       : Michel Beaudet
# CREATED      : 2026-05-16
# PYTHON       : 3.12+ (3.14 target-compatible syntax)
# DEPENDENCIES : scikit-learn>=1.3, numpy>=1.24, pandas>=2.0,
#                pydantic>=2.0, mfi_phase_01..04
# CLI          : python mfi_phase_10_predict.py --self-test
#                python mfi_phase_10_predict.py --cycles 20 --baseline 5
#                python mfi_phase_10_predict.py --cycles 20 --output scores.json
# =============================================================================

# =============================================================================
# SECTION 1 — IMPORTS
# =============================================================================
import argparse
import json
import logging
import pickle
import io
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Optional

# --- Numerical stack ---
try:
    import numpy as np
    import pandas as pd
except ImportError as exc:
    print(
        f"[FATAL] numpy/pandas not found: {exc}\n"
        "Install: pip install numpy pandas --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- scikit-learn ---
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.neighbors import LocalOutlierFactor
    from sklearn.preprocessing import RobustScaler
    from sklearn.pipeline import Pipeline
    from sklearn.exceptions import NotFittedError
    import sklearn
except ImportError as exc:
    print(
        f"[FATAL] scikit-learn not found: {exc}\n"
        "Install: pip install scikit-learn --break-system-packages",
        file=sys.stderr,
    )
    sys.exit(1)

# --- Pydantic ---
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
except ImportError as exc:
    print(f"[FATAL] pydantic not found: {exc}", file=sys.stderr)
    sys.exit(1)

# --- Phase 4 ---
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
PHASE_ID            = "10"
PHASE_NAME          = "Predictive AI"
PHASE_VERSION       = "1.0.0"

DEFAULT_SITE        = "mecanitec"
DEFAULT_CYCLES      = 20
DEFAULT_BASELINE    = 5            # Cycles of healthy data before scoring begins

# ── Feature columns fed to models ────────────────────────────────────────────
# Chosen for diagnostic value and low collinearity.
FEATURE_COLS: list[str] = [
    "temperature_c",        # Core thermal health indicator
    "speed",                # Operational load
    "cycle_time_sec",       # Process stability
    "piece_delta",          # Output rate this cycle
    "alarm_code",           # Non-zero = degraded state
    "availability",         # Longitudinal uptime ratio
    "quality_rate_pct",     # Quality (scaled 0–100 for numeric stability)
]

# ── Model hyperparameters ─────────────────────────────────────────────────────
IF_N_ESTIMATORS     = 100          # IsolationForest trees
IF_CONTAMINATION    = 0.1          # Expected anomaly fraction
IF_RANDOM_STATE     = 42

LOF_N_NEIGHBORS     = 20           # LOF neighborhood size
LOF_CONTAMINATION   = 0.1

# ── Scoring thresholds ────────────────────────────────────────────────────────
# IsolationForest: score_samples() returns negative average path lengths.
# More negative = more anomalous. Thresholds calibrated on simulated data.
IF_ANOMALY_THRESHOLD    = -0.55    # Below this → anomaly flag
IF_CRITICAL_THRESHOLD   = -0.70    # Below this → critical anomaly

# LOF: score_samples() returns negative LOF values.
# -1.0 = inlier boundary; more negative = more anomalous.
LOF_ANOMALY_THRESHOLD   = -1.3
LOF_CRITICAL_THRESHOLD  = -2.0

# ── Risk scoring ──────────────────────────────────────────────────────────────
RISK_WEIGHT_IF      = 0.60         # IsolationForest weight in composite risk
RISK_WEIGHT_LOF     = 0.40         # LOF weight in composite risk

RISK_LOW            = 0.30         # Below → LOW
RISK_MEDIUM         = 0.55         # Below → MEDIUM
RISK_HIGH           = 0.75         # Below → HIGH; above → CRITICAL

# ── Explainability thresholds (per-feature flags) ─────────────────────────────
EXPLAIN_TEMP_WARN   = 80.0         # °C
EXPLAIN_TEMP_CRIT   = 110.0
EXPLAIN_AVAIL_LOW   = 0.60
EXPLAIN_QUALITY_LOW = 92.0         # % (quality_rate_pct)
EXPLAIN_ALARM_NONZERO = 1

# ── History buffer ────────────────────────────────────────────────────────────
MACHINE_HISTORY_LEN = 50           # Observations kept per machine for trending

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


LOG = build_logger("mfi.phase10")

# =============================================================================
# SECTION 4 — PREDICTION RESULT MODEL (Pydantic)
# =============================================================================

class PredictionResult(BaseModel):
    """
    Per-machine AI prediction result for one cycle.

    Fields
    ------
    machine_id          : Source machine identifier.
    machine_type        : Machine type string.
    site_id             : Site identifier.
    timestamp           : ISO 8601 UTC timestamp of the scored record.
    if_score            : IsolationForest score_samples() value (negative).
    lof_score           : LOF score_samples() value (negative).
    if_anomaly          : True if IF score below IF_ANOMALY_THRESHOLD.
    lof_anomaly         : True if LOF score below LOF_ANOMALY_THRESHOLD.
    if_critical         : True if IF score below IF_CRITICAL_THRESHOLD.
    risk_score          : Composite risk score in [0, 1] (higher = riskier).
    risk_level          : "LOW" | "MEDIUM" | "HIGH" | "CRITICAL".
    risk_factors        : List of human-readable contributing factors.
    model_status        : "SCORED" | "BASELINE" | "NOT_FITTED" | "ERROR".
    features_used       : Dict of feature values that were scored.
    """

    model_config = {"validate_assignment": True}

    machine_id      : str
    machine_type    : str
    site_id         : str
    timestamp       : str
    if_score        : Optional[float]   = None
    lof_score       : Optional[float]   = None
    if_anomaly      : bool              = False
    lof_anomaly     : bool              = False
    if_critical     : bool              = False
    risk_score      : float             = Field(0.0, ge=0.0, le=1.0)
    risk_level      : str               = "LOW"
    risk_factors    : list[str]         = Field(default_factory=list)
    model_status    : str               = "NOT_FITTED"
    features_used   : dict[str, float]  = Field(default_factory=dict)

    @field_validator("risk_level")
    @classmethod
    def validate_risk_level(cls, v: str) -> str:
        valid = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        if v not in valid:
            raise ValueError(f"Invalid risk_level '{v}'. Valid: {valid}")
        return v

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def to_json(self, indent: int = 2) -> str:
        return self.model_dump_json(indent=indent)

    def __repr__(self) -> str:
        return (
            f"PredictionResult("
            f"machine_id={self.machine_id!r}, "
            f"risk={self.risk_level}, "
            f"score={self.risk_score:.3f}, "
            f"status={self.model_status!r})"
        )


# =============================================================================
# SECTION 5 — FEATURE EXTRACTOR
# =============================================================================

class FeatureExtractor:
    """
    Extracts and normalizes the feature vector from a MFIEnrichedRecord.

    FEATURE_COLS defines the exact columns passed to ML models.
    Missing or None values are imputed with safe defaults to prevent
    scoring failures on incomplete records.

    Imputation strategy:
      - availability    : 1.0 (assume healthy if unknown)
      - quality_rate    : 1.0 (assume perfect if no production)
      - Other numerics  : 0.0
    """

    # Feature → default value if None
    _DEFAULTS: dict[str, float] = {
        "temperature_c"   : 25.0,
        "speed"           : 0.0,
        "cycle_time_sec"  : 0.0,
        "piece_delta"     : 0,
        "alarm_code"      : 0,
        "availability"    : 1.0,
        "quality_rate_pct": 100.0,
    }

    @staticmethod
    def extract(record: MFIEnrichedRecord) -> dict[str, float]:
        """
        Extract the feature dict from one MFIEnrichedRecord.

        Args:
            record : MFIEnrichedRecord from Phase 4.

        Returns:
            Dict mapping FEATURE_COLS → float values.
        """
        quality_pct = (
            record.quality_rate * 100.0
            if record.quality_rate is not None
            else 100.0
        )
        avail = record.availability if record.availability is not None else 1.0

        raw = {
            "temperature_c"   : float(record.temperature_c),
            "speed"           : float(record.speed),
            "cycle_time_sec"  : float(record.cycle_time_sec),
            "piece_delta"     : float(record.piece_delta),
            "alarm_code"      : float(record.alarm_code),
            "availability"    : float(avail),
            "quality_rate_pct": float(quality_pct),
        }
        return raw

    @staticmethod
    def to_array(features: dict[str, float]) -> np.ndarray:
        """
        Convert feature dict to a 2-D numpy array (1 × n_features).

        Args:
            features : Dict from extract().

        Returns:
            numpy array of shape (1, len(FEATURE_COLS)).
        """
        row = [features.get(col, 0.0) for col in FEATURE_COLS]
        return np.array(row, dtype=np.float64).reshape(1, -1)

    @classmethod
    def batch_to_array(cls, records: list[MFIEnrichedRecord]) -> np.ndarray:
        """
        Convert a list of records to a 2-D feature array (n_records × n_features).

        Args:
            records : List of MFIEnrichedRecord.

        Returns:
            numpy array of shape (n_records, len(FEATURE_COLS)).
        """
        rows = [
            [cls.extract(r).get(col, 0.0) for col in FEATURE_COLS]
            for r in records
        ]
        return np.array(rows, dtype=np.float64)


# =============================================================================
# SECTION 6 — MACHINE TYPE MODEL BUNDLE
# =============================================================================

class ModelBundle:
    """
    Per-machine-type ML model bundle.

    Holds one trained IsolationForest pipeline and one LOF novelty pipeline,
    both trained on baseline records for a specific machine type.

    Training:
      - Collects feature arrays from healthy (baseline) cycles.
      - Fits RobustScaler + model pipeline on accumulated data.

    Scoring:
      - Returns (if_score, lof_score) for a single observation.
      - Returns (None, None) if not yet fitted.
    """

    def __init__(self, machine_type: str) -> None:
        """
        Args:
            machine_type : Machine type identifier (e.g., "CNC").
        """
        self.machine_type   = machine_type
        self._fitted        = False
        self._n_trained     = 0
        self._train_buffer  : list[np.ndarray] = []   # Accumulate before fit

        # IsolationForest pipeline
        self._if_pipeline   = Pipeline([
            ("scaler", RobustScaler()),
            ("model",  IsolationForest(
                n_estimators    = IF_N_ESTIMATORS,
                contamination   = IF_CONTAMINATION,
                random_state    = IF_RANDOM_STATE,
                n_jobs          = 1,
            )),
        ])

        # LOF novelty pipeline
        self._lof_pipeline  = Pipeline([
            ("scaler", RobustScaler()),
            ("model",  LocalOutlierFactor(
                n_neighbors     = LOF_N_NEIGHBORS,
                contamination   = LOF_CONTAMINATION,
                novelty         = True,           # Required for transform/predict after fit
            )),
        ])

        LOG.debug("ModelBundle created │ type=%s", machine_type)

    def add_training_data(self, X: np.ndarray) -> None:
        """
        Accumulate training feature rows (before fit() is called).

        Args:
            X : Feature array of shape (n_samples, n_features).
        """
        self._train_buffer.append(X)

    def fit(self) -> int:
        """
        Train both pipelines on all accumulated training data.

        Returns:
            Number of training samples used.

        Raises:
            ValueError : If no training data has been accumulated.
        """
        if not self._train_buffer:
            raise ValueError(
                f"ModelBundle[{self.machine_type}]: no training data accumulated."
            )

        X_train = np.vstack(self._train_buffer)
        self._n_trained = X_train.shape[0]

        self._if_pipeline.fit(X_train)
        self._lof_pipeline.fit(X_train)

        self._fitted = True
        self._train_buffer.clear()

        LOG.info(
            "ModelBundle[%s] fitted │ samples=%d │ features=%d",
            self.machine_type, self._n_trained, X_train.shape[1],
        )
        return self._n_trained

    def score(self, X: np.ndarray) -> tuple[float, float]:
        """
        Score one observation with both models.

        Args:
            X : Feature array of shape (1, n_features).

        Returns:
            (if_score, lof_score) — both are negative floats
            (more negative = more anomalous).

        Raises:
            NotFittedError : If models have not been fitted yet.
        """
        if not self._fitted:
            raise NotFittedError(
                f"ModelBundle[{self.machine_type}] is not fitted. "
                "Call fit() after accumulating baseline data."
            )
        if_score  = float(self._if_pipeline.score_samples(X)[0])
        lof_score = float(self._lof_pipeline.score_samples(X)[0])
        return if_score, lof_score

    @property
    def is_fitted(self) -> bool:
        return self._fitted

    @property
    def n_trained(self) -> int:
        return self._n_trained

    def serialize(self) -> bytes:
        """Pickle the fitted pipelines to bytes."""
        return pickle.dumps((self._if_pipeline, self._lof_pipeline, self._n_trained))

    @classmethod
    def deserialize(cls, machine_type: str, data: bytes) -> "ModelBundle":
        """Restore a ModelBundle from pickled bytes."""
        bundle = cls(machine_type)
        bundle._if_pipeline, bundle._lof_pipeline, bundle._n_trained = pickle.loads(data)
        bundle._fitted = True
        return bundle


# =============================================================================
# SECTION 7 — RISK SCORER AND EXPLAINER
# =============================================================================

class RiskScorer:
    """
    Converts raw model scores into a composite risk score and risk level.

    Composite risk formula:
      raw_if  = clip(if_score,  IF_CRITICAL_THRESHOLD,  0) → normalized [0,1]
      raw_lof = clip(lof_score, LOF_CRITICAL_THRESHOLD, 0) → normalized [0,1]
      risk    = RISK_WEIGHT_IF * raw_if + RISK_WEIGHT_LOF * raw_lof

    Normalization maps the most anomalous (most negative) score to 1.0
    and 0 (inlier boundary) to 0.0.
    """

    @staticmethod
    def _normalize_if(score: float) -> float:
        """Map IF score to [0, 1]. 0 = healthy, 1 = most anomalous."""
        clamped = max(IF_CRITICAL_THRESHOLD, min(0.0, score))
        span    = abs(IF_CRITICAL_THRESHOLD)
        return abs(clamped) / span if span > 0 else 0.0

    @staticmethod
    def _normalize_lof(score: float) -> float:
        """Map LOF score to [0, 1]. 0 = healthy, 1 = most anomalous."""
        clamped = max(LOF_CRITICAL_THRESHOLD, min(0.0, score))
        span    = abs(LOF_CRITICAL_THRESHOLD)
        return abs(clamped) / span if span > 0 else 0.0

    @classmethod
    def compute_risk(
        cls,
        if_score    : Optional[float],
        lof_score   : Optional[float],
    ) -> tuple[float, str]:
        """
        Compute composite risk score and risk level string.

        Args:
            if_score  : IsolationForest score_samples() value.
            lof_score : LOF score_samples() value.

        Returns:
            (risk_score, risk_level) where risk_score ∈ [0, 1].
        """
        if if_score is None and lof_score is None:
            return 0.0, "LOW"

        n_if  = cls._normalize_if(if_score)   if if_score  is not None else 0.0
        n_lof = cls._normalize_lof(lof_score) if lof_score is not None else 0.0

        risk = RISK_WEIGHT_IF * n_if + RISK_WEIGHT_LOF * n_lof
        risk = round(min(1.0, max(0.0, risk)), 4)

        if risk < RISK_LOW:
            level = "LOW"
        elif risk < RISK_MEDIUM:
            level = "MEDIUM"
        elif risk < RISK_HIGH:
            level = "HIGH"
        else:
            level = "CRITICAL"

        return risk, level


class RiskExplainer:
    """
    Generates human-readable risk factor descriptions from a feature dict
    and model scores.

    Explains which features most likely contributed to elevated risk,
    providing actionable context for maintenance operators.
    """

    @staticmethod
    def explain(
        features    : dict[str, float],
        if_score    : Optional[float],
        lof_score   : Optional[float],
        record      : MFIEnrichedRecord,
    ) -> list[str]:
        """
        Produce a list of risk factor strings for this observation.

        Args:
            features  : Feature dict from FeatureExtractor.extract().
            if_score  : IsolationForest raw score.
            lof_score : LOF raw score.
            record    : Source MFIEnrichedRecord for context.

        Returns:
            List of short human-readable factor strings (may be empty).
        """
        factors: list[str] = []

        temp = features.get("temperature_c", 0.0)
        if temp >= EXPLAIN_TEMP_CRIT:
            factors.append(f"Temperature CRITICAL ({temp:.1f}°C ≥ {EXPLAIN_TEMP_CRIT}°C)")
        elif temp >= EXPLAIN_TEMP_WARN:
            factors.append(f"Temperature elevated ({temp:.1f}°C ≥ {EXPLAIN_TEMP_WARN}°C)")

        alarm = features.get("alarm_code", 0)
        if alarm >= EXPLAIN_ALARM_NONZERO:
            factors.append(f"Active alarm code {int(alarm)}")

        avail = features.get("availability", 1.0)
        if avail < EXPLAIN_AVAIL_LOW:
            factors.append(f"Low availability ({avail:.1%} < {EXPLAIN_AVAIL_LOW:.0%})")

        qr = features.get("quality_rate_pct", 100.0)
        if qr < EXPLAIN_QUALITY_LOW and record.is_running:
            factors.append(f"Quality below threshold ({qr:.1f}% < {EXPLAIN_QUALITY_LOW:.0f}%)")

        if record.is_faulted:
            factors.append("Machine in FAULT state")

        if record.status_changed and record.is_faulted:
            factors.append(f"Recent transition → FAULT from {record.previous_status or '?'}")

        if if_score is not None and if_score < IF_CRITICAL_THRESHOLD:
            factors.append(
                f"IsolationForest: deeply anomalous path length "
                f"(score={if_score:.3f} < {IF_CRITICAL_THRESHOLD})"
            )
        elif if_score is not None and if_score < IF_ANOMALY_THRESHOLD:
            factors.append(f"IsolationForest: anomaly detected (score={if_score:.3f})")

        if lof_score is not None and lof_score < LOF_CRITICAL_THRESHOLD:
            factors.append(
                f"LOF: extreme local density deviation "
                f"(score={lof_score:.3f} < {LOF_CRITICAL_THRESHOLD})"
            )
        elif lof_score is not None and lof_score < LOF_ANOMALY_THRESHOLD:
            factors.append(f"LOF: outlier neighborhood (score={lof_score:.3f})")

        return factors


# =============================================================================
# SECTION 8 — MACHINE HISTORY BUFFER
# =============================================================================

class MachineHistory:
    """
    Rolling observation buffer per machine — stores recent feature dicts
    and prediction results for trend analysis.
    """

    def __init__(self, machine_id: str, maxlen: int = MACHINE_HISTORY_LEN) -> None:
        self.machine_id = machine_id
        self._features  : deque[dict[str, float]]   = deque(maxlen=maxlen)
        self._results   : deque[PredictionResult]   = deque(maxlen=maxlen)

    def add(self, features: dict[str, float], result: PredictionResult) -> None:
        self._features.append(features)
        self._results.append(result)

    def risk_trend(self, window: int = 5) -> Optional[float]:
        """
        Compute the average risk_score over the last `window` observations.

        Args:
            window : Number of recent observations to average.

        Returns:
            Mean risk score, or None if fewer than 2 observations.
        """
        recent = list(self._results)[-window:]
        if len(recent) < 2:
            return None
        return round(sum(r.risk_score for r in recent) / len(recent), 4)

    def latest_result(self) -> Optional[PredictionResult]:
        """Return the most recent PredictionResult, or None."""
        return self._results[-1] if self._results else None

    def observation_count(self) -> int:
        return len(self._results)


# =============================================================================
# SECTION 9 — PREDICT ENGINE
# =============================================================================

class PredictEngine:
    """
    MFI Predictive AI Engine — trains and scores per-machine-type models.

    Lifecycle:
      1. BASELINE phase (first `baseline_cycles` cycles):
         - Records are ingested as training data via add_baseline().
         - Models are NOT scored yet.
         - Once baseline_cycles is complete, train() is called automatically.

      2. SCORING phase (all subsequent cycles):
         - score_batch() converts each MFIEnrichedRecord → PredictionResult.
         - Risk scores and factors are attached to results.
         - MachineHistory tracks trends per machine.

    Model granularity:
      - One ModelBundle per machine type (CNC, PRESS, ROBOT, CONVEYOR, WELDER).
      - All machines of the same type share one trained model.
      - This is the correct level for Phase 10 (not per-machine, which would
        require far more data per machine for reliable fitting).
    """

    def __init__(
        self,
        baseline_cycles : int = DEFAULT_BASELINE,
        random_state    : int = IF_RANDOM_STATE,
    ) -> None:
        """
        Args:
            baseline_cycles : Cycles of data to collect before training.
            random_state    : Passed to IsolationForest for reproducibility.
        """
        self._baseline_cycles   = baseline_cycles
        self._cycles_collected  = 0
        self._is_trained        = False
        self._random_state      = random_state

        self._extractor         = FeatureExtractor()
        self._scorer            = RiskScorer()
        self._explainer         = RiskExplainer()

        # Per-machine-type model bundles
        self._bundles   : dict[str, ModelBundle] = {}
        # Per-machine history
        self._history   : dict[str, MachineHistory] = {}

        # Cumulative scoring stats
        self._stats = {
            "baseline_records"  : 0,
            "scored_records"    : 0,
            "anomalies_if"      : 0,
            "anomalies_lof"     : 0,
            "critical_alerts"   : 0,
        }

        LOG.info(
            "PredictEngine initialized │ baseline_cycles=%d │ sklearn=%s",
            baseline_cycles,
            sklearn.__version__,
        )

    # ── Bundle management ─────────────────────────────────────────────────

    def _get_bundle(self, machine_type: str) -> ModelBundle:
        """Return (or create) the ModelBundle for a machine type."""
        if machine_type not in self._bundles:
            self._bundles[machine_type] = ModelBundle(machine_type)
        return self._bundles[machine_type]

    # ── Baseline accumulation ─────────────────────────────────────────────

    def add_baseline(self, records: list[MFIEnrichedRecord]) -> int:
        """
        Add one cycle's records to the baseline training buffer.

        Only records with status RUN or IDLE are used as baseline
        (FAULT and MAINTENANCE are already anomalous — training on them
        would corrupt the healthy-state model).

        Args:
            records : Enriched records from one MFICore cycle.

        Returns:
            Number of records accepted into the baseline.
        """
        accepted = 0
        for record in records:
            # Filter: only healthy statuses for baseline
            if record.status in ("FAULT", "MAINTENANCE"):
                continue

            features    = FeatureExtractor.extract(record)
            X           = FeatureExtractor.to_array(features)
            bundle      = self._get_bundle(record.machine_type)
            bundle.add_training_data(X)
            accepted    += 1
            self._stats["baseline_records"] += 1

        self._cycles_collected += 1
        LOG.debug(
            "Baseline cycle %d/%d │ accepted=%d/%d records",
            self._cycles_collected,
            self._baseline_cycles,
            accepted,
            len(records),
        )
        return accepted

    def baseline_ready(self) -> bool:
        """Return True when enough baseline cycles have been collected."""
        return self._cycles_collected >= self._baseline_cycles

    # ── Training ──────────────────────────────────────────────────────────

    def train(self) -> dict[str, int]:
        """
        Fit all ModelBundles on the accumulated baseline data.

        Returns:
            Dict mapping machine_type → n_training_samples.

        Raises:
            RuntimeError : If no bundles have training data.
        """
        if not self._bundles:
            raise RuntimeError("No baseline data accumulated. Call add_baseline() first.")

        results: dict[str, int] = {}
        for mtype, bundle in self._bundles.items():
            try:
                n = bundle.fit()
                results[mtype] = n
            except ValueError as exc:
                LOG.warning("ModelBundle[%s] fit skipped: %s", mtype, exc)
                results[mtype] = 0

        self._is_trained = True
        LOG.info(
            "PredictEngine trained │ bundles=%d │ samples=%s",
            len(results),
            results,
        )
        return results

    # ── Scoring ───────────────────────────────────────────────────────────

    def score_one(self, record: MFIEnrichedRecord) -> PredictionResult:
        """
        Score one MFIEnrichedRecord and return a PredictionResult.

        If the model is not yet trained, returns a NOT_FITTED result.
        If scoring fails, returns an ERROR result with log entry.

        Args:
            record : Enriched record to score.

        Returns:
            PredictionResult with all risk fields populated.
        """
        features = FeatureExtractor.extract(record)
        X        = FeatureExtractor.to_array(features)

        base_result = dict(
            machine_id      = record.machine_id,
            machine_type    = record.machine_type,
            site_id         = record.site_id,
            timestamp       = record.timestamp,
            features_used   = features,
        )

        # ── Not yet trained ───────────────────────────────────────────────
        if not self._is_trained:
            return PredictionResult(
                **base_result,
                model_status = "BASELINE",
            )

        bundle = self._get_bundle(record.machine_type)
        if not bundle.is_fitted:
            return PredictionResult(
                **base_result,
                model_status = "NOT_FITTED",
            )

        # ── Score ─────────────────────────────────────────────────────────
        try:
            if_score, lof_score = bundle.score(X)
        except Exception as exc:
            LOG.error(
                "Scoring ERROR │ machine=%s │ type=%s │ %s",
                record.machine_id, record.machine_type, exc,
            )
            return PredictionResult(
                **base_result,
                model_status = "ERROR",
            )

        # ── Classify anomalies ────────────────────────────────────────────
        if_anomaly  = if_score  < IF_ANOMALY_THRESHOLD
        lof_anomaly = lof_score < LOF_ANOMALY_THRESHOLD
        if_critical = if_score  < IF_CRITICAL_THRESHOLD

        # ── Composite risk ────────────────────────────────────────────────
        risk_score, risk_level = self._scorer.compute_risk(if_score, lof_score)

        # ── Explain ───────────────────────────────────────────────────────
        factors = self._explainer.explain(features, if_score, lof_score, record)

        # ── Update stats ──────────────────────────────────────────────────
        self._stats["scored_records"] += 1
        if if_anomaly:
            self._stats["anomalies_if"]  += 1
        if lof_anomaly:
            self._stats["anomalies_lof"] += 1
        if risk_level == "CRITICAL":
            self._stats["critical_alerts"] += 1

        result = PredictionResult(
            **base_result,
            if_score        = round(if_score,  4),
            lof_score       = round(lof_score, 4),
            if_anomaly      = if_anomaly,
            lof_anomaly     = lof_anomaly,
            if_critical     = if_critical,
            risk_score      = risk_score,
            risk_level      = risk_level,
            risk_factors    = factors,
            model_status    = "SCORED",
        )

        # ── Track history ─────────────────────────────────────────────────
        if record.machine_id not in self._history:
            self._history[record.machine_id] = MachineHistory(record.machine_id)
        self._history[record.machine_id].add(features, result)

        LOG.debug(
            "Scored │ %-10s │ %-8s │ IF=%6.3f LOF=%6.3f │ risk=%-8s (%.3f) │ factors=%d",
            record.machine_id,
            record.machine_type,
            if_score,
            lof_score,
            risk_level,
            risk_score,
            len(factors),
        )
        return result

    def score_batch(
        self,
        records : list[MFIEnrichedRecord],
    ) -> list[PredictionResult]:
        """
        Score a full batch of enriched records.

        Args:
            records : Enriched records from one MFICore cycle.

        Returns:
            List of PredictionResult (one per input record).
        """
        results = [self.score_one(r) for r in records]

        scored  = sum(1 for r in results if r.model_status == "SCORED")
        crits   = sum(1 for r in results if r.risk_level   == "CRITICAL")
        highs   = sum(1 for r in results if r.risk_level   == "HIGH")
        anomaly_if  = sum(1 for r in results if r.if_anomaly)
        anomaly_lof = sum(1 for r in results if r.lof_anomaly)

        LOG.info(
            "score_batch │ total=%d │ scored=%d │ CRIT=%d HIGH=%d │ "
            "if_anom=%d lof_anom=%d",
            len(records), scored, crits, highs, anomaly_if, anomaly_lof,
        )
        return results

    # ── Fleet summary ─────────────────────────────────────────────────────

    def fleet_summary(
        self,
        results: list[PredictionResult],
    ) -> dict[str, Any]:
        """
        Build a fleet-level AI summary from a batch of PredictionResults.

        Args:
            results : PredictionResult list from score_batch().

        Returns:
            Dict with fleet-level risk distribution and top concerns.
        """
        scored  = [r for r in results if r.model_status == "SCORED"]
        if not scored:
            return {"status": "no scored results"}

        level_dist: dict[str, int] = {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "CRITICAL": 0}
        for r in scored:
            level_dist[r.risk_level] = level_dist.get(r.risk_level, 0) + 1

        avg_risk = round(sum(r.risk_score for r in scored) / len(scored), 4)
        top_concerns = sorted(
            scored, key=lambda r: r.risk_score, reverse=True
        )[:5]

        return {
            "scored_machines"   : len(scored),
            "avg_risk_score"    : avg_risk,
            "risk_distribution" : level_dist,
            "top_concerns"      : [
                {
                    "machine_id"  : r.machine_id,
                    "machine_type": r.machine_type,
                    "risk_level"  : r.risk_level,
                    "risk_score"  : r.risk_score,
                    "factors"     : r.risk_factors[:3],
                }
                for r in top_concerns
            ],
            "engine_stats"      : self._stats.copy(),
        }

    # ── Model persistence ─────────────────────────────────────────────────

    def save_models(self, path: str) -> None:
        """
        Serialize all fitted ModelBundles to a pickle file.

        Args:
            path : Output file path.
        """
        data = {
            mtype: bundle.serialize()
            for mtype, bundle in self._bundles.items()
            if bundle.is_fitted
        }
        with open(path, "wb") as fh:
            pickle.dump(data, fh)
        LOG.info("Models saved → %s (%d bundles)", path, len(data))

    def load_models(self, path: str) -> None:
        """
        Load serialized ModelBundles from a pickle file.

        Args:
            path : Input file path.
        """
        with open(path, "rb") as fh:
            data: dict[str, bytes] = pickle.load(fh)
        for mtype, raw in data.items():
            self._bundles[mtype] = ModelBundle.deserialize(mtype, raw)
        self._is_trained = True
        LOG.info("Models loaded ← %s (%d bundles)", path, len(data))

    @property
    def is_trained(self) -> bool:
        return self._is_trained

    @property
    def stats(self) -> dict[str, int]:
        return self._stats.copy()


# =============================================================================
# SECTION 10 — PREDICT SERVICE
# =============================================================================

class PredictService:
    """
    MFI Predict Service — wraps PredictEngine with a simple run() interface.

    Manages the two-phase lifecycle (baseline → scoring) automatically.
    Provides a clean summary after the full run.
    """

    def __init__(
        self,
        baseline_cycles : int   = DEFAULT_BASELINE,
        site_id         : str   = DEFAULT_SITE,
    ) -> None:
        self._engine        = PredictEngine(baseline_cycles=baseline_cycles)
        self._site_id       = site_id
        self._all_results   : list[PredictionResult] = []
        self._cycle_count   = 0
        LOG.info(
            "PredictService initialized │ site=%s │ baseline=%d cycles",
            site_id, baseline_cycles,
        )

    def ingest(self, records: list[MFIEnrichedRecord]) -> list[PredictionResult]:
        """
        Ingest one cycle's records — auto-switches baseline → scoring.

        Args:
            records : MFIEnrichedRecord list from MFICore.run_cycle().

        Returns:
            List of PredictionResult (BASELINE status during warm-up,
            SCORED status after training).
        """
        self._cycle_count += 1

        if not self._engine.baseline_ready():
            # Still collecting baseline
            self._engine.add_baseline(records)
            # Return BASELINE placeholder results
            results = [
                PredictionResult(
                    machine_id      = r.machine_id,
                    machine_type    = r.machine_type,
                    site_id         = r.site_id,
                    timestamp       = r.timestamp,
                    model_status    = "BASELINE",
                    features_used   = FeatureExtractor.extract(r),
                )
                for r in records
            ]
        else:
            # Train once, immediately after baseline is complete
            if not self._engine.is_trained:
                LOG.info(
                    "Baseline complete — training models "
                    "(baseline_cycles=%d, total_records=%d) ...",
                    self._engine._baseline_cycles,
                    self._engine._stats["baseline_records"],
                )
                self._engine.train()

            results = self._engine.score_batch(records)

        self._all_results.extend(results)
        return results

    def summary(self, last_results: list[PredictionResult]) -> dict[str, Any]:
        """
        Return a combined fleet + engine summary.

        Args:
            last_results : PredictionResult list from the most recent cycle.

        Returns:
            Summary dict.
        """
        return {
            "cycles_total"  : self._cycle_count,
            "is_trained"    : self._engine.is_trained,
            "fleet"         : self._engine.fleet_summary(last_results),
            "engine_stats"  : self._engine.stats,
        }

    def engine(self) -> PredictEngine:
        return self._engine


# =============================================================================
# SECTION 11 — CYCLE SUMMARY
# =============================================================================

def print_predict_summary(
    cycle_num   : int,
    results     : list[PredictionResult],
    is_baseline : bool,
) -> None:
    """
    Print a structured prediction summary for one cycle.

    Args:
        cycle_num   : Current cycle number.
        results     : PredictionResult list for this cycle.
        is_baseline : True if still in baseline phase.
    """
    if is_baseline:
        LOG.info(
            "─── PREDICT CYCLE %02d ─── [BASELINE PHASE] records=%d",
            cycle_num, len(results),
        )
        return

    scored  = [r for r in results if r.model_status == "SCORED"]
    crits   = sum(1 for r in scored if r.risk_level == "CRITICAL")
    highs   = sum(1 for r in scored if r.risk_level == "HIGH")
    meds    = sum(1 for r in scored if r.risk_level == "MEDIUM")
    lows    = sum(1 for r in scored if r.risk_level == "LOW")
    avg_r   = (
        round(sum(r.risk_score for r in scored) / len(scored), 3)
        if scored else 0.0
    )

    LOG.info(
        "─── PREDICT CYCLE %02d ─── scored=%d │ avg_risk=%.3f │ "
        "CRIT=%d HIGH=%d MED=%d LOW=%d",
        cycle_num, len(scored), avg_r, crits, highs, meds, lows,
    )

    # Log top-3 concerns per cycle
    for r in sorted(scored, key=lambda x: x.risk_score, reverse=True)[:3]:
        if r.risk_level in ("CRITICAL", "HIGH"):
            LOG.warning(
                "  ⚠ %-10s │ %-8s │ risk=%-8s %.3f │ %s",
                r.machine_id,
                r.machine_type,
                r.risk_level,
                r.risk_score,
                "; ".join(r.risk_factors[:2]) or "—",
            )


# =============================================================================
# SECTION 12 — OUTPUT WRITER
# =============================================================================

def write_output(results: list[PredictionResult], filepath: str) -> None:
    """
    Write all prediction results to a JSON file.

    Args:
        results  : List of PredictionResult to export.
        filepath : Destination file path.
    """
    data = [r.to_dict() for r in results]
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    LOG.info("Output written → %s (%d results)", filepath, len(data))


# =============================================================================
# SECTION 13 — SELF-TEST
# =============================================================================

def run_self_test() -> bool:
    """
    Self-test for Phase 10. Validates:
      1.  FeatureExtractor.extract() returns all FEATURE_COLS keys.
      2.  FeatureExtractor.to_array() returns shape (1, n_features).
      3.  FeatureExtractor.batch_to_array() returns shape (n, n_features).
      4.  ModelBundle accumulates training data correctly.
      5.  ModelBundle.fit() trains both pipelines without error.
      6.  ModelBundle.score() returns (float, float) after fitting.
      7.  ModelBundle.score() raises NotFittedError before fitting.
      8.  ModelBundle pickle round-trip preserves scoring behavior.
      9.  RiskScorer.compute_risk() returns (0.0, 'LOW') for None inputs.
      10. RiskScorer.compute_risk() returns high risk for extreme scores.
      11. RiskScorer.compute_risk() risk_score always in [0, 1].
      12. RiskExplainer.explain() flags FAULT state as risk factor.
      13. RiskExplainer.explain() flags critical temperature.
      14. RiskExplainer.explain() returns empty list for healthy record.
      15. PredictEngine baseline lifecycle: add_baseline → train → score.
      16. PredictEngine.score_one() returns BASELINE before training.
      17. PredictEngine.score_one() returns SCORED after training.
      18. PredictEngine.score_batch() returns one result per record.
      19. PredictEngine.fleet_summary() returns required keys.
      20. PredictionResult rejects invalid risk_level.
      21. PredictionResult is JSON-serializable.
      22. MachineHistory.risk_trend() returns None with < 2 observations.
      23. Full pipeline: MFICore → PredictService → scored results.
      24. PredictService.summary() returns fleet and engine_stats.
      25. PredictEngine model save/load round-trip preserves scores.

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

    # ── Build test records ────────────────────────────────────────────────
    def _make_er(
        mid     : str,
        status  : str,
        alarm   : int   = 0,
        temp    : float = 50.0,
        speed   : float = 1000.0,
        cycles  : int   = 5,
    ) -> MFIEnrichedRecord:
        std = MFIStandardModel(
            site_id="mecanitec", machine_id=mid, machine_type="CNC",
            protocol="opcua", status=status, alarm_code=alarm,
            piece_count=10000,
            good_count=5 if status == "RUN" else 0,
            bad_count=0,
            cycle_time_sec=42.0 if status == "RUN" else 0.0,
            temperature_c=temp, speed=speed,
            timestamp="2026-05-16T20:00:00+00:00",
        )
        col = MFICollector()
        for _ in range(cycles):
            col.collect([std])
        return MFIEnricher().enrich(std, col.get_state(mid))

    er_run   = _make_er("m01", "RUN",   temp=50.0,  speed=1200.0)
    er_fault = _make_er("m02", "FAULT", temp=115.0, speed=0.0, alarm=300)

    # ── Tests 1-3: FeatureExtractor ───────────────────────────────────────
    feats = FeatureExtractor.extract(er_run)
    check(
        "FeatureExtractor.extract() returns all FEATURE_COLS keys",
        all(k in feats for k in FEATURE_COLS),
        str([k for k in FEATURE_COLS if k not in feats]),
    )

    X_one = FeatureExtractor.to_array(feats)
    check(
        "FeatureExtractor.to_array() returns shape (1, n_features)",
        X_one.shape == (1, len(FEATURE_COLS)),
        str(X_one.shape),
    )

    X_batch = FeatureExtractor.batch_to_array([er_run, er_fault])
    check(
        "batch_to_array() returns shape (2, n_features)",
        X_batch.shape == (2, len(FEATURE_COLS)),
        str(X_batch.shape),
    )

    # ── Tests 4-8: ModelBundle ────────────────────────────────────────────
    bundle = ModelBundle("CNC")
    X_train = np.random.randn(60, len(FEATURE_COLS))
    bundle.add_training_data(X_train[:30])
    bundle.add_training_data(X_train[30:])
    check(
        "ModelBundle accumulates training data (2 batches)",
        len(bundle._train_buffer) == 2,
    )

    n = bundle.fit()
    check("ModelBundle.fit() trains on 60 samples", n == 60, f"got {n}")

    if_s, lof_s = bundle.score(X_one)
    check(
        "ModelBundle.score() returns (float, float)",
        isinstance(if_s, float) and isinstance(lof_s, float),
        f"if={if_s} lof={lof_s}",
    )

    bundle2 = ModelBundle("CNC_unfitted")
    try:
        bundle2.score(X_one)
        check("ModelBundle.score() raises NotFittedError before fit", False)
    except NotFittedError:
        check("ModelBundle.score() raises NotFittedError before fit", True)

    raw = bundle.serialize()
    bundle3 = ModelBundle.deserialize("CNC", raw)
    if3s, lof3s = bundle3.score(X_one)
    check(
        "ModelBundle pickle round-trip preserves scoring",
        abs(if3s - if_s) < 1e-9 and abs(lof3s - lof_s) < 1e-9,
        f"original=({if_s},{lof_s}) restored=({if3s},{lof3s})",
    )

    # ── Tests 9-11: RiskScorer ────────────────────────────────────────────
    rs, rl = RiskScorer.compute_risk(None, None)
    check("compute_risk(None, None) returns (0.0, 'LOW')", rs == 0.0 and rl == "LOW")

    rs2, rl2 = RiskScorer.compute_risk(-0.80, -2.5)
    check(
        "compute_risk with extreme scores returns HIGH or CRITICAL",
        rl2 in ("HIGH", "CRITICAL"),
        f"risk={rs2:.3f} level={rl2}",
    )

    for if_v, lof_v in [(-0.3, -0.8), (-0.6, -1.5), (-0.9, -3.0), (0.0, 0.0)]:
        rs_t, _ = RiskScorer.compute_risk(if_v, lof_v)
        check(
            f"risk_score in [0,1] for IF={if_v} LOF={lof_v}",
            0.0 <= rs_t <= 1.0,
            f"got {rs_t}",
        )

    # ── Tests 12-14: RiskExplainer ────────────────────────────────────────
    factors_fault = RiskExplainer.explain(
        FeatureExtractor.extract(er_fault),
        if_score=-0.75, lof_score=-2.1, record=er_fault,
    )
    check(
        "RiskExplainer flags FAULT state",
        any("FAULT" in f for f in factors_fault),
        str(factors_fault),
    )
    check(
        "RiskExplainer flags critical temperature",
        any("CRITICAL" in f or "Temperature" in f for f in factors_fault),
        str(factors_fault),
    )

    factors_healthy = RiskExplainer.explain(
        FeatureExtractor.extract(er_run),
        if_score=-0.30, lof_score=-0.90, record=er_run,
    )
    check(
        "RiskExplainer returns 0 factors for healthy normal-score record",
        len(factors_healthy) == 0,
        str(factors_healthy),
    )

    # ── Tests 15-19: PredictEngine ────────────────────────────────────────
    # Build a small fleet for engine testing
    core    = MFICore(site_id="mecanitec")
    engine  = PredictEngine(baseline_cycles=3)

    # Baseline phase
    for _ in range(3):
        r = core.run_cycle()
        engine.add_baseline(r["enriched"])

    check("Engine baseline_ready() after 3 cycles", engine.baseline_ready())

    # Score before training
    r_pre_train = core.run_cycle()
    result_pre  = engine.score_one(r_pre_train["enriched"][0])
    check(
        "score_one() returns BASELINE before training",
        result_pre.model_status == "BASELINE",
        result_pre.model_status,
    )

    # Train
    engine.train()
    check("Engine is_trained after train()", engine.is_trained)

    # Score after training
    r_post = core.run_cycle()
    result_post = engine.score_one(r_post["enriched"][0])
    check(
        "score_one() returns SCORED after training",
        result_post.model_status == "SCORED",
        result_post.model_status,
    )

    # score_batch
    batch_results = engine.score_batch(r_post["enriched"])
    check(
        "score_batch() returns one result per record",
        len(batch_results) == len(r_post["enriched"]),
        f"results={len(batch_results)} records={len(r_post['enriched'])}",
    )

    # fleet_summary
    summ = engine.fleet_summary(batch_results)
    required = ["scored_machines", "avg_risk_score", "risk_distribution", "top_concerns"]
    check(
        "fleet_summary() has required keys",
        all(k in summ for k in required),
        str([k for k in required if k not in summ]),
    )

    # ── Test 20: PredictionResult validates risk_level ────────────────────
    try:
        PredictionResult(
            machine_id="m01", machine_type="CNC", site_id="s",
            timestamp="2026-05-16T20:00:00+00:00",
            risk_level="EXTREME",
        )
        check("PredictionResult rejects invalid risk_level", False)
    except ValidationError:
        check("PredictionResult rejects invalid risk_level", True)

    # ── Test 21: PredictionResult JSON-serializable ────────────────────────
    try:
        _ = json.dumps(result_post.to_dict())
        check("PredictionResult is JSON-serializable", True)
    except (TypeError, ValueError) as exc:
        check("PredictionResult is JSON-serializable", False, str(exc))

    # ── Test 22: MachineHistory.risk_trend() ─────────────────────────────
    hist = MachineHistory("m01")
    check("risk_trend() returns None with 0 observations", hist.risk_trend() is None)
    hist.add(feats, result_post)
    check("risk_trend() returns None with 1 observation", hist.risk_trend() is None)

    # ── Test 23: Full pipeline via PredictService ─────────────────────────
    service = PredictService(baseline_cycles=3, site_id="mecanitec")
    core2   = MFICore(site_id="mecanitec")

    last_results: list[PredictionResult] = []
    for cycle in range(8):
        r2          = core2.run_cycle()
        last_results = service.ingest(r2["enriched"])

    scored_count = sum(1 for r in last_results if r.model_status == "SCORED")
    check(
        "Full PredictService pipeline: scored results after warm-up",
        scored_count >= 40,
        f"scored={scored_count}",
    )

    # ── Test 24: PredictService.summary() structure ────────────────────────
    summ2 = service.summary(last_results)
    check(
        "PredictService.summary() has fleet and engine_stats",
        "fleet" in summ2 and "engine_stats" in summ2,
        str(list(summ2.keys())),
    )

    # ── Test 25: Model save/load round-trip ───────────────────────────────
    import tempfile, os
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        model_path = f.name

    try:
        engine.save_models(model_path)

        engine2 = PredictEngine(baseline_cycles=3)
        engine2.load_models(model_path)

        # Should score identically
        test_r   = r_post["enriched"][0]
        score_a  = engine.score_one(test_r)
        score_b  = engine2.score_one(test_r)
        check(
            "Model save/load: scores match after round-trip",
            abs((score_a.if_score or 0) - (score_b.if_score or 0)) < 1e-6,
            f"a={score_a.if_score} b={score_b.if_score}",
        )
    finally:
        if os.path.exists(model_path):
            os.unlink(model_path)

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
    parser = argparse.ArgumentParser(
        prog        = "mfi_phase_10_predict.py",
        description = (
            f"MFI Phase {PHASE_ID} — {PHASE_NAME} v{PHASE_VERSION}\n"
            "IsolationForest + LOF anomaly detection and maintenance risk scoring."
        ),
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--self-test", action="store_true",
                        help="Run built-in self-test and exit.")
    parser.add_argument("--cycles", type=int, default=DEFAULT_CYCLES, metavar="N",
                        help=f"Total pipeline cycles to run (default: {DEFAULT_CYCLES}).")
    parser.add_argument("--baseline", type=int, default=DEFAULT_BASELINE, metavar="N",
                        help=f"Baseline cycles before scoring starts (default: {DEFAULT_BASELINE}).")
    parser.add_argument("--output", type=str, default=None, metavar="FILE",
                        help="Write last cycle prediction results to JSON file.")
    parser.add_argument("--save-models", type=str, default=None, metavar="FILE",
                        help="Save trained models to .pkl file after training.")
    parser.add_argument("--load-models", type=str, default=None, metavar="FILE",
                        help="Load pre-trained models from .pkl file (skips baseline).")
    parser.add_argument("--site", type=str, default=DEFAULT_SITE, metavar="SITE_ID",
                        help=f"Site identifier (default: {DEFAULT_SITE}).")
    parser.add_argument("--interval", type=float, default=0.5, metavar="SECS",
                        help="Sleep between cycles (default: 0.5).")
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

    service = PredictService(baseline_cycles=args.baseline, site_id=args.site)
    core    = MFICore(site_id=args.site)

    # Optionally pre-load models (skip baseline phase)
    if args.load_models:
        service.engine().load_models(args.load_models)
        LOG.info("Pre-trained models loaded — skipping baseline phase.")

    last_results: list[PredictionResult] = []

    for cycle in range(1, args.cycles + 1):
        result      = core.run_cycle()
        enriched    = result["enriched"]
        predictions = service.ingest(enriched)
        last_results = predictions

        is_baseline = not service.engine().is_trained
        print_predict_summary(cycle, predictions, is_baseline)

        if cycle < args.cycles:
            time.sleep(args.interval)

    # Save models if requested
    if args.save_models and service.engine().is_trained:
        service.engine().save_models(args.save_models)

    # Output last cycle results
    if args.output:
        write_output(last_results, args.output)

    # Final summary
    summ    = service.summary(last_results)
    fleet   = summ.get("fleet", {})
    stats   = summ.get("engine_stats", {})
    dist    = fleet.get("risk_distribution", {})

    LOG.info(
        "══ PREDICT FINAL ══ cycles=%d │ baseline=%d │ scored=%d │ "
        "avg_risk=%.3f │ CRIT=%d HIGH=%d MED=%d LOW=%d",
        summ["cycles_total"],
        args.baseline,
        stats.get("scored_records", 0),
        fleet.get("avg_risk_score", 0.0),
        dist.get("CRITICAL", 0),
        dist.get("HIGH", 0),
        dist.get("MEDIUM", 0),
        dist.get("LOW", 0),
    )

    top = fleet.get("top_concerns", [])
    if top:
        LOG.info("Top concerns this cycle:")
        for c in top[:3]:
            LOG.warning(
                "  ⚠ %-10s │ %-8s │ %-8s │ risk=%.3f │ %s",
                c["machine_id"], c["machine_type"], c["risk_level"],
                c["risk_score"],
                "; ".join(c["factors"][:2]) if c["factors"] else "—",
            )

    LOG.info("Phase %s complete.", PHASE_ID)


# =============================================================================
# SECTION 15 — ENTRY GUARD
# =============================================================================
if __name__ == "__main__":
    main()
