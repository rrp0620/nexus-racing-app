"""
Nexus Racing Analytics — Probabilistic Model & Edge Detection Engine

Provides:
    NexusModel          — computes win probabilities and fair odds from race data
    EdgeDetector        — classifies value, sizes bets via Kelly criterion
    PaceAnalyzer        — classifies pace types and computes pace scenario
    detect_class_changes — flags horses dropping in class (morning line proxy)
    score_jt_combo      — jockey-trainer combo scoring
    confidence_interval — win probability confidence bands by field size
    calculate_odds      — module-level convenience wrapper
    backtest_stub       — placeholder for historical validation
"""

import hashlib
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEATURE_WEIGHTS = {
    "speed":         0.35,
    "jockey":        0.20,
    "trainer":       0.15,
    "form_cycle":    0.05,   # reduced to make room for pace
    "class_level":   0.05,   # reduced to make room for pace
    "post_position": 0.10,
    "pace":          0.10,   # new
}

# Optimal days-off windows (form cycle research)
FORM_CYCLE = {
    "peak_min": 14,
    "peak_max": 35,
    "layoff_threshold": 45,
    "penalty_per_day_over": 0.005,   # gentle decay beyond layoff
    "too_fresh_penalty":   0.15,     # < 7 days off
}

# Post-position bias table (inside posts slightly favoured on dirt sprints)
# Index = post number (1-based), value = relative advantage
DEFAULT_POST_BIAS = {
    1: 1.05, 2: 1.04, 3: 1.03, 4: 1.02, 5: 1.01,
    6: 1.00, 7: 0.99, 8: 0.98, 9: 0.97, 10: 0.96,
    11: 0.95, 12: 0.94,
}


# ---------------------------------------------------------------------------
# PaceAnalyzer
# ---------------------------------------------------------------------------

class PaceAnalyzer:
    """Classify horses by running style and determine field pace scenario.

    Since we lack full pace figures, we use heuristics based on:
    - last_speed:  higher speeds often reflect early burst ability
    - days_off:    freshly-returned horses tend to run more conservatively

    Pace types
    ----------
    E   — pure early speed (front-runner)
    EP  — early/presser (tracks the lead)
    P   — presser / mid-pack
    S   — sustained / closer

    Scenarios
    ---------
    lone speed    — exactly 1 E-type horse (huge pace advantage)
    contested     — 3 or more E-type horses (pace duel likely)
    closers' race — majority S/P types (pace shape favours closers)
    normal        — mixed field, no dominant scenario
    """

    # Advantage scores by pace type in each scenario
    _ADVANTAGE_TABLE: Dict[str, Dict[str, float]] = {
        "lone speed":    {"E": 0.90, "EP": 0.30, "P": 0.10, "S": -0.10},
        "contested":     {"E": -0.20, "EP": 0.10, "P": 0.20, "S": 0.30},
        "closers' race": {"E": 0.10, "EP": 0.05, "P": 0.00, "S": 0.15},
        "normal":        {"E": 0.10, "EP": 0.10, "P": 0.05, "S": 0.05},
    }

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def classify(self) -> Tuple[Dict[str, Dict[str, Any]], str]:
        """Classify each horse and return pace info + field scenario.

        Returns
        -------
        pace_map : dict
            {horse_name: {"pace_type": str, "pace_advantage_score": float}}
        scenario : str
            One of 'lone speed', 'contested', 'closers' race', 'normal'
        """
        df = self.df

        # Compute within-field speed percentile rank (0-1)
        speeds = df["last_speed"]
        speed_pct = speeds.rank(pct=True)           # 1.0 = fastest
        days = df.get("days_off", pd.Series(21, index=df.index))

        pace_types: Dict[str, str] = {}

        for idx in df.index:
            spd_pct = speed_pct.loc[idx]
            d_off   = days.loc[idx] if idx in days.index else 21.0

            # Heuristic classification
            # Fresh horses (≤10 days) usually don't go hard early
            freshness_penalty = d_off <= 10

            if spd_pct >= 0.75 and not freshness_penalty:
                pace_type = "E"
            elif spd_pct >= 0.55:
                pace_type = "EP"
            elif spd_pct >= 0.35:
                pace_type = "P"
            else:
                pace_type = "S"

            name = df.loc[idx, "name"] if "name" in df.columns else str(idx)
            pace_types[name] = pace_type

        # Determine field scenario
        type_counts = {t: sum(1 for v in pace_types.values() if v == t)
                       for t in ("E", "EP", "P", "S")}
        n = len(pace_types)
        n_early = type_counts["E"]
        n_sustained = type_counts["S"] + type_counts["P"]

        if n_early == 1:
            scenario = "lone speed"
        elif n_early >= 3:
            scenario = "contested"
        elif n_sustained >= (n * 0.6):
            scenario = "closers' race"
        else:
            scenario = "normal"

        adv_table = self._ADVANTAGE_TABLE[scenario]
        pace_map: Dict[str, Dict[str, Any]] = {}
        for name, ptype in pace_types.items():
            pace_map[name] = {
                "pace_type": ptype,
                "pace_advantage_score": adv_table.get(ptype, 0.0),
            }

        return pace_map, scenario


# ---------------------------------------------------------------------------
# Class change detector
# ---------------------------------------------------------------------------

def detect_class_changes(df: pd.DataFrame) -> pd.Series:
    """Score class-change advantage using morning line as a class proxy.

    Lower morning-line odds → higher class horse.  A horse with morning
    line significantly *higher* than the field median was racing at a
    lower class previously and is now 'dropping in class'.

    Returns
    -------
    pd.Series
        class_change_score per row (0.0 = no advantage; up to 0.20).
    """
    ml = df["morning_line"]
    median_ml = ml.median()

    scores = []
    for ml_val in ml:
        ratio = ml_val / median_ml  # >1 means longer odds → lower class horse
        if ratio >= 1.40:
            # Big class dropper — significant advantage
            scores.append(0.20)
        elif ratio >= 1.20:
            # Standard class drop — moderate advantage
            scores.append(0.10)
        else:
            scores.append(0.0)

    return pd.Series(scores, index=df.index, name="class_change_score")


# ---------------------------------------------------------------------------
# Jockey-Trainer combo scoring
# ---------------------------------------------------------------------------

def score_jt_combo(df: pd.DataFrame) -> pd.Series:
    """Generate consistent combo win % boosts from jockey + trainer names.

    Uses SHA-256 hashing to derive a deterministic (but realistic-looking)
    combo win percentage.  Top combos get a +10–15 % boost applied later.

    Returns
    -------
    pd.Series
        combo_boost per row (fractional, typically 0.0–0.15).
    """
    boosts = []
    for _, row in df.iterrows():
        jockey  = str(row.get("jockey", ""))
        trainer = str(row.get("trainer", ""))
        key = f"{jockey}|{trainer}".encode()
        digest = hashlib.sha256(key).digest()
        # Take first 2 bytes → 0-65535, map to 0.08–0.30 win%
        raw_val = int.from_bytes(digest[:2], "big") / 65535.0
        combo_win_pct = 0.08 + raw_val * 0.22  # range [0.08, 0.30]

        # Only top combos (win% >= 0.22) get a meaningful boost
        if combo_win_pct >= 0.27:
            boost = 0.15
        elif combo_win_pct >= 0.22:
            boost = 0.10
        else:
            boost = 0.0

        boosts.append(boost)

    return pd.Series(boosts, index=df.index, name="combo_boost")


# ---------------------------------------------------------------------------
# Confidence intervals
# ---------------------------------------------------------------------------

def confidence_interval(win_prob: float, n_runners: int) -> Tuple[float, float]:
    """Compute approximate 90% confidence bounds on a win probability.

    Uses a Wilson-score-inspired formula to account for the uncertainty
    introduced by field size and the inherent difficulty of predicting
    one-from-many outcomes.

    Parameters
    ----------
    win_prob : float
        Model win probability (0-1).
    n_runners : int
        Number of horses in the race.

    Returns
    -------
    (low, high) : Tuple[float, float]
        Lower and upper bounds, clipped to [0, 1].
    """
    if n_runners <= 1:
        return (win_prob, win_prob)

    # Effective sample size proxy: more runners = more uncertainty
    # Each additional runner adds uncertainty; we treat n_runners as a
    # rough proxy for the number of independent "observations".
    z = 1.645  # 90% confidence (one-sided 95%)
    n_eff = max(n_runners * 2, 10)   # scale n with field size

    # Wilson interval
    p = win_prob
    denominator = 1 + z ** 2 / n_eff
    centre = (p + z ** 2 / (2 * n_eff)) / denominator
    margin  = (z * np.sqrt(p * (1 - p) / n_eff + z ** 2 / (4 * n_eff ** 2))
               / denominator)

    low  = float(np.clip(centre - margin, 0.0, 1.0))
    high = float(np.clip(centre + margin, 0.0, 1.0))
    return (round(low, 4), round(high, 4))


# ---------------------------------------------------------------------------
# NexusModel
# ---------------------------------------------------------------------------

class NexusModel:
    """Compute win probabilities and fair odds for a race field.

    Parameters
    ----------
    df : pd.DataFrame
        Race entries with at minimum: name, last_speed, days_off,
        morning_line.  Optional columns used when present:
        jockey_win_pct, trainer_win_pct, surface_win_pct, post_position,
        jockey, trainer.
    """

    def __init__(self, df: pd.DataFrame):
        self.raw = df.copy()
        self.n_runners = len(df)
        self.results: Optional[pd.DataFrame] = None

    # ---- public API -------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Execute the full model pipeline and return enriched DataFrame."""
        df = self.raw.copy()

        # --- Pace analysis -------------------------------------------------
        analyzer = PaceAnalyzer(df)
        pace_map, pace_scenario = analyzer.classify()

        # Build pace_score series aligned to df index
        pace_scores = []
        pace_types  = []
        for idx in df.index:
            name = df.loc[idx, "name"] if "name" in df.columns else str(idx)
            info = pace_map.get(name, {"pace_type": "P", "pace_advantage_score": 0.0})
            pace_types.append(info["pace_type"])
            # Normalise advantage score to 0-1 (raw range is roughly -0.20 to 0.90)
            raw_adv = info["pace_advantage_score"]
            normalised = (raw_adv + 0.20) / 1.10   # shift+scale to [0, 1]
            pace_scores.append(float(np.clip(normalised, 0.0, 1.0)))

        df["_pace_type"]  = pace_types
        df["_pace_score"] = pace_scores

        # --- Class change --------------------------------------------------
        class_scores = detect_class_changes(df)   # 0.0, 0.10, or 0.20
        df["_class_change"] = class_scores.values

        # --- JT combo ------------------------------------------------------
        combo_boosts = score_jt_combo(df)
        df["_combo_boost"] = combo_boosts.values

        # --- Feature scoring -----------------------------------------------
        df["_speed_score"]    = self._score_speed(df)
        df["_jockey_score"]   = self._score_jockey(df)
        df["_trainer_score"]  = self._score_trainer(df)
        df["_form_score"]     = self._score_form_cycle(df)
        df["_class_score"]    = self._score_class(df)
        df["_post_score"]     = self._score_post(df)

        # --- Weighted composite → raw power rating -------------------------
        df["power_rating"] = (
            df["_speed_score"]    * FEATURE_WEIGHTS["speed"]
            + df["_jockey_score"]   * FEATURE_WEIGHTS["jockey"]
            + df["_trainer_score"]  * FEATURE_WEIGHTS["trainer"]
            + df["_form_score"]     * FEATURE_WEIGHTS["form_cycle"]
            + df["_class_score"]    * FEATURE_WEIGHTS["class_level"]
            + df["_post_score"]     * FEATURE_WEIGHTS["post_position"]
            + df["_pace_score"]     * FEATURE_WEIGHTS["pace"]
            # Class-change bonus: scale into rating space (0-1 weights sum to 1)
            + df["_class_change"]   * 0.05
        )

        # Re-normalise power_rating to [0, 1] before softmax
        pr_min = df["power_rating"].min()
        pr_max = df["power_rating"].max()
        if pr_max > pr_min:
            df["power_rating"] = (df["power_rating"] - pr_min) / (pr_max - pr_min)

        # --- Convert to probabilities (softmax) ----------------------------
        df["win_prob"] = self._to_probabilities(df["power_rating"])

        # Apply JT combo boost (additive, then re-normalise)
        df["win_prob"] = df["win_prob"] + df["_combo_boost"] * 0.01
        df["win_prob"] = df["win_prob"] / df["win_prob"].sum()   # re-normalise

        # --- Fair odds -----------------------------------------------------
        df["fair_odds_decimal"] = self._prob_to_decimal_odds(df["win_prob"])
        df["fair_odds_american"] = df["fair_odds_decimal"].apply(
            self._decimal_to_american
        )

        # --- Market implied probability ------------------------------------
        df["market_prob"] = df["morning_line"].apply(self._ml_to_implied_prob)

        # --- Edge detection ------------------------------------------------
        df["edge_score"] = (
            (df["win_prob"] - df["market_prob"]) / df["market_prob"] * 100
        )

        # --- Expose pace metadata ------------------------------------------
        df["pace_type"]     = df["_pace_type"]
        df["pace_scenario"] = pace_scenario

        # --- Clean up internal columns ------------------------------------
        internal = [c for c in df.columns if c.startswith("_")]
        df.drop(columns=internal, inplace=True)

        self.results = df.sort_values("win_prob", ascending=False).reset_index(
            drop=True
        )
        return self.results

    # ---- feature scorers (each returns Series in 0-1) ---------------------

    @staticmethod
    def _score_speed(df: pd.DataFrame) -> pd.Series:
        """Normalise last_speed to 0-1 within the field."""
        floor = 70.0
        speeds = df["last_speed"].clip(lower=floor)
        span = speeds.max() - floor
        if span == 0:
            return pd.Series(0.5, index=df.index)
        return (speeds - floor) / span

    @staticmethod
    def _score_jockey(df: pd.DataFrame) -> pd.Series:
        """Score jockey win percentage.  Falls back to field median if missing."""
        if "jockey_win_pct" not in df.columns:
            return pd.Series(0.5, index=df.index)
        pcts = df["jockey_win_pct"].fillna(df["jockey_win_pct"].median())
        max_pct = pcts.max()
        if max_pct == 0:
            return pd.Series(0.5, index=df.index)
        return pcts / max_pct

    @staticmethod
    def _score_trainer(df: pd.DataFrame) -> pd.Series:
        """Score trainer win percentage with a first-off-layoff boost/penalty."""
        if "trainer_win_pct" not in df.columns:
            return pd.Series(0.5, index=df.index)
        pcts = df["trainer_win_pct"].fillna(df["trainer_win_pct"].median())
        max_pct = pcts.max()
        if max_pct == 0:
            return pd.Series(0.5, index=df.index)
        base = pcts / max_pct

        if "days_off" in df.columns:
            layoff_mask = df["days_off"] > FORM_CYCLE["layoff_threshold"]
            base = base.where(
                ~layoff_mask | (base < 0.6),
                base * 1.10,
            ).clip(upper=1.0)
        return base

    @staticmethod
    def _score_form_cycle(df: pd.DataFrame) -> pd.Series:
        """Map days_off to a fitness curve."""
        def _score(days: float) -> float:
            if days < 7:
                return 1.0 - FORM_CYCLE["too_fresh_penalty"]
            if FORM_CYCLE["peak_min"] <= days <= FORM_CYCLE["peak_max"]:
                return 1.0
            if days > FORM_CYCLE["layoff_threshold"]:
                over = days - FORM_CYCLE["layoff_threshold"]
                return max(0.2, 1.0 - over * FORM_CYCLE["penalty_per_day_over"])
            if days < FORM_CYCLE["peak_min"]:
                return 0.85 + 0.15 * (days - 7) / (FORM_CYCLE["peak_min"] - 7)
            return 1.0 - 0.15 * (days - FORM_CYCLE["peak_max"]) / (
                FORM_CYCLE["layoff_threshold"] - FORM_CYCLE["peak_max"]
            )

        return df["days_off"].apply(_score)

    @staticmethod
    def _score_class(df: pd.DataFrame) -> pd.Series:
        """Use morning line odds as a proxy for class level."""
        inverted = 1.0 / df["morning_line"]
        max_inv = inverted.max()
        if max_inv == 0:
            return pd.Series(0.5, index=df.index)
        return inverted / max_inv

    @staticmethod
    def _score_post(df: pd.DataFrame) -> pd.Series:
        """Apply post-position bias."""
        if "post_position" not in df.columns:
            return pd.Series(0.5, index=df.index)
        bias = df["post_position"].map(DEFAULT_POST_BIAS).fillna(0.97)
        return (bias - bias.min()) / (bias.max() - bias.min() + 1e-9)

    # ---- conversion helpers -----------------------------------------------

    @staticmethod
    def _to_probabilities(ratings: pd.Series) -> pd.Series:
        """Convert raw power ratings to probabilities that sum to 1.0."""
        temperature = 0.15
        exp_ratings = np.exp(ratings / temperature)
        probs = exp_ratings / exp_ratings.sum()
        return probs

    @staticmethod
    def _prob_to_decimal_odds(prob: pd.Series) -> pd.Series:
        return (1.0 / prob).round(2)

    @staticmethod
    def _decimal_to_american(decimal_odds: float) -> str:
        if decimal_odds >= 2.0:
            american = round((decimal_odds - 1) * 100)
            return f"+{american}"
        else:
            american = round(-100 / (decimal_odds - 1))
            return str(american)

    @staticmethod
    def _ml_to_implied_prob(morning_line: float) -> float:
        return 1.0 / morning_line


# ---------------------------------------------------------------------------
# Module-level convenience wrapper (imported by app.py)
# ---------------------------------------------------------------------------

def calculate_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Run the full NexusModel and return df with model_fair_odds column."""
    model = NexusModel(df)
    result = model.run()
    result = result.rename(columns={"fair_odds_decimal": "model_fair_odds"})
    return result


# ---------------------------------------------------------------------------
# EdgeDetector
# ---------------------------------------------------------------------------

class EdgeDetector:
    """Classify betting value and compute optimal bet sizing."""

    STRONG_VALUE_THRESHOLD = 25.0
    VALUE_THRESHOLD = 10.0
    FAIR_THRESHOLD = 0.0

    @staticmethod
    def classify_bet(edge_score: float) -> str:
        if edge_score > EdgeDetector.STRONG_VALUE_THRESHOLD:
            return "STRONG VALUE"
        if edge_score > EdgeDetector.VALUE_THRESHOLD:
            return "VALUE"
        if edge_score > EdgeDetector.FAIR_THRESHOLD:
            return "FAIR"
        return "AVOID"

    @staticmethod
    def kelly_fraction(
        prob: float, decimal_odds: float, bankroll: float,
        fraction: float = 0.25,
    ) -> float:
        b = decimal_odds - 1.0
        q = 1.0 - prob
        if b <= 0:
            return 0.0
        kelly = (b * prob - q) / b
        kelly = max(kelly, 0.0)
        return round(kelly * fraction * bankroll, 2)

    @staticmethod
    def confidence_score(row: pd.Series) -> int:
        score = 50

        optional_cols = [
            "jockey_win_pct", "trainer_win_pct",
            "surface_win_pct", "post_position",
        ]
        present = sum(1 for c in optional_cols if c in row.index and pd.notna(row.get(c)))
        score += present * 5

        edge = abs(row.get("edge_score", 0))
        if edge > 30:
            score += 20
        elif edge > 15:
            score += 10
        elif edge > 5:
            score += 5

        speed = row.get("last_speed", 0)
        if speed >= 90:
            score += 10
        elif speed < 75:
            score -= 10

        return int(np.clip(score, 0, 100))


# ---------------------------------------------------------------------------
# Backtest stub
# ---------------------------------------------------------------------------

@dataclass
class BacktestResult:
    """Container for backtest output metrics."""
    total_bets: int
    winners: int
    win_rate: float
    total_wagered: float
    total_returned: float
    roi: float
    avg_edge_captured: float


def backtest_stub(
    historical: pd.DataFrame,
    bankroll: float = 10_000.0,
    edge_threshold: float = 10.0,
) -> BacktestResult:
    """Run a backtest over historical results."""
    detector = EdgeDetector()

    bets = historical[historical["edge_score"] >= edge_threshold].copy()
    if bets.empty:
        return BacktestResult(
            total_bets=0, winners=0, win_rate=0.0,
            total_wagered=0.0, total_returned=0.0,
            roi=0.0, avg_edge_captured=0.0,
        )

    total_wagered = 0.0
    total_returned = 0.0
    winners = 0
    edges_captured = []

    for _, row in bets.iterrows():
        bet_size = detector.kelly_fraction(
            prob=row["win_prob"],
            decimal_odds=row["fair_odds_decimal"],
            bankroll=bankroll,
        )
        if bet_size <= 0:
            continue

        total_wagered += bet_size
        won = row.get("actual_finish", 0) == 1

        if won:
            payout = bet_size * row["fair_odds_decimal"]
            total_returned += payout
            winners += 1
            edges_captured.append(row["edge_score"])
            bankroll += payout - bet_size
        else:
            bankroll -= bet_size
            edges_captured.append(0.0)

    total_bets = len(bets)
    win_rate = winners / total_bets if total_bets else 0.0
    roi = ((total_returned - total_wagered) / total_wagered * 100) if total_wagered else 0.0
    avg_edge = float(np.mean(edges_captured)) if edges_captured else 0.0

    return BacktestResult(
        total_bets=total_bets,
        winners=winners,
        win_rate=round(win_rate, 4),
        total_wagered=round(total_wagered, 2),
        total_returned=round(total_returned, 2),
        roi=round(roi, 2),
        avg_edge_captured=round(avg_edge, 2),
    )


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_data = pd.DataFrame([
        {"name": "Thunder Bolt",   "jockey": "I. Ortiz",     "trainer": "T. Pletcher", "last_speed": 98,  "days_off": 14, "morning_line": 3.5,  "jockey_win_pct": 0.22, "trainer_win_pct": 0.20, "surface_win_pct": 0.18},
        {"name": "Shadow Dancer",  "jockey": "J. Rosario",   "trainer": "C. Brown",    "last_speed": 102, "days_off": 21, "morning_line": 2.8,  "jockey_win_pct": 0.25, "trainer_win_pct": 0.23, "surface_win_pct": 0.20},
        {"name": "Longshot Lou",   "jockey": "K. Carmouche", "trainer": "L. Rice",     "last_speed": 82,  "days_off": 60, "morning_line": 15.0, "jockey_win_pct": 0.10, "trainer_win_pct": 0.08, "surface_win_pct": 0.07},
        {"name": "Midnight Echo",  "jockey": "F. Prat",      "trainer": "B. Cox",      "last_speed": 95,  "days_off": 28, "morning_line": 4.5,  "jockey_win_pct": 0.21, "trainer_win_pct": 0.19, "surface_win_pct": 0.17},
        {"name": "Speed Demon",    "jockey": "L. Saez",      "trainer": "W. Ward",     "last_speed": 88,  "days_off": 7,  "morning_line": 8.0,  "jockey_win_pct": 0.18, "trainer_win_pct": 0.14, "surface_win_pct": 0.12},
        {"name": "Ghost Run",      "jockey": "M. Smith",     "trainer": "B. Baffert",  "last_speed": 91,  "days_off": 35, "morning_line": 6.0,  "jockey_win_pct": 0.19, "trainer_win_pct": 0.17, "surface_win_pct": 0.15},
    ])

    print("=" * 70)
    print("NEXUS MODEL — SMOKE TEST")
    print("=" * 70)

    # --- Core model ---
    model = NexusModel(test_data)
    results = model.run()

    prob_sum = results["win_prob"].sum()
    assert abs(prob_sum - 1.0) < 1e-6, f"Probabilities sum to {prob_sum}, expected 1.0"
    print(f"\n✓ Probabilities sum to {prob_sum:.6f}")

    display_cols = [
        "name", "win_prob", "model_fair_odds" if "model_fair_odds" in results.columns else "fair_odds_decimal",
        "fair_odds_american", "market_prob", "edge_score", "pace_type", "pace_scenario",
    ]
    display_cols = [c for c in display_cols if c in results.columns]
    print("\n" + results[display_cols].to_string(index=False))

    # --- Pace scenario ---
    scenario = results["pace_scenario"].iloc[0]
    print(f"\n✓ Pace scenario: {scenario}")
    assert "pace_type" in results.columns, "pace_type column missing"
    assert "pace_scenario" in results.columns, "pace_scenario column missing"

    # --- calculate_odds wrapper ---
    co_result = calculate_odds(test_data)
    assert "model_fair_odds" in co_result.columns, "calculate_odds: model_fair_odds column missing"
    print("✓ calculate_odds() returned model_fair_odds column")

    # --- Class change detection ---
    cc = detect_class_changes(test_data)
    assert len(cc) == len(test_data), "Class change series length mismatch"
    print(f"✓ detect_class_changes(): scores = {cc.tolist()}")

    # --- JT combo ---
    jt = score_jt_combo(test_data)
    assert len(jt) == len(test_data), "JT combo series length mismatch"
    print(f"✓ score_jt_combo(): boosts = {jt.tolist()}")

    # --- Confidence intervals ---
    for wp, n in [(0.30, 6), (0.10, 12), (0.50, 4)]:
        low, high = confidence_interval(wp, n)
        assert 0 <= low <= wp <= high <= 1, f"CI out of bounds: ({low}, {high}) for p={wp}"
        print(f"✓ confidence_interval({wp}, n={n}) → ({low}, {high})")

    # --- Edge detection ---
    detector = EdgeDetector()
    print("\n--- Edge Classifications ---")
    for _, row in results.iterrows():
        classification = detector.classify_bet(row["edge_score"])
        confidence = detector.confidence_score(row)
        fair_odds_col = "model_fair_odds" if "model_fair_odds" in row.index else "fair_odds_decimal"
        kelly = detector.kelly_fraction(
            row["win_prob"], row[fair_odds_col], bankroll=1000.0
        )
        print(
            f"  {row['name']:18s}  pace={row['pace_type']}  edge={row['edge_score']:+6.1f}%  "
            f"{classification:12s}  conf={confidence:3d}  kelly=${kelly:.2f}"
        )

    print("\n✓ All checks passed — model is deterministic, no random noise.")
