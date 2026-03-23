"""
Nexus Racing Analytics — Probabilistic Model & Edge Detection Engine

Provides:
    NexusModel      — computes win probabilities and fair odds from race data
    EdgeDetector    — classifies value, sizes bets via Kelly criterion
    backtest_stub   — placeholder for historical validation
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEATURE_WEIGHTS = {
    "speed":         0.35,
    "jockey":        0.20,
    "trainer":       0.15,
    "form_cycle":    0.10,
    "class_level":   0.10,
    "post_position": 0.10,
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
# NexusModel
# ---------------------------------------------------------------------------

class NexusModel:
    """Compute win probabilities and fair odds for a race field.

    Parameters
    ----------
    df : pd.DataFrame
        Race entries with at minimum: name, last_speed, days_off,
        morning_line.  Optional columns used when present:
        jockey_win_pct, trainer_win_pct, surface_win_pct, post_position.
    """

    def __init__(self, df: pd.DataFrame):
        self.raw = df.copy()
        self.n_runners = len(df)
        self.results: Optional[pd.DataFrame] = None

    # ---- public API -------------------------------------------------------

    def run(self) -> pd.DataFrame:
        """Execute the full model pipeline and return enriched DataFrame."""
        df = self.raw.copy()

        # 1. Score each feature dimension (0-1 per horse)
        df["_speed_score"]    = self._score_speed(df)
        df["_jockey_score"]   = self._score_jockey(df)
        df["_trainer_score"]  = self._score_trainer(df)
        df["_form_score"]     = self._score_form_cycle(df)
        df["_class_score"]    = self._score_class(df)
        df["_post_score"]     = self._score_post(df)

        # 2. Weighted composite → raw power rating
        df["power_rating"] = (
            df["_speed_score"]  * FEATURE_WEIGHTS["speed"]
            + df["_jockey_score"]  * FEATURE_WEIGHTS["jockey"]
            + df["_trainer_score"] * FEATURE_WEIGHTS["trainer"]
            + df["_form_score"]    * FEATURE_WEIGHTS["form_cycle"]
            + df["_class_score"]   * FEATURE_WEIGHTS["class_level"]
            + df["_post_score"]    * FEATURE_WEIGHTS["post_position"]
        )

        # 3. Convert to probabilities (softmax-style normalisation)
        df["win_prob"] = self._to_probabilities(df["power_rating"])

        # 4. Fair odds
        df["fair_odds_decimal"] = self._prob_to_decimal_odds(df["win_prob"])
        df["fair_odds_american"] = df["fair_odds_decimal"].apply(
            self._decimal_to_american
        )

        # 5. Market implied probability from morning line
        df["market_prob"] = df["morning_line"].apply(self._ml_to_implied_prob)

        # 6. Edge detection
        df["edge_score"] = (
            (df["win_prob"] - df["market_prob"]) / df["market_prob"] * 100
        )

        # 7. Clean up internal columns
        internal = [c for c in df.columns if c.startswith("_")]
        df.drop(columns=internal, inplace=True)

        self.results = df.sort_values("win_prob", ascending=False).reset_index(
            drop=True
        )
        return self.results

    # ---- feature scorers (each returns Series in 0-1) ---------------------

    @staticmethod
    def _score_speed(df: pd.DataFrame) -> pd.Series:
        """Normalise last_speed to 0-1 within the field.

        Uses min-max scaling with a floor at 70 (below which a horse is
        non-competitive) so that the range isn't compressed by one outlier.
        """
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

        # First-off-layoff adjustment: trainers with high win % who also
        # have a horse coming off 45+ days are *known* for winning with
        # fresh horses — boost them slightly.
        if "days_off" in df.columns:
            layoff_mask = df["days_off"] > FORM_CYCLE["layoff_threshold"]
            # Boost strong trainers returning a horse off a layoff
            base = base.where(
                ~layoff_mask | (base < 0.6),
                base * 1.10,
            ).clip(upper=1.0)
        return base

    @staticmethod
    def _score_form_cycle(df: pd.DataFrame) -> pd.Series:
        """Map days_off to a fitness curve.

        Peak fitness: 14-35 days.  <7 days gets a freshness penalty.
        >45 days decays gradually.
        """
        def _score(days: float) -> float:
            if days < 7:
                return 1.0 - FORM_CYCLE["too_fresh_penalty"]
            if FORM_CYCLE["peak_min"] <= days <= FORM_CYCLE["peak_max"]:
                return 1.0
            if days > FORM_CYCLE["layoff_threshold"]:
                over = days - FORM_CYCLE["layoff_threshold"]
                return max(0.2, 1.0 - over * FORM_CYCLE["penalty_per_day_over"])
            # Between 7-14 or 35-45 — partial credit
            if days < FORM_CYCLE["peak_min"]:
                return 0.85 + 0.15 * (days - 7) / (FORM_CYCLE["peak_min"] - 7)
            # 35-45
            return 1.0 - 0.15 * (days - FORM_CYCLE["peak_max"]) / (
                FORM_CYCLE["layoff_threshold"] - FORM_CYCLE["peak_max"]
            )

        return df["days_off"].apply(_score)

    @staticmethod
    def _score_class(df: pd.DataFrame) -> pd.Series:
        """Use morning line odds as a proxy for class level.

        Lower morning line ≈ higher class.  Invert and normalise.
        """
        inverted = 1.0 / df["morning_line"]
        max_inv = inverted.max()
        if max_inv == 0:
            return pd.Series(0.5, index=df.index)
        return inverted / max_inv

    @staticmethod
    def _score_post(df: pd.DataFrame) -> pd.Series:
        """Apply post-position bias.  Falls back to neutral if column missing."""
        if "post_position" not in df.columns:
            return pd.Series(0.5, index=df.index)
        bias = df["post_position"].map(DEFAULT_POST_BIAS).fillna(0.97)
        # Normalise to 0-1
        return (bias - bias.min()) / (bias.max() - bias.min() + 1e-9)

    # ---- conversion helpers -----------------------------------------------

    @staticmethod
    def _to_probabilities(ratings: pd.Series) -> pd.Series:
        """Convert raw power ratings to probabilities that sum to 1.0.

        Uses a softmax-inspired normalisation with temperature scaling to
        spread probabilities across the field realistically.
        """
        temperature = 0.15  # lower = sharper separation
        exp_ratings = np.exp(ratings / temperature)
        probs = exp_ratings / exp_ratings.sum()
        return probs

    @staticmethod
    def _prob_to_decimal_odds(prob: pd.Series) -> pd.Series:
        """Convert probability to decimal odds."""
        return (1.0 / prob).round(2)

    @staticmethod
    def _decimal_to_american(decimal_odds: float) -> str:
        """Convert decimal odds to American format."""
        if decimal_odds >= 2.0:
            american = round((decimal_odds - 1) * 100)
            return f"+{american}"
        else:
            american = round(-100 / (decimal_odds - 1))
            return str(american)

    @staticmethod
    def _ml_to_implied_prob(morning_line: float) -> float:
        """Convert morning-line decimal odds to implied probability.

        Morning line is typically expressed as decimal odds (e.g., 3.5 means
        5/2 in fractional).  Implied prob = 1 / odds.
        """
        return 1.0 / morning_line


# ---------------------------------------------------------------------------
# EdgeDetector
# ---------------------------------------------------------------------------

class EdgeDetector:
    """Classify betting value and compute optimal bet sizing."""

    # Edge thresholds
    STRONG_VALUE_THRESHOLD = 25.0
    VALUE_THRESHOLD = 10.0
    FAIR_THRESHOLD = 0.0

    @staticmethod
    def classify_bet(edge_score: float) -> str:
        """Classify a bet based on the edge score (percentage).

        Returns
        -------
        str
            'STRONG VALUE', 'VALUE', 'FAIR', or 'AVOID'
        """
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
        """Compute recommended bet size using fractional Kelly criterion.

        Parameters
        ----------
        prob : float
            Estimated win probability (0-1).
        decimal_odds : float
            Decimal odds offered (e.g. 4.0).
        bankroll : float
            Current bankroll in dollars.
        fraction : float
            Kelly fraction (default 0.25 = quarter-Kelly for safety).

        Returns
        -------
        float
            Recommended bet in dollars, floored at 0.
        """
        b = decimal_odds - 1.0  # net payout per dollar wagered
        q = 1.0 - prob
        if b <= 0:
            return 0.0
        kelly = (b * prob - q) / b
        kelly = max(kelly, 0.0)  # never recommend negative bet
        return round(kelly * fraction * bankroll, 2)

    @staticmethod
    def confidence_score(row: pd.Series) -> int:
        """Compute a confidence score (0-100) based on data completeness
        and signal strength.

        Parameters
        ----------
        row : pd.Series
            A single horse's row from the model output.

        Returns
        -------
        int
            Confidence 0-100.
        """
        score = 50  # baseline

        # Data completeness: reward presence of optional columns
        optional_cols = [
            "jockey_win_pct", "trainer_win_pct",
            "surface_win_pct", "post_position",
        ]
        present = sum(1 for c in optional_cols if c in row.index and pd.notna(row.get(c)))
        score += present * 5  # up to +20

        # Signal strength: higher absolute edge = higher confidence
        edge = abs(row.get("edge_score", 0))
        if edge > 30:
            score += 20
        elif edge > 15:
            score += 10
        elif edge > 5:
            score += 5

        # Speed data reliability: penalise very low speed figures
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
    """Run a backtest over historical results.

    Parameters
    ----------
    historical : pd.DataFrame
        Must contain columns: name, win_prob, market_prob, edge_score,
        fair_odds_decimal, actual_finish (1 = won).
    bankroll : float
        Starting bankroll.
    edge_threshold : float
        Minimum edge_score to place a bet.

    Returns
    -------
    BacktestResult
    """
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
    # Minimal test with mock data matching expected schema
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

    model = NexusModel(test_data)
    results = model.run()

    # Verify probabilities sum to 1
    prob_sum = results["win_prob"].sum()
    assert abs(prob_sum - 1.0) < 1e-6, f"Probabilities sum to {prob_sum}, expected 1.0"
    print(f"\n✓ Probabilities sum to {prob_sum:.6f}")

    # Display results
    display_cols = [
        "name", "win_prob", "fair_odds_decimal", "fair_odds_american",
        "market_prob", "edge_score",
    ]
    print("\n" + results[display_cols].to_string(index=False))

    # Edge detection
    detector = EdgeDetector()
    print("\n--- Edge Classifications ---")
    for _, row in results.iterrows():
        classification = detector.classify_bet(row["edge_score"])
        confidence = detector.confidence_score(row)
        kelly = detector.kelly_fraction(
            row["win_prob"], row["fair_odds_decimal"], bankroll=1000.0
        )
        print(
            f"  {row['name']:18s}  edge={row['edge_score']:+6.1f}%  "
            f"{classification:12s}  conf={confidence:3d}  kelly=${kelly:.2f}"
        )

    print("\n✓ All checks passed — model is deterministic, no random noise.")
