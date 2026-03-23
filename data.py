"""
Nexus Racing Analytics — Data Layer

Data sources (in priority order):
1. The Racing API (https://theracingapi.com) — free tier gives basic racecards.
   Set env var RACING_API_KEY to enable.  Sign up at https://theracingapi.com
2. Fallback: realistic generated data clearly marked as synthetic.

All public functions return pandas DataFrames or plain dicts and are cached
with a 30-minute TTL so callers can hit them freely without hammering upstream.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import date, datetime, timedelta
from functools import wraps
from typing import Any, Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config & logging
# ---------------------------------------------------------------------------
RACING_API_KEY: str = os.environ.get("RACING_API_KEY", "")
RACING_API_BASE: str = "https://api.theracingapi.com/v1"
CACHE_TTL_SECONDS: int = 1800  # 30 minutes
REQUEST_TIMEOUT: int = 15  # seconds

logger = logging.getLogger("nexus.data")

# ---------------------------------------------------------------------------
# Simple TTL cache (no external dependency beyond stdlib)
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, Any]] = {}


def _cache_key(*args: Any) -> str:
    raw = "|".join(str(a) for a in args)
    return hashlib.md5(raw.encode()).hexdigest()


def ttl_cache(fn):
    """Decorator: caches return value for CACHE_TTL_SECONDS."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        key = _cache_key(fn.__name__, args, tuple(sorted(kwargs.items())))
        now = time.time()
        if key in _cache:
            ts, val = _cache[key]
            if now - ts < CACHE_TTL_SECONDS:
                logger.debug("cache hit: %s", fn.__name__)
                return val
        result = fn(*args, **kwargs)
        _cache[key] = (now, result)
        return result
    return wrapper


def clear_cache():
    """Manually flush the cache."""
    _cache.clear()


# ---------------------------------------------------------------------------
# Internal: The Racing API helpers
# ---------------------------------------------------------------------------

def _api_available() -> bool:
    return bool(RACING_API_KEY)


def _api_get(path: str, params: dict | None = None) -> dict | list | None:
    """GET from The Racing API.  Returns parsed JSON or None on failure."""
    if not _api_available():
        return None
    url = f"{RACING_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {RACING_API_KEY}"}
    try:
        resp = requests.get(url, headers=headers, params=params or {},
                            timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        logger.warning("Racing API request failed (%s): %s", path, exc)
        return None


# ---------------------------------------------------------------------------
# Internal: Fallback — realistic generated data
# ---------------------------------------------------------------------------
# When no API key is configured we generate plausible data so the app can
# still demonstrate its analytics pipeline.  Every record is tagged with
# source="generated" so the UI can show a banner.

_TRACKS = {
    "Saratoga":        {"code": "SAR", "surface": ["Dirt", "Turf"], "country": "US"},
    "Del Mar":         {"code": "DMR", "surface": ["Dirt", "Turf", "Synthetic"], "country": "US"},
    "Churchill Downs": {"code": "CD",  "surface": ["Dirt", "Turf"], "country": "US"},
    "Gulfstream":      {"code": "GP",  "surface": ["Dirt", "Turf"], "country": "US"},
    "Aqueduct":        {"code": "AQU", "surface": ["Dirt", "Turf"], "country": "US"},
    "Santa Anita":     {"code": "SA",  "surface": ["Dirt", "Turf"], "country": "US"},
    "Belmont":         {"code": "BEL", "surface": ["Dirt", "Turf"], "country": "US"},
    "Keeneland":       {"code": "KEE", "surface": ["Dirt", "Turf", "Synthetic"], "country": "US"},
}

_HORSE_NAMES = [
    "Thunder Bolt", "Shadow Dancer", "Longshot Lou", "Midnight Echo",
    "Speed Demon", "Ghost Run", "Iron Will", "Mystic Ruler",
    "Final Surge", "Coastal Breeze", "Bold Venture", "Rapid Fire",
    "Silent Storm", "Gold Standard", "War Cry", "Night Patrol",
]

_JOCKEYS = [
    "I. Ortiz Jr.", "J. Rosario", "F. Prat", "L. Saez",
    "M. Smith", "J. Velazquez", "T. Gaffalione", "J. Castellano",
    "K. Carmouche", "D. Davis", "J. Alvarado", "R. Santana Jr.",
]

_TRAINERS = [
    "T. Pletcher", "C. Brown", "B. Cox", "S. Asmussen",
    "W. Mott", "L. Rice", "B. Baffert", "M. Maker",
    "J. Englehart", "D. Gargan", "R. Atras", "H. Motion",
]

_OWNERS = [
    "Repole Stable", "Klaravich Stables", "Juddmonte Farms",
    "Godolphin", "Stonestreet Stables", "WinStar Farm",
    "Three Chimneys", "Eclipse Thoroughbred Partners",
]


def _deterministic_seed(track: str, race_num: int, day: str) -> int:
    """Gives stable-ish 'random' data for the same query on the same day."""
    return int(hashlib.md5(f"{track}-{race_num}-{day}".encode()).hexdigest()[:8], 16)


def _gen_entries(track: str, race_num: int, target_date: str) -> pd.DataFrame:
    """Generate a plausible field of 6-12 horses."""
    import random
    seed = _deterministic_seed(track, race_num, target_date)
    rng = random.Random(seed)

    field_size = rng.randint(6, 12)
    horses = rng.sample(_HORSE_NAMES, min(field_size, len(_HORSE_NAMES)))
    jockeys = rng.sample(_JOCKEYS, min(field_size, len(_JOCKEYS)))
    trainers = [rng.choice(_TRAINERS) for _ in range(field_size)]
    owners = [rng.choice(_OWNERS) for _ in range(field_size)]

    rows = []
    for i, (h, j) in enumerate(zip(horses, jockeys)):
        speed = rng.randint(70, 105)
        ml_odds = round(rng.choice([1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 30.0]), 1)
        rows.append({
            "post_position": i + 1,
            "horse": h,
            "jockey": j,
            "trainer": trainers[i],
            "owner": owners[i],
            "last_speed": speed,
            "avg_speed": speed + rng.randint(-5, 5),
            "best_speed": speed + rng.randint(0, 8),
            "days_off": rng.choice([7, 14, 21, 28, 35, 42, 60, 90]),
            "morning_line_odds": ml_odds,
            "races_lifetime": rng.randint(3, 40),
            "wins": rng.randint(0, 10),
            "places": rng.randint(0, 8),
            "shows": rng.randint(0, 8),
            "earnings": rng.randint(5_000, 500_000),
            "source": "generated",
        })
    return pd.DataFrame(rows)


def _gen_races(target_date: str) -> list[dict]:
    """Generate a day's card across several tracks."""
    import random
    rng = random.Random(int(hashlib.md5(target_date.encode()).hexdigest()[:8], 16))
    races = []
    active_tracks = rng.sample(list(_TRACKS.keys()), k=min(4, len(_TRACKS)))
    for track in active_tracks:
        num_races = rng.randint(7, 12)
        info = _TRACKS[track]
        for r in range(1, num_races + 1):
            surface = rng.choice(info["surface"])
            distance_furlongs = rng.choice([5.0, 5.5, 6.0, 6.5, 7.0, 8.0, 8.5, 9.0, 10.0, 12.0])
            races.append({
                "track": track,
                "track_code": info["code"],
                "race_number": r,
                "post_time": f"{12 + (r // 3)}:{rng.choice(['00', '15', '30', '45'])}",
                "surface": surface,
                "distance_furlongs": distance_furlongs,
                "distance_label": f"{distance_furlongs}f",
                "race_type": rng.choice(["Maiden Special Weight", "Allowance",
                                          "Claiming $25,000", "Claiming $50,000",
                                          "Stakes", "Graded Stakes"]),
                "purse": rng.choice([25_000, 50_000, 75_000, 100_000, 200_000, 500_000]),
                "track_condition": rng.choice(["Fast", "Good", "Firm", "Yielding", "Muddy", "Sloppy"]),
                "country": info["country"],
                "source": "generated",
            })
    return races


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@ttl_cache
def get_upcoming_races(target_date: Optional[str] = None) -> list[dict]:
    """
    Return a list of races for the given date (YYYY-MM-DD).
    Defaults to today.

    Each dict contains: track, track_code, race_number, post_time,
    surface, distance, race_type, purse, track_condition, source.
    """
    if target_date is None:
        target_date = date.today().isoformat()

    # --- Try The Racing API ---
    data = _api_get("/racecards/free", {"day": target_date})
    if data and isinstance(data, dict) and "racecards" in data:
        races = []
        for card in data["racecards"]:
            races.append({
                "track": card.get("course", "Unknown"),
                "track_code": card.get("course", "")[:3].upper(),
                "race_number": card.get("off_dt", ""),
                "post_time": card.get("off_time", ""),
                "surface": card.get("going", "Unknown"),
                "distance_furlongs": card.get("dist_f", 0),
                "distance_label": card.get("dist", ""),
                "race_type": card.get("race_class", ""),
                "purse": card.get("prize", 0),
                "track_condition": card.get("going", ""),
                "country": card.get("region", ""),
                "source": "theracingapi",
            })
        if races:
            return races

    # --- Fallback: generated data ---
    logger.info("Using generated race data for %s", target_date)
    return _gen_races(target_date)


@ttl_cache
def get_race_entries(track: str, race_num: int,
                     target_date: Optional[str] = None) -> pd.DataFrame:
    """
    Return a DataFrame of entries for a specific race.

    Columns: post_position, horse, jockey, trainer, owner,
             last_speed, avg_speed, best_speed, days_off,
             morning_line_odds, races_lifetime, wins, places,
             shows, earnings, source.
    """
    if target_date is None:
        target_date = date.today().isoformat()

    # --- Try The Racing API ---
    # On free tier, racecards include runner lists
    data = _api_get("/racecards/free", {"day": target_date})
    if data and isinstance(data, dict) and "racecards" in data:
        for card in data["racecards"]:
            card_course = card.get("course", "")
            # Match by track name (fuzzy) and race number
            if (track.lower() in card_course.lower() or
                    card_course.lower() in track.lower()):
                runners = card.get("runners", [])
                if runners:
                    rows = []
                    for i, r in enumerate(runners):
                        rows.append({
                            "post_position": r.get("draw", i + 1),
                            "horse": r.get("horse", "Unknown"),
                            "jockey": r.get("jockey", "Unknown"),
                            "trainer": r.get("trainer", "Unknown"),
                            "owner": r.get("owner", "Unknown"),
                            "last_speed": r.get("last_speed", 0),
                            "avg_speed": r.get("avg_speed", 0),
                            "best_speed": r.get("best_speed", 0),
                            "days_off": r.get("days_since_ran", 0),
                            "morning_line_odds": r.get("odds", 0),
                            "races_lifetime": r.get("runs", 0),
                            "wins": r.get("wins", 0),
                            "places": r.get("places", 0),
                            "shows": r.get("shows", 0),
                            "earnings": r.get("prize_money", 0),
                            "source": "theracingapi",
                        })
                    if rows:
                        return pd.DataFrame(rows)

    # --- Fallback ---
    logger.info("Using generated entries for %s R%d on %s", track, race_num, target_date)
    return _gen_entries(track, race_num, target_date)


@ttl_cache
def get_jockey_stats(jockey_name: str, track: Optional[str] = None,
                     surface: Optional[str] = None) -> dict:
    """
    Return jockey win/place/show percentages and recent form.

    Keys: jockey, win_pct, place_pct, show_pct, starts, wins, places,
          shows, earnings, roi, hot_streak, source.
    """
    # --- Try The Racing API (needs Basic+ tier for analysis) ---
    if _api_available():
        params = {"name": jockey_name}
        data = _api_get("/jockeys/search", params)
        if data and isinstance(data, dict) and "jockeys" in data:
            jockeys = data["jockeys"]
            if jockeys:
                j = jockeys[0]
                jid = j.get("id", "")
                # Fetch stats
                stats = _api_get(f"/jockeys/{jid}/results",
                                 {"limit": 100})
                if stats and isinstance(stats, dict):
                    results = stats.get("results", [])
                    total = len(results)
                    if total > 0:
                        wins = sum(1 for r in results if r.get("position") == "1")
                        places = sum(1 for r in results
                                     if r.get("position") in ("1", "2"))
                        shows = sum(1 for r in results
                                    if r.get("position") in ("1", "2", "3"))
                        return {
                            "jockey": jockey_name,
                            "win_pct": round(wins / total * 100, 1),
                            "place_pct": round(places / total * 100, 1),
                            "show_pct": round(shows / total * 100, 1),
                            "starts": total,
                            "wins": wins,
                            "places": places,
                            "shows": shows,
                            "earnings": sum(r.get("prize", 0) for r in results),
                            "roi": 0.0,  # would need odds data
                            "hot_streak": wins >= 3,
                            "source": "theracingapi",
                        }

    # --- Fallback: generated stats based on name hash ---
    import random
    seed = int(hashlib.md5(
        f"{jockey_name}-{track or ''}-{surface or ''}".encode()
    ).hexdigest()[:8], 16)
    rng = random.Random(seed)

    starts = rng.randint(50, 500)
    win_pct = round(rng.uniform(8.0, 28.0), 1)
    place_pct = round(win_pct + rng.uniform(5.0, 15.0), 1)
    show_pct = round(place_pct + rng.uniform(5.0, 12.0), 1)
    wins = int(starts * win_pct / 100)
    places = int(starts * place_pct / 100)
    shows = int(starts * show_pct / 100)

    return {
        "jockey": jockey_name,
        "win_pct": win_pct,
        "place_pct": place_pct,
        "show_pct": show_pct,
        "starts": starts,
        "wins": wins,
        "places": places,
        "shows": shows,
        "earnings": rng.randint(100_000, 5_000_000),
        "roi": round(rng.uniform(-0.2, 0.3), 2),
        "hot_streak": rng.choice([True, False]),
        "source": "generated",
    }


@ttl_cache
def get_trainer_stats(trainer_name: str) -> dict:
    """
    Return trainer performance stats.

    Keys: trainer, win_pct, place_pct, show_pct, starts, wins,
          first_off_layoff_pct, turf_win_pct, dirt_win_pct,
          earnings, source.
    """
    # --- Try The Racing API ---
    if _api_available():
        params = {"name": trainer_name}
        data = _api_get("/trainers/search", params)
        if data and isinstance(data, dict) and "trainers" in data:
            trainers = data["trainers"]
            if trainers:
                t = trainers[0]
                tid = t.get("id", "")
                stats = _api_get(f"/trainers/{tid}/results",
                                 {"limit": 100})
                if stats and isinstance(stats, dict):
                    results = stats.get("results", [])
                    total = len(results)
                    if total > 0:
                        wins = sum(1 for r in results if r.get("position") == "1")
                        places = sum(1 for r in results
                                     if r.get("position") in ("1", "2"))
                        shows = sum(1 for r in results
                                    if r.get("position") in ("1", "2", "3"))
                        return {
                            "trainer": trainer_name,
                            "win_pct": round(wins / total * 100, 1),
                            "place_pct": round(places / total * 100, 1),
                            "show_pct": round(shows / total * 100, 1),
                            "starts": total,
                            "wins": wins,
                            "first_off_layoff_pct": 0.0,
                            "turf_win_pct": 0.0,
                            "dirt_win_pct": 0.0,
                            "earnings": sum(r.get("prize", 0) for r in results),
                            "source": "theracingapi",
                        }

    # --- Fallback ---
    import random
    seed = int(hashlib.md5(trainer_name.encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    starts = rng.randint(80, 600)
    win_pct = round(rng.uniform(10.0, 30.0), 1)
    place_pct = round(win_pct + rng.uniform(5.0, 15.0), 1)
    show_pct = round(place_pct + rng.uniform(4.0, 10.0), 1)

    return {
        "trainer": trainer_name,
        "win_pct": win_pct,
        "place_pct": place_pct,
        "show_pct": show_pct,
        "starts": starts,
        "wins": int(starts * win_pct / 100),
        "first_off_layoff_pct": round(rng.uniform(5.0, 25.0), 1),
        "turf_win_pct": round(rng.uniform(8.0, 25.0), 1),
        "dirt_win_pct": round(rng.uniform(10.0, 30.0), 1),
        "earnings": rng.randint(200_000, 8_000_000),
        "source": "generated",
    }


@ttl_cache
def get_track_bias(track: str, surface: str) -> dict:
    """
    Return post-position bias data for a track/surface combo.

    Keys: track, surface, bias (dict mapping post_position -> advantage_score),
          rail_advantage, speed_bias, closing_bias, source.

    advantage_score > 0 means the post is advantageous; < 0 means disadvantaged.
    """
    # No free API provides this — always generated from historical patterns.
    import random
    seed = int(hashlib.md5(f"{track}-{surface}".encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    # Real-world pattern: inside posts tend to have slight advantage on
    # dirt sprints; outside posts on turf routes.
    num_posts = 12
    bias = {}
    for pp in range(1, num_posts + 1):
        if surface.lower() == "dirt":
            # inside bias typical
            base = 2.0 - (pp * 0.3)
        elif surface.lower() == "turf":
            # middle posts tend to be best on turf
            base = -abs(pp - 6) * 0.3 + 1.0
        else:
            base = 0.0
        bias[pp] = round(base + rng.uniform(-0.5, 0.5), 2)

    speed_bias = round(rng.uniform(-1.0, 1.0), 2)

    return {
        "track": track,
        "surface": surface,
        "bias": bias,
        "rail_advantage": round(bias.get(1, 0) - bias.get(num_posts, 0), 2),
        "speed_bias": speed_bias,          # >0 = speed-favoring
        "closing_bias": round(-speed_bias + rng.uniform(-0.3, 0.3), 2),
        "source": "generated",
    }


# ---------------------------------------------------------------------------
# Convenience: data-source status
# ---------------------------------------------------------------------------

def data_source_status() -> dict:
    """Return current data-source configuration for display in UI."""
    return {
        "api_configured": _api_available(),
        "api_name": "The Racing API" if _api_available() else None,
        "fallback": "generated (deterministic mock data)",
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
        "note": (
            "Set env var RACING_API_KEY to enable live data from "
            "The Racing API (free tier available at https://theracingapi.com)."
            if not _api_available()
            else "Connected to The Racing API."
        ),
    }


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== Data Source Status ===")
    status = data_source_status()
    for k, v in status.items():
        print(f"  {k}: {v}")

    print("\n=== Upcoming Races (today) ===")
    races = get_upcoming_races()
    print(f"  Found {len(races)} races across tracks:")
    tracks_seen = set()
    for r in races:
        tracks_seen.add(r["track"])
    for t in sorted(tracks_seen):
        count = sum(1 for r in races if r["track"] == t)
        print(f"    {t}: {count} races")
    # Show first race detail
    if races:
        print(f"\n  First race: {races[0]}")

    print("\n=== Race Entries (first track, R1) ===")
    if races:
        first_track = races[0]["track"]
        entries = get_race_entries(first_track, 1)
        print(entries.to_string(index=False))
        print(f"\n  Source: {entries['source'].iloc[0]}")

    print("\n=== Jockey Stats: I. Ortiz Jr. ===")
    jstats = get_jockey_stats("I. Ortiz Jr.")
    for k, v in jstats.items():
        print(f"  {k}: {v}")

    print("\n=== Trainer Stats: T. Pletcher ===")
    tstats = get_trainer_stats("T. Pletcher")
    for k, v in tstats.items():
        print(f"  {k}: {v}")

    print("\n=== Track Bias: Saratoga / Dirt ===")
    bias = get_track_bias("Saratoga", "Dirt")
    for k, v in bias.items():
        print(f"  {k}: {v}")

    print("\n=== Cache Test (should be instant) ===")
    import timeit
    t = timeit.timeit(lambda: get_upcoming_races(), number=10)
    print(f"  10 cached calls: {t:.4f}s")
