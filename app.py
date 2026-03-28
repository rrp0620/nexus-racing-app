import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import pytz

# ======================================================================
# PAGE CONFIGURATION
# ======================================================================

st.set_page_config(
    page_title="Nexus Racing Analytics",
    page_icon="🏇",
    layout="wide",
    initial_sidebar_state="auto",
)

# ======================================================================
# CSS — Bloomberg Terminal × Premium Fintech
# ======================================================================

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  /* ── Base ── */
  html, body, .stApp {
    background-color: #080C14 !important;
    color: #F0F0F0;
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
  }
  * { font-variant-numeric: tabular-nums; }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] {
    background-color: #0A0E1A !important;
    border-right: 1px solid #1C2235;
  }
  section[data-testid="stSidebar"] > div { padding-top: 1.25rem; }

  /* ── Typography ── */
  h1, h2, h3, h4 {
    font-family: 'Inter', system-ui, sans-serif !important;
    color: #F0F0F0 !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em;
  }
  .label-muted {
    color: #8A8FA8;
    font-size: 0.65rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  /* ── Tabs ── */
  .stTabs [data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 1px solid #1C2235;
    background: transparent;
  }
  .stTabs [data-baseweb="tab"] {
    background: transparent;
    color: #8A8FA8;
    font-size: 0.8rem;
    font-weight: 500;
    letter-spacing: 0.02em;
    padding: 10px 20px;
    border-radius: 0;
    border-bottom: 2px solid transparent;
  }
  .stTabs [aria-selected="true"] {
    background: transparent !important;
    color: #F0F0F0 !important;
    border-bottom: 2px solid #C9A84C !important;
  }

  /* ── Cards ── */
  .nx-card {
    background: #0E1320;
    border: 1px solid #1C2235;
    border-radius: 6px;
    padding: 20px 24px;
    margin-bottom: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.4);
  }
  .nx-card-top {
    background: #0E1320;
    border: 1px solid #1C2235;
    border-left: 3px solid #C9A84C;
    border-radius: 6px;
    padding: 20px 24px;
    margin-bottom: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.4);
  }
  .nx-card-inset {
    background: #080C14;
    border: 1px solid #1C2235;
    border-radius: 4px;
    padding: 16px 20px;
    box-shadow: 0 1px 2px rgba(0,0,0,0.3);
  }

  /* ── Metrics ── */
  div[data-testid="stMetric"] {
    background: #0E1320;
    border: 1px solid #1C2235;
    border-radius: 6px;
    padding: 20px 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.4);
  }
  div[data-testid="stMetric"] label {
    color: #8A8FA8 !important;
    font-size: 0.65rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
  }
  div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
    color: #C9A84C !important;
    font-weight: 700 !important;
    font-size: 1.5rem !important;
  }
  div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {
    color: #8A8FA8 !important;
    font-size: 0.75rem !important;
  }

  /* ── Badges ── */
  .badge-strong-value {
    display: inline-block;
    background: #0A2818; color: #34D399;
    border: 1px solid #166534;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.06em; text-transform: uppercase;
  }
  .badge-value {
    display: inline-block;
    background: #0F1F0A; color: #86EFAC;
    border: 1px solid #166534;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.06em; text-transform: uppercase;
  }
  .badge-fair {
    display: inline-block;
    background: #1C2235; color: #8A8FA8;
    border: 1px solid #2A3050;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 600;
    letter-spacing: 0.06em; text-transform: uppercase;
  }
  .badge-avoid {
    display: inline-block;
    background: #1C0A0A; color: #F87171;
    border: 1px solid #7F1D1D;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.06em; text-transform: uppercase;
  }
  .badge-live {
    display: inline-block;
    background: #0A1F12; color: #34D399;
    border: 1px solid #166534;
    border-radius: 20px; padding: 3px 12px;
    font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase;
  }
  .badge-demo {
    display: inline-block;
    background: #1A1508; color: #C9A84C;
    border: 1px solid #4A3820;
    border-radius: 20px; padding: 3px 12px;
    font-size: 0.65rem; font-weight: 700;
    letter-spacing: 0.08em; text-transform: uppercase;
  }
  .badge-pace-lone {
    display: inline-block;
    background: #0A2818; color: #34D399;
    border: 1px solid #166534;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.06em;
  }
  .badge-pace-contested {
    display: inline-block;
    background: #1A1508; color: #C9A84C;
    border: 1px solid #4A3820;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.06em;
  }
  .badge-pace-closers {
    display: inline-block;
    background: #0A0F1F; color: #93C5FD;
    border: 1px solid #1E40AF;
    border-radius: 4px; padding: 2px 8px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.06em;
  }

  /* ── Buttons ── */
  .stButton > button {
    background: transparent !important;
    color: #8A8FA8 !important;
    border: 1px solid #1C2235 !important;
    border-radius: 5px !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    padding: 6px 16px !important;
    letter-spacing: 0.02em !important;
    transition: all 0.15s ease !important;
  }
  .stButton > button:hover {
    background: #1C2235 !important;
    color: #F0F0F0 !important;
    border-color: #2A3050 !important;
  }

  /* ── Inputs ── */
  .stTextInput > div > div > input,
  .stNumberInput > div > div > input,
  .stPasswordInput > div > div > input {
    background: #0E1320 !important;
    color: #F0F0F0 !important;
    border: 1px solid #1C2235 !important;
    border-radius: 5px !important;
    font-size: 0.9rem !important;
  }
  .stSelectbox > div > div,
  .stRadio > div {
    background: #0E1320 !important;
    color: #F0F0F0 !important;
    border-color: #1C2235 !important;
  }
  .stRadio label { color: #8A8FA8 !important; font-size: 0.85rem !important; }
  .stRadio [aria-checked="true"] + div { color: #F0F0F0 !important; }

  /* ── Dataframe ── */
  .stDataFrame {
    border: 1px solid #1C2235 !important;
    border-radius: 6px !important;
  }
  .stDataFrame thead th {
    background: #080C14 !important;
    color: #8A8FA8 !important;
    font-size: 0.65rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
  }
  .stDataFrame tbody tr { min-height: 40px !important; }
  .stDataFrame tbody tr:hover { background: #1A2235 !important; }

  /* ── Dividers ── */
  hr { border-color: #1C2235 !important; margin: 20px 0 !important; }

  /* ── Sidebar nav links ── */
  .sidebar-nav-link {
    display: block;
    padding: 8px 12px;
    border-radius: 5px;
    color: #8A8FA8;
    font-size: 0.82rem;
    font-weight: 500;
    text-decoration: none;
    margin-bottom: 2px;
    cursor: pointer;
  }
  .sidebar-nav-link:hover { background: #1C2235; color: #F0F0F0; }

  /* ── Edge progress bar ── */
  .edge-bar-bg {
    background: #1C2235;
    height: 4px;
    border-radius: 2px;
    margin-top: 6px;
    overflow: hidden;
  }
  .edge-bar-fill {
    background: #C9A84C;
    height: 4px;
    border-radius: 2px;
    transition: width 0.3s ease;
  }

  /* ── Play list divider ── */
  .play-divider {
    border: none;
    border-top: 1px solid #1C2235;
    margin: 4px 0 16px 0;
  }

  /* ── Header gold bar ── */
  .gold-bar {
    height: 2px;
    background: #C9A84C;
    opacity: 0.6;
    border-radius: 1px;
    margin-bottom: 24px;
  }

  /* ── Today summary row ── */
  .summary-row {
    background: #0E1320;
    border: 1px solid #1C2235;
    border-radius: 6px;
    padding: 16px 24px;
    display: flex;
    gap: 40px;
    align-items: center;
    margin-bottom: 24px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.4);
  }

  /* ── Login ── */
  .login-wrap {
    max-width: 380px;
    margin: 12vh auto;
    background: #0E1320;
    border: 1px solid #1C2235;
    border-radius: 8px;
    padding: 40px 36px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.5);
  }

  /* ── Hide Streamlit chrome ── */
  #MainMenu, footer, header { visibility: hidden; }
  .viewerBadge_container__1QSob { display: none; }
</style>
""", unsafe_allow_html=True)


# ======================================================================
# HELPERS
# ======================================================================

def et_now() -> datetime:
    return datetime.now(pytz.timezone("America/New_York"))


def fmt_time(dt: datetime) -> str:
    return dt.strftime("%-I:%M %p ET")


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%a, %b %-d %Y")


def badge_html(rec: str) -> str:
    classes = {
        "STRONG VALUE": "badge-strong-value",
        "VALUE":        "badge-value",
        "FAIR":         "badge-fair",
        "AVOID":        "badge-avoid",
    }
    return f'<span class="{classes.get(rec, "badge-fair")}">{rec}</span>'


def edge_bar_html(edge_pct: float) -> str:
    pct = min(100, max(0, (edge_pct / 50) * 100))
    return f"""
    <div class="edge-bar-bg">
      <div class="edge-bar-fill" style="width:{pct:.1f}%;"></div>
    </div>"""


def pace_badge(scenario: str) -> str:
    if scenario == "LONE SPEED":
        return '<span class="badge-pace-lone">🟢 LONE SPEED</span>'
    elif scenario == "CONTESTED":
        return '<span class="badge-pace-contested">🟡 CONTESTED</span>'
    else:
        return '<span class="badge-pace-closers">🔵 CLOSERS RACE</span>'


# ======================================================================
# DATA LAYER
# ======================================================================

DATA_MODE = "DEMO"

# ── Mock helpers ──────────────────────────────────────────────────────

def get_mock_race_data() -> pd.DataFrame:
    horses = [
        {"name": "Thunder Bolt",   "jockey": "I. Ortiz",    "trainer": "T. Pletcher", "post": 3, "last_speed": 98,  "days_off": 14, "morning_line": 3.5},
        {"name": "Shadow Dancer",  "jockey": "J. Rosario",  "trainer": "C. Brown",    "post": 1, "last_speed": 102, "days_off": 21, "morning_line": 2.8},
        {"name": "Longshot Lou",   "jockey": "K. Carmouche","trainer": "L. Rice",     "post": 7, "last_speed": 82,  "days_off": 60, "morning_line": 15.0},
        {"name": "Midnight Echo",  "jockey": "F. Prat",     "trainer": "B. Cox",      "post": 5, "last_speed": 95,  "days_off": 28, "morning_line": 4.5},
        {"name": "Speed Demon",    "jockey": "L. Saez",     "trainer": "W. Ward",     "post": 2, "last_speed": 88,  "days_off": 7,  "morning_line": 8.0},
        {"name": "Ghost Run",      "jockey": "M. Smith",    "trainer": "B. Baffert",  "post": 6, "last_speed": 91,  "days_off": 35, "morning_line": 6.0},
    ]
    return pd.DataFrame(horses)


def get_mock_tracks() -> dict:
    return {
        "Saratoga":        {"races": 10, "surface": ["Dirt", "Turf"],             "status": "Live"},
        "Del Mar":         {"races": 9,  "surface": ["Dirt", "Turf", "Synthetic"],"status": "Live"},
        "Churchill Downs": {"races": 11, "surface": ["Dirt", "Turf"],             "status": "Upcoming"},
        "Gulfstream":      {"races": 12, "surface": ["Dirt", "Turf"],             "status": "Live"},
    }


def get_mock_jockey_stats() -> dict:
    return {
        "I. Ortiz":    {"wins": 234, "win_pct": 24, "roi": 1.12, "hot_streak": True},
        "J. Rosario":  {"wins": 198, "win_pct": 21, "roi": 0.98, "hot_streak": False},
        "K. Carmouche":{"wins": 112, "win_pct": 14, "roi": 1.45, "hot_streak": True},
        "F. Prat":     {"wins": 210, "win_pct": 22, "roi": 1.05, "hot_streak": False},
        "L. Saez":     {"wins": 187, "win_pct": 19, "roi": 0.91, "hot_streak": False},
        "M. Smith":    {"wins": 245, "win_pct": 25, "roi": 0.88, "hot_streak": False},
    }


def get_mock_trainer_stats() -> dict:
    return {
        "T. Pletcher": {"wins": 312, "win_pct": 22, "roi": 0.95, "specialty": "Dirt"},
        "C. Brown":    {"wins": 278, "win_pct": 26, "roi": 1.18, "specialty": "Turf"},
        "L. Rice":     {"wins": 145, "win_pct": 15, "roi": 1.52, "specialty": "Claimers"},
        "B. Cox":      {"wins": 256, "win_pct": 23, "roi": 1.01, "specialty": "2YO"},
        "W. Ward":     {"wins": 134, "win_pct": 16, "roi": 0.87, "specialty": "Turf Sprint"},
        "B. Baffert":  {"wins": 340, "win_pct": 28, "roi": 0.82, "specialty": "Stakes"},
    }


def get_mock_best_plays() -> list:
    """Mock best plays across today's card."""
    return [
        {"horse": "Shadow Dancer",  "track": "Saratoga",        "race": 5,  "time": "3:48 PM", "ml_odds": 3.5,  "nexus_score": 94.2, "edge_pct": 44.6, "recommendation": "STRONG VALUE"},
        {"horse": "Copper King",    "track": "Del Mar",          "race": 3,  "time": "2:15 PM", "ml_odds": 6.0,  "nexus_score": 88.7, "edge_pct": 36.2, "recommendation": "STRONG VALUE"},
        {"horse": "True North",     "track": "Gulfstream",       "race": 7,  "time": "4:30 PM", "ml_odds": 8.5,  "nexus_score": 85.1, "edge_pct": 28.9, "recommendation": "VALUE"},
        {"horse": "Midnight Echo",  "track": "Churchill Downs",  "race": 6,  "time": "5:10 PM", "ml_odds": 4.5,  "nexus_score": 83.4, "edge_pct": 22.7, "recommendation": "VALUE"},
        {"horse": "Iron Curtain",   "track": "Saratoga",         "race": 8,  "time": "5:55 PM", "ml_odds": 12.0, "nexus_score": 79.3, "edge_pct": 18.4, "recommendation": "VALUE"},
    ]


def get_mock_track_bias(track: str, surface: str) -> dict:
    """Post-position win % by track (rough approximations)."""
    biases = {
        ("Saratoga",        "Dirt"):  {1: 17, 2: 16, 3: 14, 4: 13, 5: 12, 6: 10, 7: 9,  8: 6,  9: 2,  10: 1},
        ("Del Mar",         "Dirt"):  {1: 12, 2: 13, 3: 14, 4: 15, 5: 14, 6: 13, 7: 10, 8: 6,  9: 2,  10: 1},
        ("Churchill Downs", "Dirt"):  {1: 11, 2: 12, 3: 13, 4: 14, 5: 14, 6: 12, 7: 11, 8: 7,  9: 4,  10: 2},
        ("Gulfstream",      "Dirt"):  {1: 16, 2: 17, 3: 15, 4: 12, 5: 11, 6: 10, 7: 8,  8: 6,  9: 3,  10: 2},
    }
    key = (track, surface)
    if key in biases:
        return biases[key]
    # Generic fallback — slight rail bias
    return {1: 15, 2: 14, 3: 13, 4: 12, 5: 12, 6: 11, 7: 9, 8: 8, 9: 4, 10: 2}


# ── Live data imports (with fallback) ────────────────────────────────

try:
    from data import get_race_data, get_tracks
    _probe = get_tracks()
    if _probe:
        DATA_MODE = "LIVE"
except Exception:
    get_race_data = None
    get_tracks    = None

try:
    from data import get_todays_best_races as _get_todays_best_races
    GET_BEST_PLAYS_LIVE = True
except Exception:
    GET_BEST_PLAYS_LIVE = False

try:
    from data import get_track_bias as _get_track_bias_live
    GET_TRACK_BIAS_LIVE = True
except Exception:
    GET_TRACK_BIAS_LIVE = False

try:
    from model import calculate_odds as _calculate_odds
    CALCULATE_ODDS = _calculate_odds
except Exception:
    CALCULATE_ODDS = None

try:
    from model import PaceAnalyzer as _PaceAnalyzer
    PACE_ANALYZER = _PaceAnalyzer
except Exception:
    PACE_ANALYZER = None


# ── Public accessors ─────────────────────────────────────────────────

def fetch_best_plays() -> list:
    if GET_BEST_PLAYS_LIVE:
        try:
            raw = _get_todays_best_races()
            if raw:
                return raw
        except Exception:
            pass
    return get_mock_best_plays()


def fetch_track_bias(track: str, surface: str) -> dict:
    if GET_TRACK_BIAS_LIVE:
        try:
            result = _get_track_bias_live(track, surface)
            if result:
                return result
        except Exception:
            pass
    return get_mock_track_bias(track, surface)


# ======================================================================
# NEXUS SCORE ENGINE
# ======================================================================

def calculate_nexus_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["speed_rating"] = (df["last_speed"] / df["last_speed"].max()) * 100
    df["form_penalty"] = df["days_off"].apply(lambda x: 10 if x > 45 else (3 if x > 30 else 0))
    df["fresh_bonus"]  = df["days_off"].apply(lambda x: 5 if 7 <= x <= 21 else 0)
    df["nexus_score"]  = (
        (df["speed_rating"] * 0.7)
        - df["form_penalty"]
        + df["fresh_bonus"]
        + np.random.uniform(0, 5, size=len(df))
    ).round(1)
    df["fair_odds"]    = (100 / df["nexus_score"]).round(2)
    df["edge_pct"]     = (((df["morning_line"] - df["fair_odds"]) / df["fair_odds"]) * 100).round(1)

    def classify(r):
        if r["edge_pct"] > 40:   return "STRONG VALUE"
        elif r["edge_pct"] > 15: return "VALUE"
        elif r["edge_pct"] > -10:return "FAIR"
        else:                    return "AVOID"

    df["recommendation"] = df.apply(classify, axis=1)

    if CALCULATE_ODDS is not None:
        try:
            model_odds = CALCULATE_ODDS(df)
            if model_odds is not None and "model_fair_odds" in model_odds.columns:
                df["fair_odds"] = model_odds["model_fair_odds"]
        except Exception:
            pass

    return df.sort_values("nexus_score", ascending=False).reset_index(drop=True)


# ======================================================================
# PACE ANALYSIS
# ======================================================================

def assign_pace_types(df: pd.DataFrame) -> pd.DataFrame:
    """Assign E/EP/P/S classification per horse.  Uses PaceAnalyzer if available."""
    df = df.copy()

    if PACE_ANALYZER is not None:
        try:
            pa = PACE_ANALYZER()
            result = pa.classify(df)
            if "pace_type" in result.columns:
                df["pace_type"] = result["pace_type"]
                return df
        except Exception:
            pass

    # Fallback heuristic: fast recent + low days off → early speed
    def mock_classify(row):
        if row["last_speed"] >= 96 and row["days_off"] <= 14:
            return "E"
        elif row["last_speed"] >= 90:
            return "EP"
        elif row["last_speed"] >= 85:
            return "P"
        else:
            return "S"

    df["pace_type"] = df.apply(mock_classify, axis=1)
    return df


def get_pace_scenario(df: pd.DataFrame) -> str:
    counts = df["pace_type"].value_counts()
    early  = counts.get("E", 0) + counts.get("EP", 0)
    if early <= 1:
        return "LONE SPEED"
    elif early >= 3:
        return "CONTESTED"
    else:
        return "CLOSERS RACE"


# ======================================================================
# AUTHENTICATION
# ======================================================================

def check_login() -> bool:
    return bool(st.session_state.get("authenticated"))


def show_login():
    st.markdown("<div style='height:6vh'></div>", unsafe_allow_html=True)
    col_l, col_m, col_r = st.columns([1, 1.1, 1])
    with col_m:
        st.markdown("""
        <div style="text-align:center; margin-bottom:32px;">
          <div style="font-size:2.2rem; font-weight:700; color:#F0F0F0; letter-spacing:-0.02em;">
            🏇 NEXUS RACING
          </div>
          <div style="color:#8A8FA8; font-size:0.78rem; letter-spacing:0.14em;
                      text-transform:uppercase; margin-top:6px;">
            The Edge Every Serious Bettor Needs
          </div>
        </div>
        """, unsafe_allow_html=True)

        with st.container():
            st.markdown('<div class="nx-card">', unsafe_allow_html=True)
            username = st.text_input("Username", placeholder="Username", key="login_user", label_visibility="collapsed")
            st.markdown('<div style="margin-bottom:10px"></div>', unsafe_allow_html=True)
            password = st.text_input("Password", type="password", placeholder="Password", key="login_pass", label_visibility="collapsed")
            st.markdown('<div style="margin-bottom:16px"></div>', unsafe_allow_html=True)

            if st.button("Sign In →", use_container_width=True, key="btn_login"):
                if username == "geno" and password == "nexus2026":
                    st.session_state["authenticated"] = True
                    st.session_state["last_refresh"] = et_now()
                    st.rerun()
                else:
                    st.error("Invalid credentials.")
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("""
        <div style="text-align:center; margin-top:32px;">
          <span style="color:#4A4F62; font-size:0.7rem; letter-spacing:0.04em;">
            Nexus Racing Analytics — Proprietary &amp; Confidential
          </span>
        </div>
        """, unsafe_allow_html=True)


# ======================================================================
# MAIN APPLICATION
# ======================================================================

def main():
    now = et_now()

    # Session init
    if "last_refresh" not in st.session_state:
        st.session_state["last_refresh"] = now

    # ── SIDEBAR ────────────────────────────────────────────────────────
    with st.sidebar:
        # Logo
        st.markdown("""
        <div style="padding:4px 0 16px 0;">
          <div style="font-size:1.15rem; font-weight:700; color:#F0F0F0;
                      letter-spacing:-0.01em; line-height:1.2;">
            🏇 NEXUS RACING
          </div>
          <div style="color:#4A4F62; font-size:0.6rem; letter-spacing:0.14em;
                      text-transform:uppercase; margin-top:3px;">
            Analytics Engine
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Data mode badge
        if DATA_MODE == "LIVE":
            st.markdown('<span class="badge-live">● LIVE DATA</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge-demo">● DEMO MODE</span>', unsafe_allow_html=True)

        # Last updated
        last_upd = st.session_state["last_refresh"]
        st.markdown(
            f'<div style="color:#4A4F62; font-size:0.65rem; margin-top:6px; '
            f'letter-spacing:0.04em;">Updated {fmt_time(last_upd)}</div>',
            unsafe_allow_html=True,
        )

        st.markdown('<hr style="margin:14px 0 10px 0;">', unsafe_allow_html=True)

        # Refresh button
        if st.button("↻  Refresh Data", use_container_width=True, key="btn_refresh"):
            st.cache_data.clear()
            st.session_state["last_refresh"] = et_now()
            st.rerun()

        st.markdown('<hr style="margin:10px 0 14px 0;">', unsafe_allow_html=True)

        # Track selection
        st.markdown('<div class="label-muted" style="margin-bottom:6px;">Today\'s Tracks</div>', unsafe_allow_html=True)

        if DATA_MODE == "DEMO":
            tracks = get_mock_tracks()
            track_names = list(tracks.keys())
        else:
            try:
                raw_tracks = get_tracks()
                tracks = {
                    t["name"]: {
                        "code": t.get("code", ""),
                        "status": "Live",
                        "races": 10,
                        "surface": [t.get("surface", "Dirt")],
                    }
                    for t in raw_tracks
                }
                track_names = list(tracks.keys())
            except Exception:
                tracks = get_mock_tracks()
                track_names = list(tracks.keys())

        selected_track = st.selectbox("Track", track_names, label_visibility="collapsed")
        track_info     = tracks[selected_track]

        status_color = "#34D399" if track_info.get("status") == "Live" else "#C9A84C"
        st.markdown(
            f'<span style="color:{status_color}; font-size:0.72rem; font-weight:600;">'
            f'● {track_info.get("status","Live")}</span>',
            unsafe_allow_html=True,
        )

        st.markdown('<div style="margin:12px 0 6px 0;" class="label-muted">Race</div>', unsafe_allow_html=True)
        race_num = st.selectbox(
            "Race",
            list(range(1, track_info.get("races", 10) + 1)),
            format_func=lambda x: f"Race {x}",
            label_visibility="collapsed",
        )

        surfaces = track_info.get("surface", ["Dirt"])
        if isinstance(surfaces, str):
            surfaces = [surfaces]
        st.markdown('<div style="margin:12px 0 6px 0;" class="label-muted">Surface</div>', unsafe_allow_html=True)
        surface = st.radio("Surface", surfaces, label_visibility="collapsed")

        st.markdown('<hr style="margin:14px 0 12px 0;">', unsafe_allow_html=True)

        # Sign out
        if st.button("Sign Out", use_container_width=True, key="btn_signout"):
            st.session_state["authenticated"] = False
            st.rerun()

        st.markdown(
            '<div style="position:fixed; bottom:12px; left:0; right:0; text-align:center;">'
            '<span style="color:#4A4F62; font-size:0.62rem; letter-spacing:0.04em;">v2.1 — Nexus Racing</span>'
            '</div>',
            unsafe_allow_html=True,
        )

    # ── HEADER ─────────────────────────────────────────────────────────
    st.markdown('<div class="gold-bar"></div>', unsafe_allow_html=True)

    hcol1, hcol2 = st.columns([3, 1])
    with hcol1:
        st.markdown(
            f'<div style="font-size:1.6rem; font-weight:700; color:#F0F0F0; '
            f'letter-spacing:-0.02em; line-height:1.1;">🏇 NEXUS RACING</div>'
            f'<div style="color:#8A8FA8; font-size:0.78rem; margin-top:4px; '
            f'letter-spacing:0.04em;">The Edge Every Serious Bettor Needs</div>',
            unsafe_allow_html=True,
        )
    with hcol2:
        st.markdown(
            f'<div style="text-align:right; padding-top:6px;">'
            f'<div style="color:#F0F0F0; font-size:0.9rem; font-weight:600;">'
            f'{fmt_time(now)}</div>'
            f'<div style="color:#8A8FA8; font-size:0.72rem; margin-top:2px;">'
            f'{fmt_date(now)}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)

    # ── LOAD & ANALYZE ─────────────────────────────────────────────────
    if DATA_MODE == "LIVE" and get_race_data is not None:
        try:
            raw_data = get_race_data(selected_track, race_num)
            if raw_data is None or raw_data.empty:
                raw_data = get_mock_race_data()
        except Exception:
            raw_data = get_mock_race_data()
    else:
        raw_data = get_mock_race_data()

    analyzed     = calculate_nexus_score(raw_data)
    analyzed     = assign_pace_types(analyzed)
    top          = analyzed.iloc[0]
    value_horses = analyzed[analyzed["recommendation"].isin(["STRONG VALUE", "VALUE"])]
    best_value   = value_horses.iloc[0] if not value_horses.empty else top

    # ── TOP METRICS ────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Top Pick",        top["name"],                f"Score {top['nexus_score']}")
    m2.metric("Best Value Play", best_value["name"],         f"+{best_value['edge_pct']}% Edge")
    m3.metric("Value Plays",     f"{len(value_horses)}",     f"of {len(analyzed)} runners")
    m4.metric("System Confidence",
              f"{min(95, int(60 + top['nexus_score'] * 0.35))}%",
              f"Field {len(analyzed)}")

    st.markdown('<div style="height:28px"></div>', unsafe_allow_html=True)

    # ── TABS ───────────────────────────────────────────────────────────
    tab_best, tab_card, tab_value, tab_spotlight, tab_kelly = st.tabs([
        "🎯  Best Plays Today",
        "📊  Race Card",
        "💰  Value Bets",
        "👤  Jockey / Trainer",
        "🔢  Kelly Calculator",
    ])

    # ==================================================================
    # TAB 1 — BEST PLAYS TODAY
    # ==================================================================
    with tab_best:
        plays = fetch_best_plays()

        # Summary row
        total_plays  = len(plays)
        avg_edge     = round(sum(p["edge_pct"] for p in plays) / total_plays, 1) if plays else 0
        best_play    = max(plays, key=lambda p: p["edge_pct"]) if plays else {}

        st.markdown(f"""
        <div style="background:#0E1320; border:1px solid #1C2235; border-radius:6px;
                    padding:20px 28px; margin-bottom:28px; box-shadow:0 1px 3px rgba(0,0,0,0.4);">
          <div class="label-muted" style="margin-bottom:12px;">Today's Edge Report</div>
          <div style="display:flex; gap:48px; align-items:flex-end; flex-wrap:wrap;">
            <div>
              <div style="color:#C9A84C; font-size:2rem; font-weight:700; line-height:1;">
                {total_plays}
              </div>
              <div style="color:#8A8FA8; font-size:0.72rem; margin-top:4px; letter-spacing:0.04em;">
                VALUE PLAYS FOUND
              </div>
            </div>
            <div>
              <div style="color:#C9A84C; font-size:2rem; font-weight:700; line-height:1;">
                +{avg_edge}%
              </div>
              <div style="color:#8A8FA8; font-size:0.72rem; margin-top:4px; letter-spacing:0.04em;">
                AVERAGE EDGE
              </div>
            </div>
            <div>
              <div style="color:#F0F0F0; font-size:1.1rem; font-weight:600; line-height:1.3;">
                {best_play.get('horse','—')}
              </div>
              <div style="color:#8A8FA8; font-size:0.72rem; margin-top:4px; letter-spacing:0.04em;">
                BEST SINGLE PLAY &nbsp;
                <span style="color:#C9A84C;">+{best_play.get('edge_pct', 0)}%</span>
              </div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Play cards
        for i, play in enumerate(plays):
            is_top   = i == 0
            card_cls = "nx-card-top" if is_top else "nx-card"
            edge_bar = edge_bar_html(play["edge_pct"])
            badge    = badge_html(play["recommendation"])

            st.markdown(f"""
            <div class="{card_cls}">
              <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                <div style="flex:1;">
                  <div style="display:flex; align-items:center; gap:12px; margin-bottom:6px;">
                    <span style="font-size:1.1rem; font-weight:700; color:#F0F0F0;">
                      {play['horse']}
                    </span>
                    {badge}
                  </div>
                  <div style="color:#8A8FA8; font-size:0.78rem; margin-bottom:10px;">
                    {play['track']} &nbsp;·&nbsp; Race {play['race']}
                    &nbsp;·&nbsp; {play['time']}
                    &nbsp;·&nbsp; ML {play['ml_odds']}
                  </div>
                  <div style="display:flex; align-items:center; gap:16px;">
                    <div>
                      <span class="label-muted">Nexus Score</span><br>
                      <span style="color:#C9A84C; font-size:1rem; font-weight:700;">
                        {play['nexus_score']}
                      </span>
                    </div>
                    <div style="flex:1; max-width:200px;">
                      <div style="display:flex; justify-content:space-between;">
                        <span class="label-muted">Edge</span>
                        <span style="color:#C9A84C; font-size:0.72rem; font-weight:700;">
                          +{play['edge_pct']}%
                        </span>
                      </div>
                      {edge_bar}
                    </div>
                  </div>
                </div>
                <div style="text-align:right; padding-left:24px;">
                  <div class="label-muted" style="margin-bottom:4px;">Nexus Score</div>
                  <div style="color:#C9A84C; font-size:2.2rem; font-weight:700;
                              line-height:1; letter-spacing:-0.02em;">
                    {play['nexus_score']}
                  </div>
                </div>
              </div>
            </div>
            """, unsafe_allow_html=True)

    # ==================================================================
    # TAB 2 — RACE CARD
    # ==================================================================
    with tab_card:
        # Section header
        st.markdown(
            f'<div style="font-size:1.1rem; font-weight:600; color:#F0F0F0; '
            f'margin-bottom:16px;">{selected_track} &mdash; Race {race_num} &mdash; {surface}</div>',
            unsafe_allow_html=True,
        )

        # ── Main table ──
        display_df = analyzed[[
            "name", "jockey", "trainer", "post", "last_speed", "days_off",
            "morning_line", "nexus_score", "fair_odds", "edge_pct", "recommendation"
        ]].copy()
        display_df.columns = [
            "Horse", "Jockey", "Trainer", "Post", "Last Spd", "Days Off",
            "Morn Line", "Nexus Score", "Fair Odds", "Edge %", "Rec"
        ]
        display_df.index = range(1, len(display_df) + 1)
        display_df.index.name = "#"

        def color_rec(val):
            styles = {
                "STRONG VALUE": "background:#0A2818; color:#34D399; font-weight:700;",
                "VALUE":        "background:#0F1F0A; color:#86EFAC; font-weight:600;",
                "FAIR":         "color:#8A8FA8;",
                "AVOID":        "color:#F87171;",
            }
            return styles.get(val, "")

        def color_edge(val):
            try:
                v = float(val)
                if v > 30:   return "color:#C9A84C; font-weight:700;"
                elif v > 10: return "color:#C9A84C;"
                elif v > 0:  return "color:#8A8FA8;"
                else:        return "color:#F87171;"
            except (ValueError, TypeError):
                return ""

        styled = (
            display_df.style
            .map(color_rec,  subset=["Rec"])
            .map(color_edge, subset=["Edge %"])
            .format({
                "Last Spd":   "{:.0f}",
                "Days Off":   "{:.0f}",
                "Post":       "{:.0f}",
                "Morn Line":  "{:.1f}",
                "Nexus Score":"{:.1f}",
                "Fair Odds":  "{:.2f}",
                "Edge %":     "{:+.1f}%",
            })
            .set_table_styles([
                {"selector": "thead th",
                 "props": [("background", "#080C14"), ("color", "#8A8FA8"),
                           ("font-size", "0.65rem"), ("text-transform", "uppercase"),
                           ("letter-spacing", "0.08em"), ("font-weight", "600")]},
                {"selector": "tbody tr:hover",
                 "props": [("background", "#1A2235")]},
                {"selector": "tbody td",
                 "props": [("border-color", "#1C2235")]},
            ])
        )

        st.dataframe(styled, use_container_width=True, height=320)

        # ── Pace Setup ──
        st.markdown('<div style="height:28px"></div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="font-size:0.9rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:12px;">Pace Setup</div>',
            unsafe_allow_html=True,
        )

        scenario = get_pace_scenario(analyzed)

        pace_cols = st.columns([2, 1])
        with pace_cols[0]:
            # Compact pace type table
            pace_data = analyzed[["name", "pace_type"]].copy()
            pace_data.columns = ["Horse", "Pace Type"]
            pace_data.index = range(1, len(pace_data) + 1)

            type_labels = {"E": "Early Speed", "EP": "Early/Presser", "P": "Presser", "S": "Sustained/Closer"}
            pace_data["Pace Type"] = pace_data["Pace Type"].map(type_labels).fillna("Unknown")

            def color_pace_type(val):
                if "Early Speed" in val:   return "color:#F87171; font-weight:600;"
                if "Early/Presser" in val: return "color:#FBBF24; font-weight:600;"
                if "Presser" in val:       return "color:#A3A3A3;"
                return "color:#93C5FD;"

            pace_styled = (
                pace_data.style
                .map(color_pace_type, subset=["Pace Type"])
                .set_table_styles([
                    {"selector": "thead th",
                     "props": [("background", "#080C14"), ("color", "#8A8FA8"),
                               ("font-size", "0.65rem"), ("text-transform", "uppercase"),
                               ("letter-spacing", "0.08em")]},
                    {"selector": "tbody tr:hover",
                     "props": [("background", "#1A2235")]},
                ])
            )
            st.dataframe(pace_styled, use_container_width=True, height=280)

        with pace_cols[1]:
            st.markdown(
                f'<div class="nx-card" style="text-align:center; padding:24px 16px;">'
                f'<div class="label-muted" style="margin-bottom:10px;">Pace Scenario</div>'
                f'<div style="font-size:1.4rem; margin-bottom:10px;">'
                f'{"🟢" if scenario == "LONE SPEED" else "🟡" if scenario == "CONTESTED" else "🔵"}'
                f'</div>'
                f'<div style="font-size:0.9rem; font-weight:700; color:#F0F0F0; margin-bottom:8px;">'
                f'{scenario}</div>'
                f'<div style="color:#8A8FA8; font-size:0.72rem; line-height:1.5;">'
                f'{"Rail-to-wire setup. Front-runner advantage." if scenario == "LONE SPEED" else "Multiple speed sources. Watch for tiring. Closers benefit." if scenario == "CONTESTED" else "Slow fractions expected. Late closers will charge."}'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── Speed vs Value scatter ──
        st.markdown('<div style="height:28px"></div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="font-size:0.9rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:12px;">Speed vs. Value Matrix</div>',
            unsafe_allow_html=True,
        )

        color_map = {
            "STRONG VALUE": "#34D399",
            "VALUE":        "#C9A84C",
            "FAIR":         "#8A8FA8",
            "AVOID":        "#F87171",
        }
        fig_scatter = px.scatter(
            analyzed,
            x="nexus_score",
            y="morning_line",
            size="last_speed",
            color="recommendation",
            hover_name="name",
            text="name",
            color_discrete_map=color_map,
            labels={"nexus_score": "Nexus Score", "morning_line": "Morning Line Odds"},
        )
        fig_scatter.update_traces(textposition="top center", textfont_size=10,
                                  textfont_color="#8A8FA8")
        fig_scatter.update_layout(
            plot_bgcolor="#080C14",
            paper_bgcolor="#0E1320",
            font_color="#8A8FA8",
            font_family="Inter, system-ui, sans-serif",
            legend=dict(font=dict(color="#8A8FA8", size=11), bgcolor="#0E1320",
                        bordercolor="#1C2235", borderwidth=1),
            xaxis=dict(gridcolor="#1C2235", zerolinecolor="#1C2235",
                       title_font=dict(color="#8A8FA8", size=11)),
            yaxis=dict(gridcolor="#1C2235", zerolinecolor="#1C2235",
                       title_font=dict(color="#8A8FA8", size=11)),
            margin=dict(t=20, b=20, l=20, r=20),
            height=380,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # ── Track Bias Panel ──
        st.markdown('<div style="height:28px"></div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="font-size:0.9rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:4px;">Track Bias — Post Position Win %</div>'
            f'<div style="color:#8A8FA8; font-size:0.72rem; margin-bottom:16px;">'
            f'{selected_track} — {surface}</div>',
            unsafe_allow_html=True,
        )

        try:
            bias_data = fetch_track_bias(selected_track, surface)
            # Only show posts that appear in this field
            max_post   = max(analyzed["post"].max(), len(bias_data))
            post_posts = sorted(bias_data.keys())
            post_pcts  = [bias_data[p] for p in post_posts]

            # Highlight posts that horses in this field occupy
            field_posts = set(analyzed["post"].astype(int).tolist())
            bar_colors  = [
                "#C9A84C" if p in field_posts else "#1C2235"
                for p in post_posts
            ]

            fig_bias = go.Figure()
            fig_bias.add_trace(go.Bar(
                x=[str(p) for p in post_posts],
                y=post_pcts,
                marker_color=bar_colors,
                marker_line_width=0,
                hovertemplate="Post %{x}: %{y}% win rate<extra></extra>",
            ))
            # Avg line
            avg_pct = sum(post_pcts) / len(post_pcts)
            fig_bias.add_hline(
                y=avg_pct, line_dash="dot", line_color="#4A4F62", line_width=1,
                annotation_text=f"Avg {avg_pct:.0f}%",
                annotation_font_color="#4A4F62", annotation_font_size=10,
            )
            fig_bias.update_layout(
                plot_bgcolor="#080C14",
                paper_bgcolor="#0E1320",
                font_color="#8A8FA8",
                font_family="Inter, system-ui, sans-serif",
                xaxis=dict(title="Post Position", gridcolor="#1C2235",
                           title_font=dict(color="#8A8FA8", size=11)),
                yaxis=dict(title="Win %", gridcolor="#1C2235",
                           title_font=dict(color="#8A8FA8", size=11)),
                margin=dict(t=10, b=10, l=20, r=20),
                height=240,
                showlegend=False,
            )
            st.plotly_chart(fig_bias, use_container_width=True)

            # Track Bias Alert
            try:
                avg_bias = avg_pct
                top3     = analyzed.head(3)
                alerts   = []
                for _, horse in top3.iterrows():
                    p = int(horse["post"])
                    if p in bias_data:
                        pct = bias_data[p]
                        if pct >= avg_bias * 1.25:
                            alerts.append(
                                f'<span style="color:#34D399; font-weight:700;">✓</span> '
                                f'<b>{horse["name"]}</b> (Post {p}) — '
                                f'<span style="color:#34D399;">favorable post ({pct}% win rate)</span>'
                            )
                        elif pct <= avg_bias * 0.6:
                            alerts.append(
                                f'<span style="color:#F87171; font-weight:700;">⚠</span> '
                                f'<b>{horse["name"]}</b> (Post {p}) — '
                                f'<span style="color:#F87171;">unfavorable post ({pct}% win rate)</span>'
                            )
                if alerts:
                    alert_html = "<br>".join(alerts)
                    st.markdown(
                        f'<div class="nx-card-inset" style="margin-top:12px;">'
                        f'<div class="label-muted" style="margin-bottom:8px;">Track Bias Alert</div>'
                        f'<div style="font-size:0.83rem; color:#F0F0F0; line-height:1.8;">'
                        f'{alert_html}</div></div>',
                        unsafe_allow_html=True,
                    )
            except Exception:
                pass

        except Exception:
            st.markdown(
                '<div class="nx-card-inset"><span style="color:#4A4F62; font-size:0.8rem;">'
                'Track bias data unavailable.</span></div>',
                unsafe_allow_html=True,
            )

    # ==================================================================
    # TAB 3 — VALUE BETS
    # ==================================================================
    with tab_value:
        st.markdown(
            '<div style="font-size:1.1rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:20px;">Value Bets</div>',
            unsafe_allow_html=True,
        )

        if value_horses.empty:
            st.markdown(
                '<div class="nx-card-inset"><span style="color:#8A8FA8; font-size:0.85rem;">'
                'No value plays detected in this field.</span></div>',
                unsafe_allow_html=True,
            )
        else:
            for i, (_, horse) in enumerate(value_horses.iterrows()):
                is_top   = i == 0
                card_cls = "nx-card-top" if is_top else "nx-card"
                badge    = badge_html(horse["recommendation"])
                edge_color = "#34D399" if horse["recommendation"] == "STRONG VALUE" else "#C9A84C"

                st.markdown(f"""
                <div class="{card_cls}">
                  <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                    <div>
                      <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
                        <span style="font-size:1.1rem; font-weight:700; color:#F0F0F0;">
                          {horse['name']}
                        </span>
                        {badge}
                      </div>
                      <div style="display:flex; gap:32px; color:#8A8FA8; font-size:0.8rem;
                                  flex-wrap:wrap; margin-top:4px;">
                        <div>Jockey <span style="color:#F0F0F0;">{horse['jockey']}</span></div>
                        <div>Trainer <span style="color:#F0F0F0;">{horse['trainer']}</span></div>
                        <div>ML <span style="color:#F0F0F0;">{horse['morning_line']:.1f}</span></div>
                        <div>Fair <span style="color:#F0F0F0;">{horse['fair_odds']:.2f}</span></div>
                      </div>
                    </div>
                    <div style="text-align:right; padding-left:32px; flex-shrink:0;">
                      <div style="color:{edge_color}; font-size:1.8rem; font-weight:700;
                                  line-height:1; letter-spacing:-0.02em;">
                        +{horse['edge_pct']:.1f}%
                      </div>
                      <div style="color:#8A8FA8; font-size:0.65rem; font-weight:600;
                                  letter-spacing:0.08em; text-transform:uppercase; margin-top:3px;">
                        Edge
                      </div>
                    </div>
                  </div>
                  <div style="margin-top:14px;">
                    <div style="display:flex; justify-content:space-between;
                                margin-bottom:4px; color:#8A8FA8; font-size:0.72rem;">
                      <span>Edge vs Market</span>
                      <span style="color:{edge_color};">+{horse['edge_pct']:.1f}% of 50% scale</span>
                    </div>
                    {edge_bar_html(horse['edge_pct'])}
                  </div>
                </div>
                """, unsafe_allow_html=True)

    # ==================================================================
    # TAB 4 — JOCKEY / TRAINER
    # ==================================================================
    with tab_spotlight:
        jockey_stats  = get_mock_jockey_stats()
        trainer_stats = get_mock_trainer_stats()

        st.markdown(
            '<div style="font-size:0.9rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:16px;">Jockey Spotlight — Top 3 Picks</div>',
            unsafe_allow_html=True,
        )
        jock_cols = st.columns(min(3, len(analyzed)))
        for i, (_, horse) in enumerate(analyzed.head(3).iterrows()):
            jname = horse["jockey"]
            stats = jockey_stats.get(jname, {"wins": 0, "win_pct": 0, "roi": 0, "hot_streak": False})
            roi_color = "#34D399" if stats["roi"] > 1 else "#F87171"
            hot_badge = (
                '<span style="color:#F97316; font-size:0.65rem; font-weight:700; '
                'letter-spacing:0.06em; margin-left:8px;">HOT</span>'
                if stats["hot_streak"] else ""
            )
            with jock_cols[i]:
                st.markdown(f"""
                <div class="nx-card">
                  <div style="font-size:0.95rem; font-weight:700; color:#F0F0F0;
                              margin-bottom:4px;">{jname}{hot_badge}</div>
                  <div style="color:#8A8FA8; font-size:0.75rem; margin-bottom:14px;">
                    Mount: <span style="color:#C9A84C;">{horse['name']}</span>
                  </div>
                  <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">Season Wins</span>
                      <span style="color:#F0F0F0; font-size:0.85rem; font-weight:600;">
                        {stats['wins']}
                      </span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">Win %</span>
                      <span style="color:#F0F0F0; font-size:0.85rem; font-weight:600;">
                        {stats['win_pct']}%
                      </span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">ROI</span>
                      <span style="color:{roi_color}; font-size:0.85rem; font-weight:600;">
                        {stats['roi']:.2f}
                      </span>
                    </div>
                  </div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown('<div style="height:28px"></div>', unsafe_allow_html=True)
        st.markdown(
            '<div style="font-size:0.9rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:16px;">Trainer Spotlight — Top 3 Picks</div>',
            unsafe_allow_html=True,
        )
        train_cols = st.columns(min(3, len(analyzed)))
        for i, (_, horse) in enumerate(analyzed.head(3).iterrows()):
            tname = horse["trainer"]
            stats = trainer_stats.get(tname, {"wins": 0, "win_pct": 0, "roi": 0, "specialty": "N/A"})
            roi_color = "#34D399" if stats["roi"] > 1 else "#F87171"
            with train_cols[i]:
                st.markdown(f"""
                <div class="nx-card">
                  <div style="font-size:0.95rem; font-weight:700; color:#F0F0F0;
                              margin-bottom:4px;">{tname}</div>
                  <div style="color:#8A8FA8; font-size:0.75rem; margin-bottom:14px;">
                    Horse: <span style="color:#C9A84C;">{horse['name']}</span>
                  </div>
                  <div style="display:flex; flex-direction:column; gap:8px;">
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">Season Wins</span>
                      <span style="color:#F0F0F0; font-size:0.85rem; font-weight:600;">
                        {stats['wins']}
                      </span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">Win %</span>
                      <span style="color:#F0F0F0; font-size:0.85rem; font-weight:600;">
                        {stats['win_pct']}%
                      </span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">Specialty</span>
                      <span style="color:#F0F0F0; font-size:0.85rem; font-weight:600;">
                        {stats['specialty']}
                      </span>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                      <span class="label-muted">ROI</span>
                      <span style="color:{roi_color}; font-size:0.85rem; font-weight:600;">
                        {stats['roi']:.2f}
                      </span>
                    </div>
                  </div>
                </div>
                """, unsafe_allow_html=True)

    # ==================================================================
    # TAB 5 — KELLY CALCULATOR
    # ==================================================================
    with tab_kelly:
        st.markdown(
            '<div style="font-size:1.1rem; font-weight:600; color:#F0F0F0; '
            'margin-bottom:4px;">Kelly Criterion Calculator</div>'
            '<div style="color:#8A8FA8; font-size:0.78rem; margin-bottom:24px;">'
            'Optimal bet sizing using Fractional Kelly (25%) for conservative risk management.</div>',
            unsafe_allow_html=True,
        )

        kcol1, kcol2 = st.columns([1, 2])
        with kcol1:
            bankroll = st.number_input(
                "Bankroll ($)", min_value=100, max_value=1_000_000,
                value=1000, step=100,
            )
        with kcol2:
            st.markdown(
                '<div class="nx-card-inset" style="margin-top:28px;">'
                '<span class="label-muted">Strategy</span><br>'
                '<span style="color:#F0F0F0; font-size:0.85rem;">Quarter-Kelly (25% of full Kelly). '
                'Balances growth rate with drawdown protection.</span>'
                '</div>',
                unsafe_allow_html=True,
            )

        st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)

        KELLY_FRACTION = 0.25

        if value_horses.empty:
            st.markdown(
                '<div class="nx-card-inset"><span style="color:#8A8FA8; font-size:0.85rem;">'
                'No value plays — nothing to size.</span></div>',
                unsafe_allow_html=True,
            )
        else:
            total_exposure = 0.0

            for i, (_, horse) in enumerate(value_horses.iterrows()):
                fair_prob  = 1.0 / horse["fair_odds"]
                b          = horse["morning_line"] - 1.0
                p          = fair_prob
                q          = 1.0 - p
                full_kelly = ((b * p) - q) / b if b > 0 else 0.0
                adj_kelly  = max(0.0, full_kelly * KELLY_FRACTION)
                bet_amount = round(bankroll * adj_kelly, 2)
                total_exposure += bet_amount

                is_strong  = horse["recommendation"] == "STRONG VALUE"
                card_cls   = "nx-card-top" if i == 0 else "nx-card"
                badge      = badge_html(horse["recommendation"])

                st.markdown(f"""
                <div class="{card_cls}">
                  <div style="display:flex; justify-content:space-between; align-items:flex-start;">
                    <div>
                      <div style="display:flex; align-items:center; gap:12px; margin-bottom:6px;">
                        <span style="font-size:1rem; font-weight:700; color:#F0F0F0;">
                          {horse['name']}
                        </span>
                        {badge}
                      </div>
                      <div style="color:#8A8FA8; font-size:0.78rem; margin-bottom:12px;">
                        ML {horse['morning_line']:.1f}
                        &nbsp;→&nbsp; Fair {horse['fair_odds']:.2f}
                        &nbsp;·&nbsp; Edge +{horse['edge_pct']:.1f}%
                      </div>
                      <div style="display:flex; gap:28px; flex-wrap:wrap;">
                        <div>
                          <div class="label-muted">Full Kelly</div>
                          <div style="color:#8A8FA8; font-size:0.85rem; font-weight:600;">
                            {full_kelly*100:.1f}%
                          </div>
                        </div>
                        <div>
                          <div class="label-muted">25% Kelly</div>
                          <div style="color:#F0F0F0; font-size:0.85rem; font-weight:600;">
                            {adj_kelly*100:.1f}% of bankroll
                          </div>
                        </div>
                      </div>
                    </div>
                    <div style="text-align:right; padding-left:32px; flex-shrink:0;">
                      <div style="color:#C9A84C; font-size:2.2rem; font-weight:700;
                                  line-height:1; letter-spacing:-0.02em;">
                        ${bet_amount:,.2f}
                      </div>
                      <div style="color:#8A8FA8; font-size:0.65rem; font-weight:600;
                                  letter-spacing:0.08em; text-transform:uppercase; margin-top:3px;">
                        Recommended Bet
                      </div>
                    </div>
                  </div>
                </div>
                """, unsafe_allow_html=True)

            # Totals
            st.markdown(f"""
            <div style="margin-top:16px; padding:18px 24px; background:#0E1320;
                        border:1px solid #1C2235; border-radius:6px;
                        display:flex; justify-content:space-between; align-items:center;
                        box-shadow:0 1px 3px rgba(0,0,0,0.4);">
              <div>
                <div class="label-muted">Total Exposure</div>
                <div style="color:#8A8FA8; font-size:0.75rem; margin-top:4px;">
                  {len(value_horses)} plays &nbsp;·&nbsp;
                  {total_exposure/bankroll*100:.1f}% of bankroll
                </div>
              </div>
              <div style="text-align:right;">
                <div style="color:#C9A84C; font-size:1.8rem; font-weight:700;
                            letter-spacing:-0.02em;">${total_exposure:,.2f}</div>
              </div>
            </div>
            """, unsafe_allow_html=True)


# ======================================================================
# ENTRY POINT
# ======================================================================

if not check_login():
    show_login()
else:
    main()
