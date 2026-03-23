import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Nexus Racing Analytics",
    page_icon="🏇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    /* Dark navy background */
    .stApp {
        background-color: #0a0e1a;
        color: #e8e8e8;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #0d1224;
        border-right: 1px solid #1a2040;
    }

    /* Headers gold accent */
    h1, h2, h3 {
        color: #b8965a !important;
        font-family: 'Helvetica Neue', sans-serif;
        letter-spacing: 0.5px;
    }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background-color: #111833;
        border: 1px solid #1a2040;
        border-radius: 8px;
        padding: 16px;
    }
    div[data-testid="stMetric"] label {
        color: #8890a4 !important;
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #b8965a !important;
        font-weight: 700;
    }

    /* Dataframe styling */
    .stDataFrame {
        border: 1px solid #1a2040;
        border-radius: 8px;
    }

    /* Buttons */
    .stButton > button {
        background-color: #b8965a !important;
        color: #0a0e1a !important;
        font-weight: 700;
        border: none;
        border-radius: 6px;
        padding: 0.5rem 2rem;
        letter-spacing: 0.5px;
    }
    .stButton > button:hover {
        background-color: #d4ae6a !important;
        color: #0a0e1a !important;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid #1a2040;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        color: #8890a4;
        border-radius: 6px 6px 0 0;
        padding: 8px 20px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #111833 !important;
        color: #b8965a !important;
        border-bottom: 2px solid #b8965a;
    }

    /* Login screen */
    .login-container {
        max-width: 400px;
        margin: 10vh auto;
        padding: 40px;
        background: #111833;
        border: 1px solid #1a2040;
        border-radius: 12px;
        text-align: center;
    }
    .login-container h1 {
        font-size: 2rem;
        margin-bottom: 0.25rem;
    }
    .login-tagline {
        color: #8890a4;
        font-size: 0.9rem;
        margin-bottom: 2rem;
    }

    /* Mode banner */
    .mode-banner {
        display: inline-block;
        padding: 4px 14px;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
    }
    .mode-live {
        background-color: #0d3320;
        color: #34d399;
        border: 1px solid #166534;
    }
    .mode-demo {
        background-color: #3b2508;
        color: #b8965a;
        border: 1px solid #b8965a;
    }

    /* Value bet cards */
    .value-card {
        background: linear-gradient(135deg, #111833 0%, #0d1224 100%);
        border: 1px solid #b8965a;
        border-radius: 10px;
        padding: 20px;
        margin: 8px 0;
    }
    .value-card-strong {
        border-color: #34d399;
        box-shadow: 0 0 15px rgba(52, 211, 153, 0.1);
    }

    /* Spotlight cards */
    .spotlight-card {
        background-color: #111833;
        border: 1px solid #1a2040;
        border-radius: 10px;
        padding: 16px;
    }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Input fields */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input {
        background-color: #111833 !important;
        color: #e8e8e8 !important;
        border: 1px solid #1a2040 !important;
        border-radius: 6px;
    }

    /* Select boxes */
    .stSelectbox > div > div {
        background-color: #111833 !important;
        color: #e8e8e8 !important;
    }

    /* Divider */
    hr {
        border-color: #1a2040;
    }
</style>
""", unsafe_allow_html=True)


# ======================================================================
# DATA LAYER — try live imports, fall back to mock
# ======================================================================

DATA_MODE = "DEMO"

def get_mock_race_data():
    """Original mock data preserved as fallback."""
    horses = [
        {"name": "Thunder Bolt", "jockey": "I. Ortiz", "trainer": "T. Pletcher", "last_speed": 98, "days_off": 14, "morning_line": 3.5},
        {"name": "Shadow Dancer", "jockey": "J. Rosario", "trainer": "C. Brown", "last_speed": 102, "days_off": 21, "morning_line": 2.8},
        {"name": "Longshot Lou", "jockey": "K. Carmouche", "trainer": "L. Rice", "last_speed": 82, "days_off": 60, "morning_line": 15.0},
        {"name": "Midnight Echo", "jockey": "F. Prat", "trainer": "B. Cox", "last_speed": 95, "days_off": 28, "morning_line": 4.5},
        {"name": "Speed Demon", "jockey": "L. Saez", "trainer": "W. Ward", "last_speed": 88, "days_off": 7, "morning_line": 8.0},
        {"name": "Ghost Run", "jockey": "M. Smith", "trainer": "B. Baffert", "last_speed": 91, "days_off": 35, "morning_line": 6.0},
    ]
    return pd.DataFrame(horses)


def get_mock_tracks():
    """Mock track/race schedule."""
    return {
        "Saratoga": {"races": 10, "surface": ["Dirt", "Turf"], "status": "Live"},
        "Del Mar": {"races": 9, "surface": ["Dirt", "Turf", "Synthetic"], "status": "Live"},
        "Churchill Downs": {"races": 11, "surface": ["Dirt", "Turf"], "status": "Upcoming"},
        "Gulfstream": {"races": 12, "surface": ["Dirt", "Turf"], "status": "Live"},
    }


def get_mock_jockey_stats():
    """Mock jockey statistics."""
    return {
        "I. Ortiz": {"wins": 234, "win_pct": 24, "roi": 1.12, "hot_streak": True},
        "J. Rosario": {"wins": 198, "win_pct": 21, "roi": 0.98, "hot_streak": False},
        "K. Carmouche": {"wins": 112, "win_pct": 14, "roi": 1.45, "hot_streak": True},
        "F. Prat": {"wins": 210, "win_pct": 22, "roi": 1.05, "hot_streak": False},
        "L. Saez": {"wins": 187, "win_pct": 19, "roi": 0.91, "hot_streak": False},
        "M. Smith": {"wins": 245, "win_pct": 25, "roi": 0.88, "hot_streak": False},
    }


def get_mock_trainer_stats():
    """Mock trainer statistics."""
    return {
        "T. Pletcher": {"wins": 312, "win_pct": 22, "roi": 0.95, "specialty": "Dirt"},
        "C. Brown": {"wins": 278, "win_pct": 26, "roi": 1.18, "specialty": "Turf"},
        "L. Rice": {"wins": 145, "win_pct": 15, "roi": 1.52, "specialty": "Claimers"},
        "B. Cox": {"wins": 256, "win_pct": 23, "roi": 1.01, "specialty": "2YO"},
        "W. Ward": {"wins": 134, "win_pct": 16, "roi": 0.87, "specialty": "Turf Sprint"},
        "B. Baffert": {"wins": 340, "win_pct": 28, "roi": 0.82, "specialty": "Stakes"},
    }


# Attempt live data import
try:
    from data import get_race_data, get_tracks
    _test = get_tracks()
    if _test:
        DATA_MODE = "LIVE"
except Exception:
    pass

# Attempt model import
try:
    from model import calculate_odds
except Exception:
    calculate_odds = None


# ======================================================================
# CORE ALGORITHM — THE NEXUS SCORE
# ======================================================================

def calculate_nexus_score(df):
    """Score each horse and generate fair odds + recommendations."""
    df = df.copy()

    # Normalize speed (0-100 relative to field)
    df['speed_rating'] = (df['last_speed'] / df['last_speed'].max()) * 100

    # Form cycle penalty
    df['form_penalty'] = df['days_off'].apply(lambda x: 10 if x > 45 else (3 if x > 30 else 0))

    # Freshness bonus
    df['fresh_bonus'] = df['days_off'].apply(lambda x: 5 if 7 <= x <= 21 else 0)

    # Nexus Score
    df['nexus_score'] = (
        (df['speed_rating'] * 0.7)
        - df['form_penalty']
        + df['fresh_bonus']
        + np.random.uniform(0, 5, size=len(df))
    ).round(1)

    # Fair odds from score
    df['fair_odds'] = (100 / df['nexus_score']).round(1)

    # Edge percentage
    df['edge_pct'] = (((df['morning_line'] - df['fair_odds']) / df['fair_odds']) * 100).round(1)

    # Recommendation tiers
    def classify(row):
        if row['edge_pct'] > 40:
            return "STRONG VALUE"
        elif row['edge_pct'] > 15:
            return "VALUE"
        elif row['edge_pct'] > -10:
            return "FAIR"
        else:
            return "AVOID"
    df['recommendation'] = df.apply(classify, axis=1)

    # If a model engine is available, overlay its odds
    if calculate_odds is not None:
        try:
            model_odds = calculate_odds(df)
            if model_odds is not None and 'model_fair_odds' in model_odds.columns:
                df['fair_odds'] = model_odds['model_fair_odds']
        except Exception:
            pass

    return df.sort_values(by='nexus_score', ascending=False).reset_index(drop=True)


# ======================================================================
# AUTHENTICATION
# ======================================================================

def check_login():
    """Simple login gate."""
    if st.session_state.get("authenticated"):
        return True
    return False


def show_login():
    """Render the login screen."""
    st.markdown("<div style='height: 8vh'></div>", unsafe_allow_html=True)

    col_l, col_m, col_r = st.columns([1, 1.2, 1])
    with col_m:
        st.markdown("""
        <div style="text-align:center; margin-bottom: 30px;">
            <h1 style="font-size: 2.5rem; margin-bottom: 0; color: #b8965a !important;">
                🏇 NEXUS RACING
            </h1>
            <p style="color: #8890a4; font-size: 1rem; letter-spacing: 2px; text-transform: uppercase;">
                Algorithmic Edge. Every Race.
            </p>
        </div>
        """, unsafe_allow_html=True)

        with st.container():
            st.markdown("""
            <div style="background: #111833; border: 1px solid #1a2040; border-radius: 12px; padding: 30px; margin-top: 10px;">
            """, unsafe_allow_html=True)

            username = st.text_input("Username", placeholder="Enter username", key="login_user")
            password = st.text_input("Password", type="password", placeholder="Enter password", key="login_pass")

            if st.button("Sign In", use_container_width=True):
                if username == "geno" and password == "nexus2026":
                    st.session_state["authenticated"] = True
                    st.rerun()
                else:
                    st.error("Invalid credentials.")

            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("""
        <div style="text-align:center; margin-top: 40px;">
            <p style="color: #333d56; font-size: 0.75rem;">Nexus Racing Analytics v2.0 — Proprietary & Confidential</p>
        </div>
        """, unsafe_allow_html=True)


# ======================================================================
# MAIN APPLICATION
# ======================================================================

def main():
    # --- Sidebar ---
    with st.sidebar:
        st.markdown("""
        <div style="text-align:center; padding: 10px 0 5px 0;">
            <span style="font-size: 1.6rem; font-weight: 700; color: #b8965a; letter-spacing: 1px;">
                NEXUS RACING
            </span>
            <br>
            <span style="color: #8890a4; font-size: 0.65rem; letter-spacing: 2px; text-transform: uppercase;">
                Analytics Engine
            </span>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("---")

        # Data mode banner
        if DATA_MODE == "LIVE":
            st.markdown('<span class="mode-banner mode-live">● LIVE DATA</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="mode-banner mode-demo">● DEMO MODE</span>', unsafe_allow_html=True)

        st.markdown("")

        # Track selection
        tracks = get_mock_tracks() if DATA_MODE == "DEMO" else get_tracks()
        track_names = list(tracks.keys())

        st.markdown("##### Today's Tracks")
        selected_track = st.selectbox("Track", track_names, label_visibility="collapsed")
        track_info = tracks[selected_track]

        # Show track status
        status_color = "#34d399" if track_info["status"] == "Live" else "#b8965a"
        st.markdown(
            f'<span style="color:{status_color}; font-size:0.75rem; font-weight:600;">'
            f'● {track_info["status"]}</span>',
            unsafe_allow_html=True,
        )

        # Race selection
        st.markdown("")
        race_num = st.selectbox("Race", list(range(1, track_info["races"] + 1)),
                                format_func=lambda x: f"Race {x}")

        surface = st.radio("Surface", track_info["surface"])

        st.markdown("---")

        # Logout
        if st.button("Sign Out", use_container_width=True):
            st.session_state["authenticated"] = False
            st.rerun()

        st.markdown("""
        <div style="position: fixed; bottom: 10px; padding: 10px;">
            <span style="color: #333d56; font-size: 0.65rem;">v2.0 — Nexus Racing Analytics</span>
        </div>
        """, unsafe_allow_html=True)

    # --- Header ---
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown(f"# {selected_track}")
        st.markdown(
            f'<span style="color:#8890a4;">Race {race_num} &nbsp;|&nbsp; {surface} '
            f'&nbsp;|&nbsp; <span style="color:{status_color};">● {track_info["status"]}</span></span>',
            unsafe_allow_html=True,
        )
    with header_col2:
        st.markdown("<div style='text-align:right; padding-top:20px;'>", unsafe_allow_html=True)
        if DATA_MODE == "LIVE":
            st.markdown('<span class="mode-banner mode-live">● LIVE DATA</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="mode-banner mode-demo">● DEMO MODE</span>', unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")

    # --- Load & Analyze ---
    if DATA_MODE == "LIVE":
        try:
            raw_data = get_race_data(selected_track, race_num)
            if raw_data is None or raw_data.empty:
                raw_data = get_mock_race_data()
        except Exception:
            raw_data = get_mock_race_data()
    else:
        raw_data = get_mock_race_data()

    analyzed = calculate_nexus_score(raw_data)

    # --- Top Metrics ---
    top = analyzed.iloc[0]
    value_horses = analyzed[analyzed['recommendation'].isin(["STRONG VALUE", "VALUE"])]
    best_value = value_horses.iloc[0] if not value_horses.empty else top

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Top Pick", top['name'], f"Score: {top['nexus_score']}")
    m2.metric("Best Value", best_value['name'], f"+{best_value['edge_pct']}% Edge")
    m3.metric("Value Plays", f"{len(value_horses)}", f"of {len(analyzed)} runners")
    m4.metric("System Confidence",
              f"{min(95, int(60 + top['nexus_score'] * 0.35))}%",
              f"Field size: {len(analyzed)}")

    st.markdown("")

    # --- Tabbed Sections ---
    tab_card, tab_value, tab_spotlight, tab_kelly = st.tabs([
        "Race Card", "Value Bets", "Jockey / Trainer", "Kelly Calculator"
    ])

    # ===== RACE CARD =====
    with tab_card:
        st.markdown("### Race Card")
        display_df = analyzed[[
            'name', 'jockey', 'trainer', 'last_speed', 'days_off',
            'morning_line', 'nexus_score', 'fair_odds', 'edge_pct', 'recommendation'
        ]].copy()
        display_df.columns = [
            'Horse', 'Jockey', 'Trainer', 'Last Speed', 'Days Off',
            'Morning Line', 'Nexus Score', 'Fair Odds', 'Edge %', 'Recommendation'
        ]
        display_df.index = range(1, len(display_df) + 1)
        display_df.index.name = '#'

        def color_rec(val):
            if val == "STRONG VALUE":
                return "background-color: #0d3320; color: #34d399; font-weight: 700;"
            elif val == "VALUE":
                return "background-color: #1a2a10; color: #a3e635; font-weight: 600;"
            elif val == "FAIR":
                return "color: #8890a4;"
            else:
                return "color: #ef4444;"

        def color_edge(val):
            try:
                v = float(val)
                if v > 40:
                    return "color: #34d399; font-weight: 700;"
                elif v > 15:
                    return "color: #a3e635;"
                elif v > 0:
                    return "color: #8890a4;"
                else:
                    return "color: #ef4444;"
            except (ValueError, TypeError):
                return ""

        styled = (
            display_df.style
            .map(color_rec, subset=['Recommendation'])
            .map(color_edge, subset=['Edge %'])
            .format({
                'Last Speed': '{:.0f}',
                'Days Off': '{:.0f}',
                'Morning Line': '{:.1f}',
                'Nexus Score': '{:.1f}',
                'Fair Odds': '{:.1f}',
                'Edge %': '{:+.1f}%',
            })
        )

        st.dataframe(styled, use_container_width=True, height=320)

        # Speed vs Value scatter
        st.markdown("### Speed vs. Value Matrix")
        fig = px.scatter(
            analyzed,
            x="nexus_score",
            y="morning_line",
            size="last_speed",
            color="recommendation",
            hover_name="name",
            text="name",
            color_discrete_map={
                "STRONG VALUE": "#34d399",
                "VALUE": "#a3e635",
                "FAIR": "#8890a4",
                "AVOID": "#ef4444",
            },
            labels={"nexus_score": "Nexus Score", "morning_line": "Morning Line Odds"},
        )
        fig.update_traces(textposition='top center', textfont_size=11)
        fig.update_layout(
            plot_bgcolor="#0a0e1a",
            paper_bgcolor="#0a0e1a",
            font_color="#e8e8e8",
            legend=dict(font=dict(color="#8890a4")),
            xaxis=dict(gridcolor="#1a2040", zerolinecolor="#1a2040"),
            yaxis=dict(gridcolor="#1a2040", zerolinecolor="#1a2040"),
            margin=dict(t=30, b=30),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ===== VALUE BETS =====
    with tab_value:
        st.markdown("### Value Bets")
        if value_horses.empty:
            st.info("No value plays detected in this field.")
        else:
            for _, horse in value_horses.iterrows():
                is_strong = horse['recommendation'] == "STRONG VALUE"
                badge_color = "#34d399" if is_strong else "#a3e635"
                card_class = "value-card value-card-strong" if is_strong else "value-card"

                st.markdown(f"""
                <div class="{card_class}">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="font-size:1.3rem; font-weight:700; color:#e8e8e8;">
                                {horse['name']}
                            </span>
                            <span style="background:{badge_color}; color:#0a0e1a; padding:2px 10px;
                                         border-radius:12px; font-size:0.7rem; font-weight:700;
                                         margin-left:12px; text-transform:uppercase;">
                                {horse['recommendation']}
                            </span>
                        </div>
                        <div style="text-align:right;">
                            <span style="color:#b8965a; font-size:1.5rem; font-weight:700;">
                                {horse['nexus_score']}
                            </span>
                            <span style="color:#8890a4; font-size:0.75rem; display:block;">Nexus Score</span>
                        </div>
                    </div>
                    <div style="display:flex; gap:40px; margin-top:14px; color:#8890a4; font-size:0.85rem;">
                        <div>Jockey: <span style="color:#e8e8e8;">{horse['jockey']}</span></div>
                        <div>Trainer: <span style="color:#e8e8e8;">{horse['trainer']}</span></div>
                        <div>ML: <span style="color:#e8e8e8;">{horse['morning_line']:.1f}</span></div>
                        <div>Fair Odds: <span style="color:#e8e8e8;">{horse['fair_odds']:.1f}</span></div>
                        <div>Edge: <span style="color:{badge_color}; font-weight:700;">+{horse['edge_pct']:.1f}%</span></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.markdown("")

    # ===== JOCKEY / TRAINER SPOTLIGHT =====
    with tab_spotlight:
        jockey_stats = get_mock_jockey_stats()
        trainer_stats = get_mock_trainer_stats()

        st.markdown("### Jockey Spotlight")
        jock_cols = st.columns(min(3, len(analyzed)))
        for i, (_, horse) in enumerate(analyzed.head(3).iterrows()):
            jname = horse['jockey']
            stats = jockey_stats.get(jname, {"wins": 0, "win_pct": 0, "roi": 0, "hot_streak": False})
            with jock_cols[i]:
                streak_badge = (
                    '<span style="color:#34d399; font-size:0.7rem; font-weight:700;">🔥 HOT</span>'
                    if stats["hot_streak"] else ""
                )
                st.markdown(f"""
                <div class="spotlight-card">
                    <div style="font-size:1.1rem; font-weight:700; color:#e8e8e8; margin-bottom:8px;">
                        {jname} {streak_badge}
                    </div>
                    <div style="color:#8890a4; font-size:0.8rem; line-height:1.8;">
                        Mount: <span style="color:#b8965a;">{horse['name']}</span><br>
                        Season Wins: <span style="color:#e8e8e8;">{stats['wins']}</span><br>
                        Win %: <span style="color:#e8e8e8;">{stats['win_pct']}%</span><br>
                        ROI: <span style="color:{'#34d399' if stats['roi'] > 1 else '#ef4444'};">
                            {stats['roi']:.2f}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("")
        st.markdown("### Trainer Spotlight")
        train_cols = st.columns(min(3, len(analyzed)))
        for i, (_, horse) in enumerate(analyzed.head(3).iterrows()):
            tname = horse['trainer']
            stats = trainer_stats.get(tname, {"wins": 0, "win_pct": 0, "roi": 0, "specialty": "N/A"})
            with train_cols[i]:
                st.markdown(f"""
                <div class="spotlight-card">
                    <div style="font-size:1.1rem; font-weight:700; color:#e8e8e8; margin-bottom:8px;">
                        {tname}
                    </div>
                    <div style="color:#8890a4; font-size:0.8rem; line-height:1.8;">
                        Horse: <span style="color:#b8965a;">{horse['name']}</span><br>
                        Season Wins: <span style="color:#e8e8e8;">{stats['wins']}</span><br>
                        Win %: <span style="color:#e8e8e8;">{stats['win_pct']}%</span><br>
                        Specialty: <span style="color:#e8e8e8;">{stats['specialty']}</span><br>
                        ROI: <span style="color:{'#34d399' if stats['roi'] > 1 else '#ef4444'};">
                            {stats['roi']:.2f}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    # ===== KELLY CALCULATOR =====
    with tab_kelly:
        st.markdown("### Kelly Criterion Calculator")
        st.markdown(
            '<span style="color:#8890a4; font-size:0.85rem;">'
            'Optimal bet sizing based on edge and bankroll. Uses fractional Kelly (25%) for safety.</span>',
            unsafe_allow_html=True,
        )
        st.markdown("")

        bankroll = st.number_input("Bankroll ($)", min_value=100, max_value=1_000_000,
                                   value=1000, step=100)
        kelly_fraction = 0.25  # Quarter-Kelly for conservative sizing

        if value_horses.empty:
            st.info("No value plays — nothing to size.")
        else:
            st.markdown("")
            for _, horse in value_horses.iterrows():
                # Kelly formula: (bp - q) / b
                # b = decimal odds - 1, p = implied prob from fair odds, q = 1 - p
                fair_prob = 1 / horse['fair_odds']
                market_prob = 1 / horse['morning_line']
                b = horse['morning_line'] - 1
                p = fair_prob
                q = 1 - p

                full_kelly = ((b * p) - q) / b if b > 0 else 0
                adj_kelly = max(0, full_kelly * kelly_fraction)
                bet_amount = round(bankroll * adj_kelly, 2)

                is_strong = horse['recommendation'] == "STRONG VALUE"
                accent = "#34d399" if is_strong else "#a3e635"

                st.markdown(f"""
                <div class="value-card" style="border-color: {accent};">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="font-size:1.1rem; font-weight:700; color:#e8e8e8;">
                                {horse['name']}
                            </span>
                            <span style="color:#8890a4; font-size:0.8rem; margin-left:12px;">
                                ML {horse['morning_line']:.1f} → Fair {horse['fair_odds']:.1f}
                            </span>
                        </div>
                        <div style="text-align:right;">
                            <span style="color:{accent}; font-size:1.6rem; font-weight:700;">
                                ${bet_amount:,.2f}
                            </span>
                            <span style="color:#8890a4; font-size:0.7rem; display:block;">
                                {adj_kelly*100:.1f}% of bankroll
                            </span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.markdown("")

            total_bet = sum(
                round(bankroll * max(0, (((row['morning_line'] - 1) * (1 / row['fair_odds'])) - (1 - 1 / row['fair_odds'])) / (row['morning_line'] - 1) * kelly_fraction), 2)
                for _, row in value_horses.iterrows()
            )
            st.markdown(f"""
            <div style="margin-top:10px; padding:16px; background:#111833; border-radius:8px;
                        border:1px solid #1a2040; display:flex; justify-content:space-between;">
                <span style="color:#8890a4;">Total Exposure</span>
                <span style="color:#b8965a; font-weight:700; font-size:1.1rem;">
                    ${total_bet:,.2f}
                    <span style="color:#8890a4; font-size:0.75rem; font-weight:400;">
                        ({total_bet/bankroll*100:.1f}% of bankroll)
                    </span>
                </span>
            </div>
            """, unsafe_allow_html=True)


# ======================================================================
# APP ENTRY
# ======================================================================

if not check_login():
    show_login()
else:
    main()
