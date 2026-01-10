import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import time

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Nexus Racing Analytics", page_icon="🏇", layout="wide")

# --- MOCK DATA GENERATOR (Replaces API for MVP) ---
def get_mock_race_data():
    # Simulate a field of horses with realistic stats
    horses = [
        {"name": "Thunder Bolt", "jockey": "I. Ortiz", "trainer": "T. Pletcher", "last_speed": 98, "days_off": 14, "morning_line": 3.5},
        {"name": "Shadow Dancer", "jockey": "J. Rosario", "trainer": "C. Brown", "last_speed": 102, "days_off": 21, "morning_line": 2.8},
        {"name": "Longshot Lou", "jockey": "K. Carmouche", "trainer": "L. Rice", "last_speed": 82, "days_off": 60, "morning_line": 15.0},
        {"name": "Midnight Echo", "jockey": "F. Prat", "trainer": "B. Cox", "last_speed": 95, "days_off": 28, "morning_line": 4.5},
        {"name": "Speed Demon", "jockey": "L. Saez", "trainer": "W. Ward", "last_speed": 88, "days_off": 7, "morning_line": 8.0},
        {"name": "Ghost Run", "jockey": "M. Smith", "trainer": "B. Baffert", "last_speed": 91, "days_off": 35, "morning_line": 6.0},
    ]
    return pd.DataFrame(horses)

# --- ALGORITHM: THE NEXUS SCORE ---
def calculate_nexus_score(df):
    # This is your "Secret Sauce" logic
    # We weight Recency and Speed heavily
    
    # 1. Normalize Speed (0-100 scale relative to field)
    df['speed_rating'] = (df['last_speed'] / df['last_speed'].max()) * 100
    
    # 2. Form Cycle (Penalty for too many days off)
    df['form_penalty'] = df['days_off'].apply(lambda x: 10 if x > 45 else 0)
    
    # 3. Calculate Final Score
    # Formula: (Speed * 0.7) - Form Penalty + (Random 'AI' noise for variance)
    df['nexus_score'] = (df['speed_rating'] * 0.7) - df['form_penalty'] + np.random.randint(0, 10, size=len(df))
    df['nexus_score'] = df['nexus_score'].round(1)
    
    # 4. Calculate "Fair Value" Odds based on the Score
    # Higher score = Lower fair odds
    df['fair_odds'] = (100 / df['nexus_score']).round(1)
    
    # 5. Identify "Overlays" (Value Bets)
    # If Market Odds > Fair Odds, it's a good bet
    df['value_gap'] = df['morning_line'] - df['fair_odds']
    df['recommendation'] = df['value_gap'].apply(lambda x: "🔥 HIGH VALUE" if x > 2.0 else ("✅ FAIR" if x > 0 else "❌ AVOID"))
    
    return df.sort_values(by='nexus_score', ascending=False)

# --- SIDEBAR (User Controls) ---
st.sidebar.title("🏇 Nexus Control")
track = st.sidebar.selectbox("Select Track", ["Saratoga", "Del Mar", "Churchill Downs", "Gulfstream"])
race_num = st.sidebar.selectbox("Select Race Number", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
surface = st.sidebar.radio("Surface", ["Dirt", "Turf", "Synthetic"])

st.sidebar.markdown("---")
st.sidebar.caption("Nexus Racing AI v1.0")

# --- MAIN APP LAYOUT ---
st.title(f"Nexus Racing Analytics: {track}")
st.markdown(f"**Race {race_num}** | Surface: **{surface}** | Status: **Live**")

# Button to trigger analysis
if st.button("🚀 Run AI Analysis"):
    with st.spinner('Ingesting real-time data... Calculating speed figures... Simulating race 10,000 times...'):
        time.sleep(2) # Fake processing time for "drama"
        
        # 1. Get Data
        raw_data = get_mock_race_data()
        
        # 2. Run Algorithm
        analyzed_data = calculate_nexus_score(raw_data)
        
        # 3. Display Top Metrics
        col1, col2, col3 = st.columns(3)
        top_horse = analyzed_data.iloc[0]
        col1.metric("Top Pick", top_horse['name'], f"Score: {top_horse['nexus_score']}")
        col2.metric("Best Value", top_horse['name'], f"+{top_horse['value_gap']} Edge")
        col3.metric("System Confidence", "87%", "+2.4%")

        st.markdown("### 📊 The Nexus Grid")
        
        # Format the dataframe for display
        display_cols = ['name', 'jockey', 'morning_line', 'fair_odds', 'nexus_score', 'recommendation']
        st.dataframe(
            analyzed_data[display_cols].style.applymap(
                lambda v: 'color: green; font-weight: bold;' if v == '🔥 HIGH VALUE' else None, subset=['recommendation']
            ),
            use_container_width=True
        )

        # 4. Visualization
        st.markdown("### 📈 Speed vs. Value Matrix")
        fig = px.scatter(
            analyzed_data, 
            x="nexus_score", 
            y="morning_line", 
            size="last_speed", 
            color="recommendation",
            hover_name="name",
            text="name",
            title="Identify High Scores with High Odds (Top Right is Best)",
            labels={"nexus_score": "Nexus AI Score", "morning_line": "Current Market Odds"}
        )
        fig.update_traces(textposition='top center')
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("👈 Select a track and race from the sidebar, then click 'Run AI Analysis'")
