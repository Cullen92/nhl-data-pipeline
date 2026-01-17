"""
NHL Player Dashboard

A Streamlit app for visualizing NHL player statistics and game data.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

from config import ST_CONFIG, CACHE_TTL

# Page configuration
st.set_page_config(**ST_CONFIG)

# Initialize session state
if 'selected_player' not in st.session_state:
    st.session_state.selected_player = None

@st.cache_data(ttl=CACHE_TTL["static_data"])
def load_players():
    """Load available players (mock data for now)."""
    return [
        "A. Peeke", "Connor McDavid", "Leon Draisaitl", 
        "Nathan MacKinnon", "David Pastrnak"
    ]

@st.cache_data(ttl=CACHE_TTL["player_stats"])
def load_player_stats(player_name: str):
    """Load player statistics (mock data for now)."""
    # TODO: Replace with actual data source (Snowflake/Iceberg)
    import numpy as np
    
    # Generate mock data
    games = 45
    dates = pd.date_range(end=datetime.now(), periods=games)
    
    return pd.DataFrame({
        'date': dates,
        'goals': np.random.poisson(0.5, games),
        'assists': np.random.poisson(1.0, games),
        'shots': np.random.poisson(2.5, games),
        'hits': np.random.poisson(1.8, games),
        'blocks': np.random.poisson(0.8, games)
    })

@st.cache_data(ttl=CACHE_TTL["player_stats"])
def load_shot_coordinates(player_name: str):
    """Load shot coordinate data (mock data for now)."""
    import numpy as np
    
    # Hockey rink is approximately 200ft x 85ft
    # Coordinates: x=-100 to 100, y=-42.5 to 42.5
    num_shots = 50
    
    return pd.DataFrame({
        'x': np.random.uniform(-100, 100, num_shots),
        'y': np.random.uniform(-42.5, 42.5, num_shots),
        'shot_type': np.random.choice(['Goal', 'Shot', 'Miss'], num_shots, p=[0.1, 0.7, 0.2]),
        'game_date': np.random.choice(pd.date_range(end=datetime.now(), periods=10), num_shots)
    })

def main():
    """Main dashboard interface."""
    
    st.title("🏒 NHL Player Dashboard")
    
    # Sidebar - Player selection
    with st.sidebar:
        st.header("Player Selection")
        players = load_players()
        selected_player = st.selectbox("Choose a player", players)
        
        if selected_player != st.session_state.selected_player:
            st.session_state.selected_player = selected_player
            st.rerun()
    
    if not selected_player:
        st.info("Please select a player from the sidebar.")
        return
    
    # Load data
    stats_df = load_player_stats(selected_player)
    shots_df = load_shot_coordinates(selected_player)
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header(f"📊 {selected_player} Statistics")
        
        # Shot map
        st.subheader("Shot Map")
        fig_shots = go.Figure()
        
        # Color mapping for shot types
        colors = {'Goal': 'red', 'Shot': 'blue', 'Miss': 'lightblue'}
        
        for shot_type in shots_df['shot_type'].unique():
            shot_data = shots_df[shots_df['shot_type'] == shot_type]
            fig_shots.add_trace(go.Scatter(
                x=shot_data['x'],
                y=shot_data['y'],
                mode='markers',
                marker=dict(
                    color=colors.get(shot_type, 'gray'),
                    size=8,
                    opacity=0.7
                ),
                name=shot_type,
                text=shot_data['game_date'].dt.strftime('%Y-%m-%d'),
                hovertemplate=f'{shot_type}<br>Date: %{{text}}<extra></extra>'
            ))
        
        # Add rink outline (simplified)
        fig_shots.add_shape(
            type="rect",
            x0=-100, y0=-42.5, x1=100, y1=42.5,
            line=dict(color="black", width=2),
            fillcolor="rgba(0,0,0,0)"
        )
        
        fig_shots.update_layout(
            height=400,
            xaxis_title="Rink Length (ft)",
            yaxis_title="Rink Width (ft)",
            showlegend=True,
            xaxis=dict(range=[-110, 110]),
            yaxis=dict(range=[-50, 50], scaleanchor="x", scaleratio=1)
        )
        
        st.plotly_chart(fig_shots, width='stretch')
        
        # Performance over time
        st.subheader("Performance Over Time")
        
        metric_choice = st.selectbox("Select metric", ['shots', 'goals', 'assists', 'hits', 'blocks'])
        
        fig_time = px.line(
            stats_df, 
            x='date', 
            y=metric_choice,
            title=f"{metric_choice.capitalize()} Over Time"
        )
        fig_time.add_hline(y=stats_df[metric_choice].mean(), line_dash="dash", annotation_text="Season Average")
        
        st.plotly_chart(fig_time, width='stretch')
    
    with col2:
        st.header("📈 Key Stats")
        
        # Current stats
        recent_stats = stats_df.tail(10)
        
        st.metric(
            "Goals (Last 10 games)",
            recent_stats['goals'].sum(),
            delta=f"{recent_stats['goals'].sum() - stats_df['goals'].iloc[-20:-10].sum()}"
        )
        
        st.metric(
            "Shots per game (Last 10)",
            f"{recent_stats['shots'].mean():.2f}",
            delta=f"{recent_stats['shots'].mean() - stats_df['shots'].mean():.2f}"
        )
        
        st.metric(
            "Total shots (Season)",
            stats_df['shots'].sum()
        )
        
        # Mini charts
        st.subheader("Recent Trends")
        
        # Goals trend
        st.line_chart(recent_stats.set_index('date')['goals'], height=150)
        st.caption("Goals - Last 10 games")
        
        # Shots trend  
        st.line_chart(recent_stats.set_index('date')['shots'], height=150)
        st.caption("Shots - Last 10 games")

if __name__ == "__main__":
    main()