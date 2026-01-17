"""
Configuration for Streamlit apps.
"""

import os
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"

# Streamlit configuration
ST_CONFIG = {
    "page_title": "NHL Data Dashboard",
    "page_icon": "🏒",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Data source settings
DATA_SOURCES = {
    "snowflake": {
        "use_connection": True,
        "connection_name": "snowflake"  # Use Streamlit secrets
    },
    "iceberg": {
        "enabled": False,  # Future enhancement
        "s3_bucket": os.getenv("S3_BUCKET", "")
    }
}

# Cache settings
CACHE_TTL = {
    "player_stats": 300,  # 5 minutes
    "game_data": 60,      # 1 minute for live games
    "static_data": 3600   # 1 hour for rosters, etc.
}