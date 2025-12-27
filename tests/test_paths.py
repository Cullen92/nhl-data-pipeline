from nhl_pipeline.utils.paths import (
    raw_game_boxscore_key,
    raw_game_pbp_key,
    raw_meta_backfill_gamecenter_success_key,
    raw_schedule_key,
    raw_stats_skater_powerplay_key,
    raw_stats_skater_summary_key,
    raw_stats_skater_timeonice_key,
    snapshot_filename,
)

# These tests intentionally “lock” the S3 key conventions.
# If someone changes folder names, partition layout, or filename format,
# we want a fast unit test failure before it breaks Snowflake COPY patterns.

# In all functions, the raw string (right side of equation) is the source of truth.
# Those values are checked against our functions to assure they've not accidentally changed.


def test_snapshot_filename_format():
    # Canonical snapshot filename includes partition date + hour.
    assert snapshot_filename("2025-12-25", "03") == "snapshot_2025_12_25_03.json"


def test_raw_schedule_key_format():
    # Schedule snapshots live under raw/nhl/schedule with date/hour partitions.
    assert (
        raw_schedule_key("2025-12-25", "03")
        == "raw/nhl/schedule/date=2025-12-25/hour=03/snapshot_2025_12_25_03.json"
    )


def test_raw_game_boxscore_key_format():
    # Game boxscores include game_id as an extra partition level.
    assert (
        raw_game_boxscore_key("2025-12-25", "03", game_id=2025020575)
        == "raw/nhl/game_boxscore/date=2025-12-25/hour=03/game_id=2025020575/snapshot_2025_12_25_03.json"
    )


def test_raw_game_pbp_key_format():
    # Play-by-play (pbp) uses the same partition scheme as boxscore.
    assert (
        raw_game_pbp_key("2025-12-25", "03", game_id="2025020575")
        == "raw/nhl/game_pbp/date=2025-12-25/hour=03/game_id=2025020575/snapshot_2025_12_25_03.json"
    )


def test_raw_stats_report_key_format():
    # Stats REST endpoints are paged (start/limit). We partition by start to avoid collisions.
    assert (
        raw_stats_skater_summary_key("2025-12-25", "03", 20252026, 2, start=0)
        == "raw/nhl/stats/skater_summary/date=2025-12-25/hour=03/season_id=20252026/game_type_id=2/start=0/snapshot_2025_12_25_03.json"
    )
    assert (
        raw_stats_skater_timeonice_key("2025-12-25", "03", 20252026, 2, start=1000)
        == "raw/nhl/stats/skater_timeonice/date=2025-12-25/hour=03/season_id=20252026/game_type_id=2/start=1000/snapshot_2025_12_25_03.json"
    )
    assert (
        raw_stats_skater_powerplay_key("2025-12-25", "03", 20252026, 2, start=2000)
        == "raw/nhl/stats/skater_powerplay/date=2025-12-25/hour=03/season_id=20252026/game_type_id=2/start=2000/snapshot_2025_12_25_03.json"
    )


def test_raw_meta_backfill_gamecenter_success_key_format():
    assert (
        raw_meta_backfill_gamecenter_success_key("2025-12-25")
        == "raw/nhl/_meta/backfill/gamecenter/date=2025-12-25/_SUCCESS.json"
    )
