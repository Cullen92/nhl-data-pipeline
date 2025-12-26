from datetime import datetime, timezone

from nhl_pipeline.ingestion.gamecenter_selection import extract_game_ids


def test_extract_game_ids_final_only_filters_non_final_and_time_window():
    payload = {
        "gameWeek": [
            {
                "date": "2025-10-07",
                "games": [
                    {
                        "id": 2025020001,
                        "startTimeUTC": "2025-10-07T23:00:00Z",
                        "gameState": "OFF",
                    },
                    {
                        "id": 2025020002,
                        "startTimeUTC": "2025-10-07T23:00:00Z",
                        "gameState": "FUT",
                    },
                    {
                        "id": 2025020003,
                        "startTimeUTC": "2025-10-01T23:00:00Z",
                        "gameState": "OFF",
                    },
                ],
            }
        ]
    }

    partition_dt = datetime(2025, 10, 7, 12, 0, 0, tzinfo=timezone.utc)

    # Look back 3 days: excludes the 10/01 game; FINAL-only excludes FUT.
    game_ids = extract_game_ids(payload, partition_dt=partition_dt, lookback_days=3, only_final=True)

    assert game_ids == [2025020001]
