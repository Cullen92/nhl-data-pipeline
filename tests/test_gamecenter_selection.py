
from nhl_pipeline.ingestion.gamecenter_selection import extract_final_game_ids


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

    # Only FINAL games are returned
    game_ids = extract_final_game_ids(payload)

    assert game_ids == [2025020001]
