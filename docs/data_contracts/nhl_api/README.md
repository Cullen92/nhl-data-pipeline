# NHL API Data Contracts

Documentation for the NHL API (api-web.nhle.com) response payloads.

## Endpoints

| Endpoint | File | Description |
|----------|------|-------------|
| `/v1/gamecenter/{game_id}/boxscore` | [boxscore.yml](boxscore.yml) | Complete game stats |
| `/v1/gamecenter/{game_id}/play-by-play` | play_by_play.yml | Event-by-event data |
| `/v1/schedule/now` | schedule.yml | Current week schedule |

## Base URL

```
https://api-web.nhle.com
```

## Common Patterns

### Localized Strings
Many fields use a localization pattern:
```json
{
  "name": {
    "default": "Bruins",
    "fr": "les Bruins"
  }
}
```
Use `.default` for English.

### Game Types
| Value | Meaning |
|-------|---------|
| 1 | Preseason |
| 2 | Regular Season |
| 3 | Playoffs |
| 4 | All-Star |

### Game States
| Value | Meaning |
|-------|---------|
| FUT | Future (not started) |
| PRE | Pre-game |
| LIVE | In progress |
| FINAL | Completed |
| OFF | Final (older format) |

### Game ID Format
Game IDs follow the pattern: `YYYYTTNNNN`
- `YYYY` = Season start year (e.g., 2024)
- `TT` = Game type (01=preseason, 02=regular, 03=playoffs)
- `NNNN` = Game number

Example: `2024020001` = 2024-25 regular season, game 1

## Rate Limiting

The NHL API is public but undocumented. Be respectful:
- Add delays between requests (0.25s recommended)
- Cache responses when possible
- Don't hammer during game time
