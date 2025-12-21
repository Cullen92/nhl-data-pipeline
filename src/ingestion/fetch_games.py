import os
import json
import logging
from datetime import datetime, timezone
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

NHL_API_URL = "https://api-web.nhle.com/v1/schedule/now"


def fetch_nhl_schedule(date: str | None = None) -> dict:
    params = {"date": date} if date else {}
    logging.info(f"Fetching NHL schedule for {date or 'today'}")

    response = requests.get(NHL_API_URL, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def save_raw_json(data: dict, output_dir: str = "data/raw") -> str:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_path = os.path.join(output_dir, f"nhl_schedule_{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logging.info(f"Saved raw data to {file_path}")
    return file_path


def main():
    data = fetch_nhl_schedule()
    save_raw_json(data)


if __name__ == "__main__":
    main()