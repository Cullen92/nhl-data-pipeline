import requests
import logging
from requests.exceptions import HTTPError, RequestException

def make_api_call(url, headers=None, retries=3):
    """Make a resilient API call with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response
        except (HTTPError, RequestException) as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise
