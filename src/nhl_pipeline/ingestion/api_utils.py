"""
Utilities for making resilient HTTP API calls.

Provides retry logic and error handling for external API requests,
particularly for the NHL API endpoints.
"""

import requests
import logging
from requests.exceptions import HTTPError, RequestException

def make_api_call(url, headers=None, retries=3, timeout=30):
    """Make a resilient API call with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except (HTTPError, RequestException) as e:
            logging.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise type(e)(f"Failed to fetch {url} after {retries} attempts: {e}") from e
