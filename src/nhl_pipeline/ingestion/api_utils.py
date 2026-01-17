"""
Utilities for making resilient HTTP API calls.

Provides retry logic and error handling for external API requests,
particularly for the NHL API endpoints and The Odds API.
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Any

import requests
from requests.exceptions import HTTPError, RequestException

logger = logging.getLogger(__name__)


@dataclass
class ApiUsage:
    """Track API credit usage for The Odds API."""
    requests_used: int = 0
    requests_remaining: int = 0
    last_cost: int = 0
    
    def to_dict(self) -> dict[str, int]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "requests_used": self.requests_used,
            "requests_remaining": self.requests_remaining,
            "last_cost": self.last_cost,
        }


def make_api_call(url, headers=None, retries=3, timeout=30):
    """Make a resilient API call with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except (HTTPError, RequestException) as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt == retries - 1:
                raise type(e)(f"Failed to fetch {url} after {retries} attempts: {e}") from e


def make_odds_api_request(
    base_url: str,
    endpoint: str,
    params: dict[str, Any],
    api_key: str,
    max_retries: int = 3,
    retry_delay_seconds: int = 5,
) -> tuple[dict[str, Any] | list[Any], ApiUsage]:
    """
    Make a request to The Odds API with retry logic and rate limiting.
    
    Args:
        base_url: The base URL for The Odds API
        endpoint: API endpoint path
        params: Query parameters (apiKey will be added automatically)
        api_key: The Odds API key
        max_retries: Maximum number of retry attempts (default: 3)
        retry_delay_seconds: Base delay between retries in seconds (default: 5)
        
    Returns:
        Tuple of (response data, API usage info)
        
    Raises:
        RuntimeError: If all retry attempts fail
        requests.exceptions.RequestException: For unrecoverable request errors
    """
    params["apiKey"] = api_key
    url = f"{base_url}/{endpoint}"
    
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            
            if resp.status_code == 429:
                # Rate limited - wait and retry with exponential backoff
                wait_time = retry_delay_seconds * (attempt + 1)
                logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            
            resp.raise_for_status()
            
            # Extract usage info from response headers
            usage = ApiUsage(
                requests_used=int(resp.headers.get("x-requests-used", 0)),
                requests_remaining=int(resp.headers.get("x-requests-remaining", 0)),
                last_cost=int(resp.headers.get("x-requests-last", 0)),
            )
            
            return resp.json(), usage
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Request failed: {e}. Retrying...")
                time.sleep(retry_delay_seconds)
            else:
                raise
    
    raise RuntimeError(f"Failed after {max_retries} retries")
