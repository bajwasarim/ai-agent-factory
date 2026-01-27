"""Serper API tool for Google-style search integration."""

import os
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from core.logger import get_logger
from core.tool_registry import tool_registry

load_dotenv()
logger = get_logger(__name__)

SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERPER_BASE_URL = "https://google.serper.dev/search"
DEFAULT_TIMEOUT = 15
MOCK_SEARCH = os.getenv("MOCK_SEARCH", "").lower() == "true"

logger.info(f"SERPER_API_KEY loaded: {bool(SERPER_API_KEY)}")
logger.info(f"MOCK_SEARCH enabled: {MOCK_SEARCH}")


def serper_search(
    query: str,
    location: str = "",
    num_results: int = 20,
    search_type: str = "search",
) -> Dict[str, Any]:
    """
    Perform Google-style search using Serper API.

    Args:
        query: Search query string.
        location: Optional location code (e.g., "us", "uk").
        num_results: Number of results to return (default: 20).
        search_type: Type of search - "search", "news", "images" (default: "search").

    Returns:
        Dict containing search results with keys like 'organic', 'knowledgeGraph', etc.

    Raises:
        ValueError: If SERPER_API_KEY is not set and MOCK_SEARCH is disabled.
        requests.HTTPError: If API request fails.
    """
    # Return mock data if MOCK_SEARCH is enabled
    if MOCK_SEARCH:
        logger.info(f"Mock search for: '{query}' in '{location}'")
        return {
            "organic": [
                {"title": "Mock Dentist NYC", "link": "https://example.com/dentist1", "snippet": "Top rated dentist in NYC area."},
                {"title": "Mock Dental Clinic", "link": "https://example.com/dentist2", "snippet": "Family dental care services."},
                {"title": "Mock Orthodontist", "link": "https://example.com/dentist3", "snippet": "Braces and aligners specialist."},
            ]
        }

    if not SERPER_API_KEY:
        raise ValueError("SERPER_API_KEY not set in environment")

    payload: Dict[str, Any] = {
        "q": query,
        "num": num_results,
    }

    if location:
        payload["gl"] = location

    headers = {
        "X-API-KEY": SERPER_API_KEY,
        "Content-Type": "application/json",
    }

    logger.debug(f"Serper search: query='{query}', location='{location}', num={num_results}")

    try:
        response = requests.post(
            SERPER_BASE_URL,
            json=payload,
            headers=headers,
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Serper returned {len(data.get('organic', []))} organic results")
        return data

    except requests.Timeout:
        logger.error("Serper API request timed out")
        raise
    except requests.HTTPError as e:
        logger.error(f"Serper API error: {e.response.status_code} - {e.response.text}")
        raise


def extract_organic_results(
    serper_response: Dict[str, Any],
    max_results: Optional[int] = None,
) -> List[Dict[str, str]]:
    """
    Extract simplified organic results from Serper response.

    Args:
        serper_response: Raw response from serper_search.
        max_results: Optional limit on number of results.

    Returns:
        List of dicts with 'title', 'link', 'snippet' keys.
    """
    organic = serper_response.get("organic", [])

    if max_results:
        organic = organic[:max_results]

    return [
        {
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "snippet": item.get("snippet", ""),
        }
        for item in organic
    ]


# Register tools for hot-swappable integration
tool_registry.register("serper_search", serper_search)
tool_registry.register("extract_organic_results", extract_organic_results)
