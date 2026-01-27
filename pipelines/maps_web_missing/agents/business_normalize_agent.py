"""Business normalization agent for Maps No-Website Pipeline."""

from typing import Any, Dict, List

from pipelines.core.base_agent import BaseAgent
from pipelines.maps_web_missing.config import SOURCE_IDENTIFIER
from pipelines.maps_web_missing.utils.helpers import compute_dedup_key
from core.logger import get_logger

logger = get_logger(__name__)


class BusinessNormalizeAgent(BaseAgent):
    """
    Agent that normalizes raw search results into a clean structure.

    Designed for Google Maps results where businesses may not have websites.
    Accepts both raw API format and pre-normalized format from MapsSearchAgent.

    Input: raw_search_results (with 'places' list), location
    Output: normalized_businesses (list of cleaned business dicts)
    """

    def __init__(self) -> None:
        """Initialize the business normalize agent."""
        super().__init__(name="BusinessNormalizeAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize raw search results into clean business records.

        Handles both:
        1. Raw Serper API format (title, phone, etc.)
        2. Pre-normalized format from MapsSearchAgent (name, phone_number, etc.)

        Args:
            input_data: Dict with 'raw_search_results' from search agent.

        Returns:
            Dict with 'normalized_businesses' list of cleaned business data.

        Raises:
            KeyError: If 'raw_search_results' is missing from input_data.
        """
        raw = input_data.get("raw_search_results", {})
        location = input_data.get("location", "")

        # Support both 'organic' (Serper) and 'places' (Maps) result formats
        results = raw.get("places", raw.get("organic", []))

        normalized: List[Dict[str, Any]] = []

        for item in results:
            normalized.append(self._normalize_item(item, location))

        logger.info(f"Normalized {len(normalized)} businesses")
        return {"normalized_businesses": normalized}

    def _normalize_item(self, item: Dict[str, Any], location: str) -> Dict[str, Any]:
        """
        Normalize a single business item to standard schema.

        Handles both raw API format and pre-normalized format.

        Args:
            item: Raw or pre-normalized business dict.
            location: Fallback location string.

        Returns:
            Normalized business dict.
        """
        # Handle both 'title' (raw API) and 'name' (pre-normalized)
        name = item.get("name") or item.get("title", "")

        # Handle both 'link' (organic) and 'website' (maps)
        website = item.get("website") or item.get("link", "")

        # Handle description: use address if snippet not available
        description = item.get("snippet") or item.get("description") or item.get("address", "")

        # Handle phone: both 'phone' (raw) and 'phone_number' (normalized)
        phone = item.get("phone_number") or item.get("phone", "")

        # Get address
        address = item.get("address", "")

        # Use item's location if available, otherwise fallback
        item_location = item.get("location") or location

        # Get place_id for deduplication
        place_id = item.get("place_id", item.get("cid", ""))

        # Compute or preserve dedup_key
        dedup_key = item.get("dedup_key") or compute_dedup_key(
            place_id=place_id,
            name=name,
            phone=phone,
            address=address,
        )

        return {
            "name": name,
            "website": website,
            "description": description,
            "source": item.get("source", SOURCE_IDENTIFIER),
            "location": item_location,
            # Maps-specific fields (optional)
            "phone": phone,
            "rating": item.get("rating", ""),
            "reviews": item.get("reviews", ""),
            "address": address,
            "place_id": place_id,
            "dedup_key": dedup_key,
            "category": item.get("category", ""),
        }
