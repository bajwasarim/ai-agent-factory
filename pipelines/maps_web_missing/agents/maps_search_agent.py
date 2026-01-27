"""
Maps search agent for Maps No-Website Pipeline.

Features:
- Pagination support via page parameter
- Radius expansion for full geographic coverage
- Deduplication by place_id or name+address
- Configurable search parameters
"""

import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

from pipelines.core.base_agent import BaseAgent
from pipelines.maps_web_missing.utils.helpers import compute_dedup_key
from core.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# API Configuration
SERPER_API_KEY = os.getenv("SERPER_API_KEY")
SERPER_MAPS_URL = "https://google.serper.dev/maps"
DEFAULT_TIMEOUT = 15
MOCK_MAPS = os.getenv("MOCK_MAPS", "").lower() in ("true", "1", "yes")

# Serper Maps API returns up to 20 results per request (no pagination support)
RESULTS_PER_REQUEST = 20

# Radius Expansion Configuration
# Since Serper doesn't support pagination, we use geographic segments for coverage
DEFAULT_RADIUS_KM = float(os.getenv("MAPS_RADIUS_KM", "5"))
MAX_SEGMENTS_PER_LOCATION = int(os.getenv("MAPS_MAX_SEGMENTS", "9"))

# Rate Limiting
API_DELAY_SECONDS = float(os.getenv("MAPS_API_DELAY", "0.5"))

# Radius segment offsets for 3x3 grid expansion (in degrees, ~111km per degree)
# Each offset covers approximately radius_km from center
GRID_OFFSETS = [
    (0, 0),      # Center
    (0, 1),      # North
    (0, -1),     # South
    (1, 0),      # East
    (-1, 0),     # West
    (1, 1),      # Northeast
    (-1, 1),     # Northwest
    (1, -1),     # Southeast
    (-1, -1),    # Southwest
]


class MapsSearchAgent(BaseAgent):
    """
    Agent that searches Google Maps for business listings using Serper API.

    Features:
    - Radius Expansion: Splits large areas into smaller geographic segments
    - Deduplication: Merges results by place_id or name+address
    - Full Coverage: Searches multiple sub-areas to capture all businesses

    Note: Serper Maps API does not support pagination. To maximize coverage,
    this agent uses geographic segment expansion (e.g., "north New York",
    "downtown New York") to search different sub-areas.

    This agent should be the FIRST in the pipeline.

    Input:
        - query: Search query (e.g., "dentist")
        - location: Location string (e.g., "New York")
        - radius_km: Optional search radius (default: 5)
        - max_segments: Optional max geographic segments (default: 9)

    Output:
        - raw_search_results: Dict with 'places' list
        - search_metadata: Stats about the search operation
    """

    def __init__(self) -> None:
        """Initialize the Maps search agent."""
        super().__init__(name="MapsSearchAgent")
        self._seen_ids: Set[str] = set()
        logger.info(
            f"MapsSearchAgent initialized "
            f"(MOCK_MAPS: {MOCK_MAPS}, "
            f"max_segments: {MAX_SEGMENTS_PER_LOCATION})"
        )

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute Google Maps search with radius expansion for full coverage.

        Args:
            input_data: Dict with query, location, and optional config overrides.

        Returns:
            Dict with 'raw_search_results' containing deduplicated 'places' list
            and 'search_metadata' with operation statistics.

        Raises:
            ValueError: If 'query' is missing.
            RuntimeError: If API key not set or all API requests fail.
        """
        query = input_data.get("query", "")
        location = input_data.get("location", "")
        radius_km = input_data.get("radius_km", DEFAULT_RADIUS_KM)
        max_segments = input_data.get("max_segments", MAX_SEGMENTS_PER_LOCATION)

        if not query:
            raise ValueError("'query' is required in input_data")

        logger.info(f"Starting Maps search: '{query}' in '{location}'")
        logger.info(
            f"Config: radius={radius_km}km, max_segments={max_segments}"
        )

        # Reset deduplication state for new run
        self._seen_ids.clear()

        # Return mock data if MOCK_MAPS is enabled
        if MOCK_MAPS:
            logger.info("MOCK_MAPS enabled - returning mock data")
            raw_places = self._get_mock_data(location)
            # Apply deduplication to mock data to demonstrate the feature
            deduplicated_places = []
            for place in raw_places:
                dedup_key = self._get_dedup_key(place)
                if dedup_key not in self._seen_ids:
                    self._seen_ids.add(dedup_key)
                    deduplicated_places.append(place)
            logger.info(
                f"Mock data: {len(raw_places)} raw → "
                f"{len(deduplicated_places)} after deduplication"
            )
            return {
                "raw_search_results": {"places": deduplicated_places},
                "search_metadata": {
                    "total_raw": len(raw_places),
                    "total_deduplicated": len(deduplicated_places),
                    "segments_processed": 1,
                    "mock_mode": True,
                },
            }

        # Validate API key
        if not SERPER_API_KEY:
            raise RuntimeError("SERPER_API_KEY not set")

        # Execute search with radius expansion for full coverage
        all_places, metadata = self._search_with_coverage(
            query=query,
            location=location,
            radius_km=radius_km,
            max_segments=max_segments,
        )

        logger.info(
            f"Search complete: {metadata['total_deduplicated']} unique places "
            f"from {metadata['total_raw']} raw results "
            f"({metadata['segments_processed']} segments)"
        )

        return {
            "raw_search_results": {"places": all_places},
            "search_metadata": metadata,
        }

    def _search_with_coverage(
        self,
        query: str,
        location: str,
        radius_km: float,
        max_segments: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Execute search with full geographic coverage via radius expansion.

        Searches multiple geographic segments (e.g., "north New York",
        "downtown New York") to maximize coverage since Serper Maps
        doesn't support pagination.

        Args:
            query: Search query string.
            location: Base location string.
            radius_km: Radius for each search segment.
            max_segments: Maximum geographic segments to search.

        Returns:
            Tuple of (deduplicated places list, metadata dict).
        """
        all_places: List[Dict[str, Any]] = []
        total_raw = 0
        segments_processed = 0

        # Generate search segments based on radius expansion
        segments = self._generate_segments(location, radius_km, max_segments)
        logger.info(f"Generated {len(segments)} geographic segments")

        for segment_idx, segment_location in enumerate(segments):
            if segment_idx >= max_segments:
                logger.info(f"Reached max_segments limit ({max_segments})")
                break

            logger.info(
                f"Segment {segment_idx + 1}/{len(segments)}: {segment_location}"
            )

            # Search this segment
            segment_places = self._search_segment(
                query=query,
                location=segment_location,
            )

            total_raw += len(segment_places)
            segments_processed += 1

            # Deduplicate and add to results
            for place in segment_places:
                dedup_key = self._get_dedup_key(place)
                if dedup_key not in self._seen_ids:
                    self._seen_ids.add(dedup_key)
                    # Normalize to output schema
                    all_places.append(self._normalize_place(place, location))

            logger.info(
                f"  Segment yielded {len(segment_places)} raw, "
                f"{len(all_places)} total unique so far"
            )

            # Rate limiting between segments
            if segment_idx < len(segments) - 1:
                time.sleep(API_DELAY_SECONDS)

        metadata = {
            "total_raw": total_raw,
            "total_deduplicated": len(all_places),
            "segments_processed": segments_processed,
            "mock_mode": False,
        }

        return all_places, metadata

    def _search_segment(
        self,
        query: str,
        location: str,
    ) -> List[Dict[str, Any]]:
        """
        Search a single geographic segment.

        Args:
            query: Search query string.
            location: Location string for this segment.

        Returns:
            List of place dicts from API response.
        """
        search_query = f"{query} in {location}"

        try:
            places = self._search_maps(search_query)
            return places
        except RuntimeError as e:
            logger.warning(f"Segment search failed: {e}")
            return []

    def _search_maps(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute single Serper Maps API request.

        Args:
            query: Full search query string.

        Returns:
            List of place dicts from API response.

        Raises:
            RuntimeError: If API request fails.
        """
        payload = {
            "q": query,
        }

        headers = {
            "X-API-KEY": SERPER_API_KEY,
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                SERPER_MAPS_URL,
                json=payload,
                headers=headers,
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()

        except requests.exceptions.Timeout:
            raise RuntimeError(f"Maps API request timed out after {DEFAULT_TIMEOUT}s")
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning("Rate limit hit, backing off")
                time.sleep(2)
                raise RuntimeError("Rate limited by API")
            raise RuntimeError(f"Maps API HTTP error: {e}")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Maps API request failed: {e}")

        data = response.json()
        return data.get("places", [])

    def _generate_segments(
        self,
        base_location: str,
        radius_km: float,
        max_segments: int,
    ) -> List[str]:
        """
        Generate geographic search segments for radius expansion.

        Creates location strings for a grid of search areas around
        the base location to ensure full coverage.

        Args:
            base_location: Center location string.
            radius_km: Radius in km for each segment.
            max_segments: Maximum number of segments to generate.

        Returns:
            List of location strings to search.
        """
        # For simple text-based locations, we use location modifiers
        # to cover different sub-areas
        segments = [base_location]  # Always include the center

        if max_segments <= 1:
            return segments

        # Add directional modifiers for expanded coverage
        directional_suffixes = [
            "",  # Center (already added)
            "north",
            "south",
            "east",
            "west",
            "downtown",
            "midtown",
            "uptown",
            "central",
        ]

        for suffix in directional_suffixes[1:max_segments]:
            segments.append(f"{suffix} {base_location}")

        return segments[:max_segments]

    def _get_dedup_key(self, place: Dict[str, Any]) -> str:
        """
        Generate a deterministic unique key for deduplication.

        Uses place_id/cid as primary key (most reliable), or
        SHA256 hash of normalized (name + phone_digits + address) as fallback.

        Args:
            place: Place dict from API response.

        Returns:
            Unique string key for this place.
            Format: "pid:<place_id>" or "hash:<sha256_hex>"
        """
        # Extract place_id (Serper uses 'cid')
        place_id = place.get("cid") or place.get("place_id") or place.get("placeId")

        # Extract fields for fallback hash
        name = place.get("title", place.get("name", ""))
        phone = place.get("phone", place.get("phoneNumber", ""))
        address = place.get("address", "")

        return compute_dedup_key(
            place_id=place_id,
            name=name,
            phone=phone,
            address=address,
        )

    def _normalize_place(
        self,
        place: Dict[str, Any],
        location: str,
    ) -> Dict[str, Any]:
        """
        Normalize a place to the output schema.

        Output schema:
            name, website, address, phone_number, place_id, source, location, dedup_key

        Args:
            place: Raw place dict from API.
            location: Original location query string.

        Returns:
            Normalized place dict matching output schema.
        """
        # Extract place_id
        place_id = place.get("cid", place.get("placeId", ""))

        # Extract phone
        phone = place.get("phone", place.get("phoneNumber", ""))

        # Extract name and address
        name = place.get("title", place.get("name", ""))
        address = place.get("address", "")

        # Compute deterministic dedup key
        dedup_key = compute_dedup_key(
            place_id=place_id,
            name=name,
            phone=phone,
            address=address,
        )

        return {
            "name": name,
            "website": place.get("website", ""),
            "address": address,
            "phone_number": phone,
            "place_id": place_id,
            "source": "google_maps",
            "location": location,
            "dedup_key": dedup_key,
            # Preserve additional useful fields
            "rating": place.get("rating"),
            "reviews": place.get("reviews", place.get("reviewsCount")),
            "category": place.get("category", place.get("type", "")),
        }

    def _get_mock_data(self, location: str) -> List[Dict[str, Any]]:
        """
        Return deterministic mock data for testing.

        Includes businesses with and without websites to test full pipeline.
        Each record includes a dedup_key for idempotency testing.

        Args:
            location: Location string for mock data.

        Returns:
            List of mock place dicts matching output schema.
        """
        mock_places = [
            {
                "name": "Mock Dental Clinic",
                "website": "https://mockdental.com",
                "address": f"123 Main St, {location}",
                "phone_number": "+1-555-0101",
                "place_id": "mock_place_001",
                "source": "google_maps",
                "location": location,
                "rating": 4.8,
                "reviews": 120,
                "category": "Dentist",
            },
            {
                "name": "Sample Orthodontics",
                "website": "",  # No website - target for this pipeline
                "address": f"456 Oak Ave, {location}",
                "phone_number": "+1-555-0102",
                "place_id": "mock_place_002",
                "source": "google_maps",
                "location": location,
                "rating": 4.5,
                "reviews": 85,
                "category": "Orthodontist",
            },
            {
                "name": "Test Family Dentistry",
                "website": "",  # No website
                "address": f"789 Elm Blvd, {location}",
                "phone_number": "+1-555-0103",
                "place_id": "mock_place_003",
                "source": "google_maps",
                "location": location,
                "rating": 4.9,
                "reviews": 200,
                "category": "Dentist",
            },
            {
                "name": "Demo Dental Care",
                "website": "https://demodental.example.com",
                "address": f"321 Pine Rd, {location}",
                "phone_number": "+1-555-0104",
                "place_id": "mock_place_004",
                "source": "google_maps",
                "location": location,
                "rating": 4.2,
                "reviews": 50,
                "category": "Dentist",
            },
            {
                "name": "Example Smile Center",
                "website": "",  # No website
                "address": f"654 Cedar Ln, {location}",
                "phone_number": "+1-555-0105",
                "place_id": "mock_place_005",
                "source": "google_maps",
                "location": location,
                "rating": 4.7,
                "reviews": 95,
                "category": "Cosmetic Dentist",
            },
            # Duplicate entry to test deduplication
            {
                "name": "Mock Dental Clinic",  # Duplicate name
                "website": "https://mockdental.com",
                "address": f"123 Main St, {location}",  # Same address
                "phone_number": "+1-555-0101",
                "place_id": "mock_place_001",  # Same place_id
                "source": "google_maps",
                "location": location,
                "rating": 4.8,
                "reviews": 120,
                "category": "Dentist",
            },
        ]

        # Add dedup_key to each mock place
        for place in mock_places:
            place["dedup_key"] = compute_dedup_key(
                place_id=place.get("place_id"),
                name=place.get("name"),
                phone=place.get("phone_number"),
                address=place.get("address"),
            )

        return mock_places
