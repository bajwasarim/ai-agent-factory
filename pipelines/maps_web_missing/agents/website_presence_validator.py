"""
Website Presence Validator Agent for Maps No-Website Pipeline.

Validates whether businesses have real websites vs placeholder/social media links.
Classifies businesses for downstream targeting (landing-page outreach).

Integration Position:
    BusinessNormalizeAgent
            ↓
    WebsitePresenceValidator   ← THIS AGENT
            ↓
    LeadFormatterAgent
            ↓
    ExporterAgent
"""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)

# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# Mock mode for CI testing
MOCK_WEBSITE_CHECK = os.getenv("MOCK_WEBSITE_CHECK", "").lower() in ("true", "1", "yes")

# Request configuration
REQUEST_TIMEOUT_SECONDS = 5
MAX_REDIRECTS = 3
MAX_RETRIES = 2
RATE_LIMIT_RPS = 5  # Max requests per second
RATE_LIMIT_DELAY = 1.0 / RATE_LIMIT_RPS

# Blacklisted domains - NOT considered real business websites
WEBSITE_BLACKLIST = frozenset([
    "facebook.com",
    "www.facebook.com",
    "m.facebook.com",
    "yelp.com",
    "www.yelp.com",
    "google.com",
    "www.google.com",
    "maps.google.com",
    "instagram.com",
    "www.instagram.com",
    "booking.com",
    "www.booking.com",
    "tripadvisor.com",
    "www.tripadvisor.com",
    "linktr.ee",
    "www.linktr.ee",
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
    "linkedin.com",
    "www.linkedin.com",
    "tiktok.com",
    "www.tiktok.com",
    "pinterest.com",
    "www.pinterest.com",
    "yellowpages.com",
    "www.yellowpages.com",
])

# Valid content types for real websites
VALID_CONTENT_TYPES = frozenset([
    "text/html",
    "application/xhtml+xml",
])

# Website status constants
STATUS_VALID = "valid"
STATUS_INVALID = "invalid"
STATUS_MISSING = "missing"
STATUS_ERROR = "error"


class WebsitePresenceValidator(BaseAgent):
    """
    Agent that validates whether businesses have real websites.

    Filters out:
    - Social media pages (Facebook, Instagram, etc.)
    - Booking platforms (Yelp, TripAdvisor, Booking.com)
    - Placeholder/dead domains
    - Redirects to blacklisted sites

    Features:
    - HTTP HEAD/GET validation with timeout
    - Domain blacklist filtering
    - Rate limiting (5 req/sec)
    - Retry policy (2 retries on timeout)
    - Mock mode for CI testing

    Input: normalized_businesses (list of business dicts)
    Output: validated_businesses (list with appended validation fields)
    """

    def __init__(self) -> None:
        """Initialize the website presence validator."""
        super().__init__(name="WebsitePresenceValidator")
        self._session: Optional[requests.Session] = None
        self._last_request_time: float = 0.0

        logger.info(
            f"WebsitePresenceValidator initialized "
            f"(MOCK_WEBSITE_CHECK: {MOCK_WEBSITE_CHECK})"
        )

    def _get_session(self) -> requests.Session:
        """
        Get or create a requests session with retry configuration.

        Returns:
            Configured requests.Session with retry policy.
        """
        if self._session is None:
            self._session = requests.Session()

            # Configure retry policy
            retry_strategy = Retry(
                total=MAX_RETRIES,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET"],
            )

            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=10,
                pool_maxsize=10,
            )

            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)

            # Set default headers
            self._session.headers.update({
                "User-Agent": "Mozilla/5.0 (compatible; WebsiteValidator/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            })

        return self._session

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate website presence for all normalized businesses.

        Args:
            input_data: Dict with 'normalized_businesses' or 'validated_businesses' list.
                        (supports both normal ingestion and retry mode)

        Returns:
            Dict with 'validated_businesses' list containing appended fields:
                - has_real_website: bool
                - website_status: "valid" | "invalid" | "missing" | "error"
                - website_checked_at: ISO timestamp
        """
        # Support both normal mode (normalized_businesses) and retry mode (validated_businesses)
        businesses = input_data.get("normalized_businesses") or input_data.get("validated_businesses", [])

        if not businesses:
            logger.warning("No businesses to validate")
            return {"validated_businesses": []}

        # Statistics tracking
        stats = {
            "total": len(businesses),
            "valid": 0,
            "invalid": 0,
            "missing": 0,
            "error": 0,
            "response_times": [],
        }

        validated = []
        check_timestamp = datetime.now(timezone.utc).isoformat()

        for idx, business in enumerate(businesses):
            website = business.get("website", "").strip()

            # Validate and append fields
            result = self._validate_website(website, stats)

            # Create new dict with appended fields (preserve all existing)
            validated_business = {
                **business,
                "has_real_website": result["has_real_website"],
                "website_status": result["status"],
                "website_checked_at": check_timestamp,
            }

            validated.append(validated_business)

            logger.debug(
                f"[{idx + 1}/{len(businesses)}] {business.get('name', 'Unknown')}: "
                f"website_status={result['status']}"
            )

        # Log summary statistics
        avg_response_time = (
            sum(stats["response_times"]) / len(stats["response_times"])
            if stats["response_times"]
            else 0.0
        )

        logger.info(
            f"Website validation complete: "
            f"{stats['total']} checked, "
            f"{stats['valid']} valid, "
            f"{stats['invalid']} invalid, "
            f"{stats['missing']} missing, "
            f"{stats['error']} errors, "
            f"avg_response_time={avg_response_time:.2f}s"
        )

        return {"validated_businesses": validated}

    def _validate_website(
        self, website: str, stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate a single website URL.

        Args:
            website: URL string (may be empty).
            stats: Statistics dict to update.

        Returns:
            Dict with 'has_real_website' and 'status' keys.
        """
        # Case 1: No website provided
        if not website:
            stats["missing"] += 1
            return {
                "has_real_website": False,
                "status": STATUS_MISSING,
            }

        # Mock mode for CI testing
        if MOCK_WEBSITE_CHECK:
            return self._mock_validate(website, stats)

        # Live validation
        return self._live_validate(website, stats)

    def _mock_validate(
        self, website: str, stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Mock validation for CI testing.

        Deterministic rules:
        - Domains containing "example" → valid
        - Domains containing "facebook", "yelp", etc. → invalid
        - Otherwise → valid (simulates real site)

        Args:
            website: URL string.
            stats: Statistics dict to update.

        Returns:
            Dict with validation result.
        """
        website_lower = website.lower()

        # Check blacklist patterns
        for blacklisted in WEBSITE_BLACKLIST:
            if blacklisted in website_lower:
                stats["invalid"] += 1
                logger.debug(f"[MOCK] Invalid (blacklisted): {website}")
                return {
                    "has_real_website": False,
                    "status": STATUS_INVALID,
                }

        # "example" domains are valid in mock mode
        if "example" in website_lower:
            stats["valid"] += 1
            logger.debug(f"[MOCK] Valid (example domain): {website}")
            return {
                "has_real_website": True,
                "status": STATUS_VALID,
            }

        # Default: treat as valid real website
        stats["valid"] += 1
        stats["response_times"].append(0.05)  # Mock response time
        logger.debug(f"[MOCK] Valid (default): {website}")
        return {
            "has_real_website": True,
            "status": STATUS_VALID,
        }

    def _live_validate(
        self, website: str, stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Live HTTP validation of website.

        Steps:
        1. Parse and validate URL
        2. Check domain against blacklist
        3. Make HTTP HEAD request (fallback to GET)
        4. Validate status code and content type
        5. Check final redirect destination

        Args:
            website: URL string.
            stats: Statistics dict to update.

        Returns:
            Dict with validation result.
        """
        # Normalize URL
        url = self._normalize_url(website)
        if not url:
            stats["error"] += 1
            logger.debug(f"Invalid URL format: {website}")
            return {
                "has_real_website": False,
                "status": STATUS_ERROR,
            }

        # Pre-check: domain blacklist
        domain = self._extract_domain(url)
        if domain and self._is_blacklisted(domain):
            stats["invalid"] += 1
            logger.debug(f"Blacklisted domain: {domain}")
            return {
                "has_real_website": False,
                "status": STATUS_INVALID,
            }

        # Apply rate limiting
        self._rate_limit()

        # Make HTTP request
        start_time = time.time()
        try:
            response = self._make_request(url)
            elapsed = time.time() - start_time
            stats["response_times"].append(elapsed)

            # Check response
            return self._evaluate_response(response, stats)

        except requests.exceptions.Timeout:
            stats["error"] += 1
            logger.debug(f"Timeout: {url}")
            return {
                "has_real_website": False,
                "status": STATUS_ERROR,
            }

        except requests.exceptions.TooManyRedirects:
            stats["invalid"] += 1
            logger.debug(f"Too many redirects: {url}")
            return {
                "has_real_website": False,
                "status": STATUS_INVALID,
            }

        except requests.exceptions.SSLError:
            stats["error"] += 1
            logger.debug(f"SSL error: {url}")
            return {
                "has_real_website": False,
                "status": STATUS_ERROR,
            }

        except requests.exceptions.ConnectionError:
            stats["invalid"] += 1
            logger.debug(f"Connection error (DNS/network): {url}")
            return {
                "has_real_website": False,
                "status": STATUS_INVALID,
            }

        except requests.exceptions.RequestException as e:
            stats["error"] += 1
            logger.debug(f"Request error: {url} - {e}")
            return {
                "has_real_website": False,
                "status": STATUS_ERROR,
            }

    def _normalize_url(self, website: str) -> Optional[str]:
        """
        Normalize URL to include scheme.

        Args:
            website: Raw URL string.

        Returns:
            Normalized URL with scheme, or None if invalid.
        """
        website = website.strip()

        if not website:
            return None

        # Add scheme if missing
        if not website.startswith(("http://", "https://")):
            website = f"https://{website}"

        # Validate URL structure
        try:
            parsed = urlparse(website)
            if not parsed.netloc:
                return None
            return website
        except Exception:
            return None

    def _extract_domain(self, url: str) -> Optional[str]:
        """
        Extract domain from URL.

        Args:
            url: Full URL string.

        Returns:
            Domain string (e.g., "example.com").
        """
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return None

    def _is_blacklisted(self, domain: str) -> bool:
        """
        Check if domain is in the blacklist.

        Also checks if domain ends with a blacklisted domain
        (e.g., "business.facebook.com" matches "facebook.com").

        Args:
            domain: Domain string.

        Returns:
            True if blacklisted.
        """
        domain = domain.lower()

        # Direct match
        if domain in WEBSITE_BLACKLIST:
            return True

        # Subdomain match (e.g., business.facebook.com)
        for blacklisted in WEBSITE_BLACKLIST:
            if domain.endswith(f".{blacklisted}"):
                return True

        return False

    def _make_request(self, url: str) -> requests.Response:
        """
        Make HTTP request with HEAD fallback to GET.

        Args:
            url: Validated URL string.

        Returns:
            requests.Response object.

        Raises:
            requests.exceptions.RequestException on failure.
        """
        session = self._get_session()

        # Try HEAD first (faster, less bandwidth)
        try:
            response = session.head(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                allow_redirects=True,
            )

            # Some servers don't support HEAD, fall back to GET
            if response.status_code == 405:
                response = session.get(
                    url,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    allow_redirects=True,
                    stream=True,  # Don't download full body
                )

            return response

        except requests.exceptions.RequestException:
            # HEAD might be blocked, try GET
            return session.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                allow_redirects=True,
                stream=True,
            )

    def _evaluate_response(
        self, response: requests.Response, stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Evaluate HTTP response to determine website validity.

        Checks:
        1. Status code (200-399 = valid)
        2. Final URL not blacklisted (after redirects)
        3. Content-Type is HTML

        Args:
            response: HTTP response object.
            stats: Statistics dict to update.

        Returns:
            Dict with validation result.
        """
        # Check status code
        if response.status_code >= 400:
            stats["invalid"] += 1
            logger.debug(
                f"Invalid status {response.status_code}: {response.url}"
            )
            return {
                "has_real_website": False,
                "status": STATUS_INVALID,
            }

        # Check final URL after redirects
        final_domain = self._extract_domain(response.url)
        if final_domain and self._is_blacklisted(final_domain):
            stats["invalid"] += 1
            logger.debug(f"Redirected to blacklisted: {response.url}")
            return {
                "has_real_website": False,
                "status": STATUS_INVALID,
            }

        # Check content type
        content_type = response.headers.get("Content-Type", "").lower()
        is_valid_content = any(
            valid_type in content_type for valid_type in VALID_CONTENT_TYPES
        )

        if not is_valid_content and content_type:
            # Only mark invalid if content-type is present but wrong
            # Some servers don't return content-type for HEAD requests
            if response.request.method != "HEAD":
                stats["invalid"] += 1
                logger.debug(
                    f"Invalid content-type '{content_type}': {response.url}"
                )
                return {
                    "has_real_website": False,
                    "status": STATUS_INVALID,
                }

        # Valid website!
        stats["valid"] += 1
        logger.debug(f"Valid website: {response.url}")
        return {
            "has_real_website": True,
            "status": STATUS_VALID,
        }

    def __del__(self):
        """Cleanup session on destruction."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
