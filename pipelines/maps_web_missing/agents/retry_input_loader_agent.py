"""
Retry Input Loader Agent for Maps No-Website Pipeline.

Loads retry candidates from the WEBSITE_CHECK_ERRORS worksheet in Google Sheets
and re-injects them into the pipeline using the SAME contract as MapsSearchAgent output.

This agent:
- Reads rows from the retry sheet (WEBSITE_CHECK_ERRORS)
- Filters out leads that have exceeded MAX_RETRIES
- Increments retry_attempt counter for each loaded lead
- Outputs `validated_businesses` for downstream processing

Contract Compliance:
    Output key: validated_businesses (same as BusinessNormalizeAgent)
    Downstream compatibility: WebsitePresenceValidator → Router → Formatter → Export

Environment Configuration:
    PIPELINE_MAX_RETRIES: Maximum retry attempts (default: 3)
    MOCK_SHEETS: Enable mock mode for testing (default: false)
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default retry sheet name (matches fan-out export RETRY route)
DEFAULT_RETRY_SHEET = "WEBSITE_CHECK_ERRORS"

# Default maximum retries before giving up
DEFAULT_MAX_RETRIES = 3

# Default credentials path (same as GoogleSheetsExportAgent)
DEFAULT_CREDENTIALS_PATH = Path("credentials/service_account.json")


def _get_max_retries() -> int:
    """
    Get MAX_RETRIES from environment with graceful fallback.

    Priority:
        1. PIPELINE_MAX_RETRIES environment variable
        2. DEFAULT_MAX_RETRIES constant (3)

    Returns:
        Maximum retry attempts as integer.
    """
    env_value = os.getenv("PIPELINE_MAX_RETRIES", "")

    if not env_value:
        return DEFAULT_MAX_RETRIES

    try:
        value = int(env_value)
        if value < 0:
            logger.warning(
                f"PIPELINE_MAX_RETRIES={env_value} is negative, using default {DEFAULT_MAX_RETRIES}"
            )
            return DEFAULT_MAX_RETRIES
        return value
    except ValueError:
        logger.warning(
            f"PIPELINE_MAX_RETRIES={env_value} is not a valid integer, "
            f"using default {DEFAULT_MAX_RETRIES}"
        )
        return DEFAULT_MAX_RETRIES


# Check if mock mode is enabled (consistent with GoogleSheetsExportAgent)
MOCK_SHEETS = os.getenv("MOCK_SHEETS", "").lower() in ("true", "1", "yes")


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class RetryStats(TypedDict):
    """Statistics from retry loading operation."""
    total_rows: int
    loaded: int
    skipped_max_retry: int
    skipped_missing_fields: int
    max_retries: int


class RetryCandidate(TypedDict, total=False):
    """Type definition for a retry candidate record."""
    # Required fields (must be in sheet)
    name: str  # Maps to business_name in sheet
    address: str
    phone: str
    website: str
    dedup_key: str

    # Retry tracking fields
    retry_attempt: int
    last_retry_ts: str

    # Source identifier
    source: str

    # Optional fields from original lead
    place_id: str
    rating: str
    reviews: str
    description: str


# =============================================================================
# REQUIRED SHEET COLUMNS
# =============================================================================

# Columns that MUST exist for a row to be valid
REQUIRED_COLUMNS = frozenset(["name", "address", "phone", "dedup_key"])

# Column that triggers soft-skip if missing (website can be empty but column must exist)
SOFT_SKIP_IF_MISSING = frozenset(["website"])


# =============================================================================
# PURE TRANSFORMATION FUNCTIONS (for testability)
# =============================================================================

def parse_retry_attempt(value: Any) -> int:
    """
    Parse retry_attempt value from sheet cell.

    Pure function for testability.

    Args:
        value: Cell value (may be str, int, float, None, or empty).

    Returns:
        Current retry_attempt as integer (0 if invalid/missing).
    """
    if value is None or value == "":
        return 0

    try:
        # Handle float strings like "1.0" from Sheets
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return 0


def parse_row_to_candidate(
    row: Dict[str, str],
    max_retries: int,
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Parse a sheet row into a retry candidate.

    Pure function - no side effects, no I/O.

    Args:
        row: Dict mapping column headers to cell values.
        max_retries: Maximum allowed retry attempts.

    Returns:
        Tuple of (candidate, skip_reason) where skip_reason is:
        - None: Successfully parsed
        - "max_retry": Exceeded max retries
        - "missing_required": Missing required field
        - "missing_dedup_key": Missing dedup_key (hard fail)
    """
    # Check for dedup_key (hard requirement)
    dedup_key = row.get("dedup_key", "").strip()
    if not dedup_key:
        return None, "missing_dedup_key"

    # Check required fields
    for field in REQUIRED_COLUMNS:
        if field == "dedup_key":
            continue  # Already checked
        value = row.get(field, "").strip()
        if not value:
            return None, "missing_required"

    # Parse and check retry_attempt
    current_attempt = parse_retry_attempt(row.get("retry_attempt"))
    new_attempt = current_attempt + 1

    if new_attempt > max_retries:
        return None, "max_retry"

    # Build candidate with all fields
    candidate: Dict[str, Any] = {
        # Required fields
        "name": row.get("name", "").strip(),
        "address": row.get("address", "").strip(),
        "phone": row.get("phone", "").strip(),
        "website": row.get("website", "").strip(),
        "dedup_key": dedup_key,

        # Retry tracking
        "retry_attempt": new_attempt,
        "last_retry_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),

        # Source identifier for tracing
        "source": "retry",
    }

    # Copy optional fields if present
    optional_fields = [
        "place_id", "rating", "reviews", "description",
        "has_real_website", "website_status", "lead_route", "target_sheet",
    ]
    for field in optional_fields:
        if field in row and row[field]:
            candidate[field] = row[field].strip() if isinstance(row[field], str) else row[field]

    return candidate, None


def transform_rows_to_candidates(
    rows: List[Dict[str, str]],
    max_retries: int,
) -> tuple[List[Dict[str, Any]], RetryStats]:
    """
    Transform sheet rows into retry candidates.

    Pure function - processes all rows and returns candidates + stats.

    Args:
        rows: List of row dicts (header -> value mapping).
        max_retries: Maximum allowed retry attempts.

    Returns:
        Tuple of (candidates list, retry stats dict).
    """
    candidates: List[Dict[str, Any]] = []
    stats: RetryStats = {
        "total_rows": len(rows),
        "loaded": 0,
        "skipped_max_retry": 0,
        "skipped_missing_fields": 0,
        "max_retries": max_retries,
    }

    for row in rows:
        candidate, skip_reason = parse_row_to_candidate(row, max_retries)

        if skip_reason == "max_retry":
            stats["skipped_max_retry"] += 1
        elif skip_reason in ("missing_required", "missing_dedup_key"):
            stats["skipped_missing_fields"] += 1
        elif candidate is not None:
            candidates.append(candidate)
            stats["loaded"] += 1

    return candidates, stats


# =============================================================================
# MOCK DATA LOADER
# =============================================================================

def _get_mock_retry_rows() -> List[Dict[str, str]]:
    """
    Generate mock retry rows for testing.

    Returns data consistent with WEBSITE_CHECK_ERRORS sheet format.

    Returns:
        List of mock row dicts.
    """
    return [
        {
            "name": "Mock Retry Business 1",
            "address": "123 Retry St",
            "phone": "555-0001",
            "website": "https://broken-link-1.com",
            "dedup_key": "pid:mock_retry_001",
            "retry_attempt": "1",
            "place_id": "mock_retry_001",
        },
        {
            "name": "Mock Retry Business 2",
            "address": "456 Error Ave",
            "phone": "555-0002",
            "website": "",  # No website
            "dedup_key": "pid:mock_retry_002",
            "retry_attempt": "0",
            "place_id": "mock_retry_002",
        },
        {
            "name": "Mock Retry Business 3 (Max Retries)",
            "address": "789 Exhausted Blvd",
            "phone": "555-0003",
            "website": "https://always-fails.com",
            "dedup_key": "pid:mock_retry_003",
            "retry_attempt": "3",  # Will be skipped at MAX_RETRIES=3
            "place_id": "mock_retry_003",
        },
    ]


# =============================================================================
# GSPREAD CLIENT (reuse pattern from GoogleSheetsExportAgent)
# =============================================================================

def _get_gspread_client(credentials_path: Path):
    """
    Initialize gspread client with service account credentials.

    Args:
        credentials_path: Path to service account JSON file.

    Returns:
        Authorized gspread client.

    Raises:
        ImportError: If gspread or google-auth not installed.
        FileNotFoundError: If credentials file not found.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise ImportError(
            "Google Sheets integration requires gspread and google-auth. "
            "Install with: pip install gspread google-auth"
        ) from e

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Service account credentials not found: {credentials_path}"
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    return gspread.authorize(creds)


# =============================================================================
# RETRY INPUT LOADER AGENT
# =============================================================================

class RetryInputLoaderAgent(BaseAgent):
    """
    Agent that loads retry candidates from Google Sheets.

    Reads rows from the WEBSITE_CHECK_ERRORS worksheet, filters retry-eligible
    leads, increments retry counters, and outputs `validated_businesses` for
    downstream pipeline processing.

    Features:
    - Configurable MAX_RETRIES via environment variable
    - Preserves original sheet row order
    - Soft-skips invalid rows with counters
    - Hard-fails on auth/spreadsheet errors
    - Mock mode support for testing

    Input Context:
        spreadsheet_id: str (REQUIRED) - Google Sheets document ID
        retry_sheet_name: str (OPTIONAL) - Worksheet name (default: WEBSITE_CHECK_ERRORS)

    Output Context:
        validated_businesses: List[dict] - Retry candidates in pipeline format
        retry_stats: dict - Loading statistics

    Contract Compliance:
        Output matches BusinessNormalizeAgent format for downstream compatibility.
    """

    def __init__(
        self,
        credentials_path: Path = DEFAULT_CREDENTIALS_PATH,
        max_retries: Optional[int] = None,
    ) -> None:
        """
        Initialize the retry input loader agent.

        Args:
            credentials_path: Path to service account JSON file.
            max_retries: Override MAX_RETRIES (default: from env or 3).
        """
        super().__init__(name="RetryInputLoaderAgent")

        self.credentials_path = credentials_path
        self.max_retries = max_retries if max_retries is not None else _get_max_retries()
        self._client = None

        logger.info(
            f"RetryInputLoaderAgent initialized "
            f"(MOCK_SHEETS: {MOCK_SHEETS}, MAX_RETRIES: {self.max_retries})"
        )

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load retry candidates from Google Sheets.

        Reads rows from the retry sheet, filters by retry eligibility,
        and outputs validated_businesses for downstream pipeline stages.

        Args:
            input_data: Dict with:
                - spreadsheet_id: str (REQUIRED) - Google Sheets ID
                - retry_sheet_name: str (OPTIONAL) - Worksheet name

        Returns:
            Dict with:
                - validated_businesses: List of retry candidate dicts
                - retry_stats: Loading statistics

        Raises:
            RuntimeError: If spreadsheet_id missing, auth fails, or sheet not found.
        """
        # =================================================================
        # VALIDATE INPUT CONTEXT
        # =================================================================

        spreadsheet_id = input_data.get("spreadsheet_id")
        if not spreadsheet_id:
            raise RuntimeError(
                "RetryInputLoaderAgent requires 'spreadsheet_id' in context. "
                "Cannot load retry candidates without a target spreadsheet."
            )

        retry_sheet_name = input_data.get("retry_sheet_name", DEFAULT_RETRY_SHEET)

        # Check for context-level mock override
        mock_mode = input_data.get("MOCK_SHEETS", MOCK_SHEETS)

        logger.info(
            f"Loading retry candidates from sheet '{retry_sheet_name}' "
            f"(spreadsheet: {spreadsheet_id[:20]}...)"
        )

        # =================================================================
        # MOCK MODE
        # =================================================================

        if mock_mode:
            logger.info("MOCK_SHEETS enabled - using mock retry data")
            rows = _get_mock_retry_rows()
            candidates, stats = transform_rows_to_candidates(rows, self.max_retries)

            logger.info(
                f"Mock retry load complete: {stats['loaded']} loaded, "
                f"{stats['skipped_max_retry']} skipped (max retries), "
                f"{stats['skipped_missing_fields']} skipped (missing fields)"
            )

            return {
                "validated_businesses": candidates,
                "retry_stats": stats,
            }

        # =================================================================
        # REAL GOOGLE SHEETS LOAD
        # =================================================================

        try:
            rows = self._load_sheet_rows(spreadsheet_id, retry_sheet_name)
        except Exception as e:
            logger.error(f"Failed to load retry sheet: {e}")
            raise RuntimeError(f"RetryInputLoaderAgent sheet load failed: {e}") from e

        # Transform rows to candidates
        candidates, stats = transform_rows_to_candidates(rows, self.max_retries)

        logger.info(
            f"Retry load complete: {stats['loaded']} loaded, "
            f"{stats['skipped_max_retry']} skipped (max retries), "
            f"{stats['skipped_missing_fields']} skipped (missing fields)"
        )

        return {
            "validated_businesses": candidates,
            "retry_stats": stats,
        }

    def _load_sheet_rows(
        self,
        spreadsheet_id: str,
        sheet_name: str,
    ) -> List[Dict[str, str]]:
        """
        Load all rows from a Google Sheets worksheet as dicts.

        Args:
            spreadsheet_id: Google Sheets document ID.
            sheet_name: Name of the worksheet to read.

        Returns:
            List of row dicts (header -> value mapping).

        Raises:
            RuntimeError: If spreadsheet or sheet not found, or auth fails.
        """
        import gspread

        # Initialize client if needed
        if self._client is None:
            self._client = _get_gspread_client(self.credentials_path)

        # Open spreadsheet
        try:
            spreadsheet = self._client.open_by_key(spreadsheet_id)
        except gspread.SpreadsheetNotFound:
            raise RuntimeError(f"Spreadsheet not found: {spreadsheet_id}")

        # Get worksheet
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            logger.warning(f"Worksheet '{sheet_name}' not found - returning empty list")
            return []

        # Get all values
        all_values = worksheet.get_all_values()

        if not all_values:
            logger.info(f"Worksheet '{sheet_name}' is empty")
            return []

        # First row is headers
        headers = [h.lower().strip() for h in all_values[0]]

        # Validate required column exists
        if "dedup_key" not in headers:
            raise RuntimeError(
                f"Worksheet '{sheet_name}' missing required 'dedup_key' column. "
                f"Available columns: {headers}"
            )

        # Convert data rows to dicts (preserve order)
        rows: List[Dict[str, str]] = []
        for row_values in all_values[1:]:
            # Skip completely empty rows
            if not any(cell.strip() for cell in row_values):
                continue

            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(row_values):
                    row_dict[header] = row_values[i]
                else:
                    row_dict[header] = ""

            rows.append(row_dict)

        logger.info(f"Loaded {len(rows)} rows from '{sheet_name}'")
        return rows


# =============================================================================
# TEST PLAN
# =============================================================================
#
# Unit Tests (tests/test_retry_input_loader.py):
#
# 1. test_parse_retry_attempt_valid_integer
#    - Input: "2" → Output: 2
#
# 2. test_parse_retry_attempt_empty_string
#    - Input: "" → Output: 0
#
# 3. test_parse_retry_attempt_none
#    - Input: None → Output: 0
#
# 4. test_parse_retry_attempt_float_string
#    - Input: "1.0" → Output: 1
#
# 5. test_parse_retry_attempt_invalid
#    - Input: "abc" → Output: 0
#
# 6. test_parse_row_to_candidate_valid
#    - Valid row with all fields → candidate dict
#
# 7. test_parse_row_to_candidate_missing_dedup_key
#    - Row without dedup_key → (None, "missing_dedup_key")
#
# 8. test_parse_row_to_candidate_missing_required_field
#    - Row without name → (None, "missing_required")
#
# 9. test_parse_row_to_candidate_exceeds_max_retries
#    - retry_attempt=3, max_retries=3 → (None, "max_retry")
#
# 10. test_parse_row_to_candidate_increments_retry
#     - retry_attempt=1 → candidate["retry_attempt"]=2
#
# 11. test_transform_rows_to_candidates_mixed
#     - Mix of valid, max_retry, missing → correct counts
#
# 12. test_agent_mock_mode
#     - MOCK_SHEETS=True → returns mock data
#
# 13. test_agent_missing_spreadsheet_id
#     - No spreadsheet_id → raises RuntimeError
#
# 14. test_agent_preserves_row_order
#     - Verify output order matches input order
#
# 15. test_get_max_retries_from_env
#     - PIPELINE_MAX_RETRIES=5 → returns 5
#
# 16. test_get_max_retries_invalid_env
#     - PIPELINE_MAX_RETRIES="abc" → returns 3 with warning
#
# Integration Tests:
#
# 17. test_real_sheets_load (requires credentials, skip in CI)
#     - Load from real WEBSITE_CHECK_ERRORS sheet
#
# 18. test_output_contract_compatibility
#     - Verify validated_businesses format matches BusinessNormalizeAgent
#
