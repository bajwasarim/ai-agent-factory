"""Google Sheets export agent for Maps No-Website Pipeline.

Implements fan-out export by route (TARGET/EXCLUDED/RETRY) with:
- Per-sheet idempotency (independent dedup per worksheet)
- 3-phase atomic export: Preflight → Write → Backup
- Batch-safe writes (MAX_BATCH_SIZE = 200)
- Structured export stats per route
"""

import json
import csv
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TypedDict

from pipelines.core.base_agent import BaseAgent
from pipelines.maps_web_missing.config import EXPORT_PATH
from pipelines.maps_web_missing.utils.helpers import (
    ensure_export_dir,
    sanitize_filename,
    get_timestamp,
    format_phone_for_sheets,
    compute_dedup_key,
)
from core.logger import get_logger

logger = get_logger(__name__)

# Check if mock mode is enabled
MOCK_SHEETS = os.getenv("MOCK_SHEETS", "").lower() in ("true", "1", "yes")

# Default paths
DEFAULT_CREDENTIALS_PATH = Path("credentials/service_account.json")

# =============================================================================
# FAN-OUT CONSTANTS
# =============================================================================

# Maximum rows per batch append (Google Sheets API best practice)
MAX_BATCH_SIZE = 200

# Deterministic export order for atomic writes
SHEET_EXPORT_ORDER = ["NO_WEBSITE_TARGETS", "HAS_WEBSITE_EXCLUDED", "WEBSITE_CHECK_ERRORS"]

# Route to sheet mapping (for backward compatibility checks)
ROUTE_TO_SHEET = {
    "TARGET": "NO_WEBSITE_TARGETS",
    "EXCLUDED": "HAS_WEBSITE_EXCLUDED",
    "RETRY": "WEBSITE_CHECK_ERRORS",
}


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class SheetExportStats(TypedDict):
    """Export statistics for a single worksheet."""
    exported: int
    skipped: int
    sheet_name: str


class FanOutResult(TypedDict):
    """Result of fan-out export operation."""
    TARGET: SheetExportStats
    EXCLUDED: SheetExportStats
    RETRY: SheetExportStats
    total_exported: int
    total_skipped: int
    sheet_url: Optional[str]


class PreflightData(TypedDict):
    """Data gathered during preflight phase."""
    spreadsheet: Any  # gspread.Spreadsheet
    worksheets: Dict[str, Any]  # sheet_name -> gspread.Worksheet
    existing_dedup_keys: Dict[str, Set[str]]  # sheet_name -> set of dedup keys
    headers: List[str]


# =============================================================================
# GSPREAD CLIENT
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
# GOOGLE SHEETS EXPORT AGENT
# =============================================================================

class GoogleSheetsExportAgent(BaseAgent):
    """
    Agent that exports formatted leads to Google Sheets with fan-out by route.

    Features:
    - Fan-out export: Routes leads to separate worksheets by target_sheet field
    - Per-sheet idempotency: Independent dedup per worksheet (not global)
    - 3-phase atomic export: Preflight → Write → Backup
    - Batch-safe writes: MAX_BATCH_SIZE = 200 rows per API call
    - Backward compatible: Same input/output contract

    Export Order (deterministic):
        1. NO_WEBSITE_TARGETS (TARGET route)
        2. HAS_WEBSITE_EXCLUDED (EXCLUDED route)
        3. WEBSITE_CHECK_ERRORS (RETRY route)

    Input: formatted_leads, summary, query, location, spreadsheet_id (optional)
    Output: export_status with per-sheet stats, URLs, and optional file paths
    """

    def __init__(
        self,
        credentials_path: Path = DEFAULT_CREDENTIALS_PATH,
        export_path: Path = EXPORT_PATH,
        enable_file_backup: bool = True,
    ) -> None:
        """
        Initialize the Google Sheets export agent.

        Args:
            credentials_path: Path to service account JSON file.
            export_path: Directory path for file backups.
            enable_file_backup: Whether to also export to JSON/CSV files.
        """
        super().__init__(name="GoogleSheetsExportAgent")
        self.credentials_path = credentials_path
        self.export_path = export_path
        self.enable_file_backup = enable_file_backup
        self._client = None

        logger.info(
            f"GoogleSheetsExportAgent initialized "
            f"(MOCK_SHEETS: {MOCK_SHEETS}, file_backup: {enable_file_backup})"
        )

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Export formatted leads to Google Sheets with fan-out by route.

        3-Phase Atomic Export:
            Phase 1 (Preflight): Validate sheets, load dedup keys - NO WRITES
            Phase 2 (Write): Batch append to sheets in order - abort on failure
            Phase 3 (Backup): Write local files only after ALL sheets succeed

        Args:
            input_data: Dict with:
                - formatted_leads: List of lead dicts with target_sheet field
                - summary: Summary dict from formatter
                - query: Search query string
                - location: Location string
                - spreadsheet_id: Optional Google Sheets ID
                - sheet_name: Optional worksheet name prefix (legacy)

        Returns:
            Dict with 'export_status' containing:
                - total_leads: Total leads processed
                - total_exported: Total new leads exported across all sheets
                - total_skipped: Total duplicates skipped across all sheets
                - per_sheet_stats: Dict with stats per target_sheet
                - sheet_url: URL to the Google Sheet
                - json_path: Path to JSON backup (if enabled)
                - csv_path: Path to CSV backup (if enabled)
        """
        leads = input_data.get("formatted_leads", [])
        summary = input_data.get("summary", {})
        query = input_data.get("query", "export")
        location = input_data.get("location", "")
        spreadsheet_id = input_data.get("spreadsheet_id")

        # Initialize export status
        export_status = {
            "total_leads": len(leads),
            "total_exported": 0,
            "total_skipped": 0,
            "per_sheet_stats": {},
            "sheet_url": None,
            "json_path": None,
            "csv_path": None,
        }

        if not leads:
            logger.info("No leads to export")
            return {"export_status": export_status, "exported_leads": []}

        # =====================================================================
        # PHASE 0: Validate contract fields and partition leads
        # =====================================================================
        try:
            partitioned = self._partition_leads_by_sheet(leads)
        except ValueError as e:
            logger.error(f"Contract violation during partitioning: {e}")
            raise

        # Log partition summary
        for sheet_name, sheet_leads in partitioned.items():
            logger.info(f"Partitioned {len(sheet_leads)} leads for sheet '{sheet_name}'")

        # =====================================================================
        # MOCK MODE or NO SPREADSHEET_ID
        # =====================================================================
        if MOCK_SHEETS or not spreadsheet_id:
            if not spreadsheet_id:
                logger.warning("No spreadsheet_id provided, skipping Google Sheets export")
            else:
                logger.info("MOCK_SHEETS enabled - simulating Google Sheets export")

            # Simulate fan-out stats
            per_sheet_stats = {}
            total_exported = 0
            for sheet_name, sheet_leads in partitioned.items():
                per_sheet_stats[sheet_name] = {
                    "exported": len(sheet_leads),
                    "skipped": 0,
                    "sheet_name": sheet_name,
                }
                total_exported += len(sheet_leads)

            export_status["total_exported"] = total_exported
            export_status["total_skipped"] = 0
            export_status["per_sheet_stats"] = per_sheet_stats
            export_status["sheet_url"] = "https://docs.google.com/spreadsheets/d/MOCK_ID/edit#gid=0"

            # File backup in mock mode (still atomic - only after "success")
            if self.enable_file_backup:
                file_result = self._export_to_files(leads, summary, query, location)
                export_status["json_path"] = file_result.get("json_path")
                export_status["csv_path"] = file_result.get("csv_path")

            self._log_export_summary(export_status)
            
            # Output exported_leads for downstream agents (e.g., LandingPageGeneratorAgent)
            return {"export_status": export_status, "exported_leads": leads}

        # =====================================================================
        # REAL GOOGLE SHEETS EXPORT (3-PHASE ATOMIC)
        # =====================================================================
        try:
            fanout_result = self._fanout_export_to_sheets(
                partitioned_leads=partitioned,
                spreadsheet_id=spreadsheet_id,
            )

            export_status["total_exported"] = fanout_result["total_exported"]
            export_status["total_skipped"] = fanout_result["total_skipped"]
            export_status["per_sheet_stats"] = {
                "TARGET": fanout_result["TARGET"],
                "EXCLUDED": fanout_result["EXCLUDED"],
                "RETRY": fanout_result["RETRY"],
            }
            export_status["sheet_url"] = fanout_result["sheet_url"]

        except Exception as e:
            # CRITICAL: Do NOT write backups if sheets export failed
            logger.error(f"Google Sheets fan-out export FAILED: {e}")
            logger.error("Backup files will NOT be written (atomic export policy)")
            export_status["error"] = str(e)
            raise RuntimeError(f"Fan-out export failed: {e}") from e

        # =====================================================================
        # PHASE 3: Backup Commit (only after ALL sheets succeed)
        # =====================================================================
        if self.enable_file_backup:
            logger.info("Phase 3: Committing backup files (all sheets exported successfully)")
            file_result = self._export_to_files(leads, summary, query, location)
            export_status["json_path"] = file_result.get("json_path")
            export_status["csv_path"] = file_result.get("csv_path")
            logger.info(f"Backup commit SUCCESS: JSON={file_result.get('json_path')}")

        self._log_export_summary(export_status)
        
        # Output exported_leads for downstream agents (e.g., LandingPageGeneratorAgent)
        return {"export_status": export_status, "exported_leads": leads}

    # =========================================================================
    # FAN-OUT PARTITIONING
    # =========================================================================

    def _partition_leads_by_sheet(
        self, leads: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Partition leads into buckets by target_sheet field.

        Fail-fast validation: Raises if any lead is missing required fields.

        Args:
            leads: List of formatted lead dicts.

        Returns:
            Dict mapping sheet_name to list of leads for that sheet.

        Raises:
            ValueError: If any lead is missing target_sheet or dedup_key.
        """
        partitioned: Dict[str, List[Dict[str, Any]]] = {
            "NO_WEBSITE_TARGETS": [],
            "HAS_WEBSITE_EXCLUDED": [],
            "WEBSITE_CHECK_ERRORS": [],
        }

        for idx, lead in enumerate(leads):
            # Validate required contract fields
            target_sheet = lead.get("target_sheet")
            dedup_key = lead.get("dedup_key")

            if not target_sheet:
                raise ValueError(
                    f"Exporter contract violation: target_sheet missing. "
                    f"Lead index={idx}, place_id={lead.get('place_id', 'unknown')}"
                )

            if not dedup_key:
                raise ValueError(
                    f"Exporter contract violation: dedup_key missing. "
                    f"Lead index={idx}, place_id={lead.get('place_id', 'unknown')}"
                )

            # Route to appropriate bucket
            if target_sheet not in partitioned:
                logger.warning(
                    f"Unknown target_sheet '{target_sheet}' for lead {idx}, "
                    f"routing to WEBSITE_CHECK_ERRORS"
                )
                partitioned["WEBSITE_CHECK_ERRORS"].append(lead)
            else:
                partitioned[target_sheet].append(lead)

        return partitioned

    # =========================================================================
    # 3-PHASE ATOMIC EXPORT
    # =========================================================================

    def _fanout_export_to_sheets(
        self,
        partitioned_leads: Dict[str, List[Dict[str, Any]]],
        spreadsheet_id: str,
    ) -> FanOutResult:
        """
        Execute 3-phase atomic fan-out export to Google Sheets.

        Phase 1 (Preflight): Validate spreadsheet, create/validate worksheets,
                            load existing dedup keys. NO WRITES.

        Phase 2 (Write): Sequentially process sheets in order:
                        TARGET → EXCLUDED → RETRY
                        Batch append rows (200 max per batch).
                        Abort on ANY failure.

        Phase 3: Handled by caller (backup commit).

        Args:
            partitioned_leads: Dict mapping sheet_name to leads.
            spreadsheet_id: Google Sheets document ID.

        Returns:
            FanOutResult with per-sheet stats.

        Raises:
            RuntimeError: If any phase fails.
        """
        import gspread

        # Initialize client if needed
        if self._client is None:
            self._client = _get_gspread_client(self.credentials_path)

        # =====================================================================
        # PHASE 1: PREFLIGHT (NO WRITES)
        # =====================================================================
        logger.info("Phase 1: Preflight - validating sheets and loading dedup keys")

        try:
            preflight = self._preflight_sheets(
                spreadsheet_id=spreadsheet_id,
                sheet_names=SHEET_EXPORT_ORDER,
                sample_lead=self._get_sample_lead(partitioned_leads),
            )
        except gspread.SpreadsheetNotFound:
            raise RuntimeError(f"Spreadsheet not found: {spreadsheet_id}")
        except Exception as e:
            raise RuntimeError(f"Preflight failed: {e}") from e

        logger.info(f"Preflight complete: {len(preflight['worksheets'])} worksheets ready")

        # =====================================================================
        # PHASE 2: WRITE PHASE (SEQUENTIAL, ABORT ON FAILURE)
        # =====================================================================
        logger.info("Phase 2: Write - exporting to sheets in order TARGET → EXCLUDED → RETRY")

        result: FanOutResult = {
            "TARGET": {"exported": 0, "skipped": 0, "sheet_name": "NO_WEBSITE_TARGETS"},
            "EXCLUDED": {"exported": 0, "skipped": 0, "sheet_name": "HAS_WEBSITE_EXCLUDED"},
            "RETRY": {"exported": 0, "skipped": 0, "sheet_name": "WEBSITE_CHECK_ERRORS"},
            "total_exported": 0,
            "total_skipped": 0,
            "sheet_url": preflight["spreadsheet"].url,
        }

        # Map sheet names to route keys
        sheet_to_route = {v: k for k, v in ROUTE_TO_SHEET.items()}

        for sheet_name in SHEET_EXPORT_ORDER:
            leads_for_sheet = partitioned_leads.get(sheet_name, [])
            route_key = sheet_to_route.get(sheet_name, "RETRY")

            if not leads_for_sheet:
                logger.info(f"  [{route_key}] No leads for '{sheet_name}', skipping")
                continue

            try:
                sheet_result = self._write_sheet_batch(
                    worksheet=preflight["worksheets"][sheet_name],
                    leads=leads_for_sheet,
                    headers=preflight["headers"],
                    existing_dedup_keys=preflight["existing_dedup_keys"][sheet_name],
                    sheet_name=sheet_name,
                )

                result[route_key] = {
                    "exported": sheet_result["exported"],
                    "skipped": sheet_result["skipped"],
                    "sheet_name": sheet_name,
                }
                result["total_exported"] += sheet_result["exported"]
                result["total_skipped"] += sheet_result["skipped"]

                logger.info(
                    f"  [{route_key}] '{sheet_name}': "
                    f"{sheet_result['exported']} exported, {sheet_result['skipped']} skipped"
                )

            except Exception as e:
                # ABORT: Do not continue to remaining sheets
                logger.error(f"  [{route_key}] FAILED writing to '{sheet_name}': {e}")
                logger.error("ABORTING remaining sheets (atomic export policy)")
                raise RuntimeError(
                    f"Write phase failed on sheet '{sheet_name}': {e}"
                ) from e

        logger.info(
            f"Phase 2 complete: {result['total_exported']} exported, "
            f"{result['total_skipped']} skipped across all sheets"
        )

        return result

    def _preflight_sheets(
        self,
        spreadsheet_id: str,
        sheet_names: List[str],
        sample_lead: Optional[Dict[str, Any]],
    ) -> PreflightData:
        """
        Phase 1: Preflight validation and data loading.

        - Validates spreadsheet exists
        - Creates/validates worksheets for each target sheet
        - Loads existing dedup keys per sheet

        NO WRITES in this phase (except worksheet creation if needed).

        Args:
            spreadsheet_id: Google Sheets document ID.
            sheet_names: List of worksheet names to prepare.
            sample_lead: Sample lead for header extraction.

        Returns:
            PreflightData with spreadsheet, worksheets, and dedup keys.
        """
        import gspread

        # Open spreadsheet
        spreadsheet = self._client.open_by_key(spreadsheet_id)
        logger.info(f"  Spreadsheet validated: {spreadsheet.title}")

        # Determine headers from sample lead
        if sample_lead:
            headers = list(sample_lead.keys())
        else:
            headers = self._get_default_headers()

        # Prepare worksheets and load dedup keys
        worksheets: Dict[str, Any] = {}
        existing_dedup_keys: Dict[str, Set[str]] = {}

        for sheet_name in sheet_names:
            # Get or create worksheet
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                logger.info(f"  Worksheet '{sheet_name}': exists")
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=1000,
                    cols=len(headers) + 5,
                )
                logger.info(f"  Worksheet '{sheet_name}': created")

            worksheets[sheet_name] = worksheet

            # Load existing data and compute dedup keys
            current_values = worksheet.get_all_values()
            current_values = [row for row in current_values if any(cell.strip() for cell in row)]

            # Ensure headers exist
            self._ensure_headers(worksheet, current_values, headers, sheet_name)

            # Re-fetch after potential header insert
            if not current_values or current_values[0] != headers:
                current_values = worksheet.get_all_values()
                current_values = [row for row in current_values if any(cell.strip() for cell in row)]

            # Compute existing dedup keys for this sheet
            dedup_keys = self._compute_existing_hashes(current_values, headers)
            existing_dedup_keys[sheet_name] = dedup_keys
            logger.info(f"  Worksheet '{sheet_name}': {len(dedup_keys)} existing dedup keys loaded")

        return {
            "spreadsheet": spreadsheet,
            "worksheets": worksheets,
            "existing_dedup_keys": existing_dedup_keys,
            "headers": headers,
        }

    def _write_sheet_batch(
        self,
        worksheet,
        leads: List[Dict[str, Any]],
        headers: List[str],
        existing_dedup_keys: Set[str],
        sheet_name: str,
    ) -> Dict[str, int]:
        """
        Write leads to a single worksheet with batch-safe appends.

        - Filters duplicates using per-sheet dedup keys
        - Batches writes in chunks of MAX_BATCH_SIZE
        - Returns export stats

        Args:
            worksheet: gspread Worksheet object.
            leads: Leads to write to this sheet.
            headers: Column headers.
            existing_dedup_keys: Set of dedup keys already in this sheet.
            sheet_name: Name of the sheet (for logging).

        Returns:
            Dict with 'exported' and 'skipped' counts.
        """
        # Filter duplicates and prepare rows
        new_rows = []
        skipped = 0
        local_dedup_keys = set(existing_dedup_keys)  # Copy to track within batch

        for lead in leads:
            dedup_key = lead.get("dedup_key")

            if dedup_key in local_dedup_keys:
                skipped += 1
                continue

            row = self._format_row_for_sheets(lead, headers)
            new_rows.append(row)
            local_dedup_keys.add(dedup_key)

        # Batch append
        if new_rows:
            # Split into batches
            for i in range(0, len(new_rows), MAX_BATCH_SIZE):
                batch = new_rows[i:i + MAX_BATCH_SIZE]
                worksheet.append_rows(batch, value_input_option="USER_ENTERED")
                logger.debug(
                    f"    Batch appended {len(batch)} rows to '{sheet_name}' "
                    f"(batch {i // MAX_BATCH_SIZE + 1})"
                )

        return {
            "exported": len(new_rows),
            "skipped": skipped,
        }

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _get_sample_lead(
        self, partitioned_leads: Dict[str, List[Dict[str, Any]]]
    ) -> Optional[Dict[str, Any]]:
        """Get a sample lead for header extraction."""
        for sheet_name in SHEET_EXPORT_ORDER:
            leads = partitioned_leads.get(sheet_name, [])
            if leads:
                return leads[0]
        return None

    def _get_default_headers(self) -> List[str]:
        """Return default headers if no sample lead available."""
        return [
            "rank", "name", "website", "description", "source", "location",
            "phone", "rating", "reviews", "address", "place_id", "dedup_key",
            "has_website", "has_real_website", "website_status", "website_checked_at",
            "lead_route", "target_sheet",
        ]

    def _ensure_headers(
        self,
        worksheet,
        current_values: List[List[str]],
        headers: List[str],
        sheet_name: str,
    ) -> None:
        """Ensure worksheet has correct headers."""
        if not current_values:
            # Empty sheet - add headers
            worksheet.append_row(headers, value_input_option="USER_ENTERED")
            logger.debug(f"    Added header row to '{sheet_name}'")
        else:
            # Check if first row matches expected headers
            first_row_lower = [str(c).lower().strip() for c in current_values[0]]
            expected_lower = [h.lower() for h in headers]

            if first_row_lower != expected_lower:
                # Headers don't match - insert at top
                worksheet.insert_row(headers, index=1, value_input_option="USER_ENTERED")
                logger.debug(f"    Inserted header row at top of '{sheet_name}'")

    def _compute_existing_hashes(
        self, all_values: List[List[str]], headers: List[str]
    ) -> Set[str]:
        """
        Compute dedup keys of existing rows for idempotency checking.

        Uses the dedup_key column if present, otherwise falls back to
        computing from place_id/name/phone/address.

        Args:
            all_values: All values from worksheet (including header row).
            headers: Expected header list for column mapping.

        Returns:
            Set of dedup key strings for existing data rows.
        """
        dedup_keys: Set[str] = set()

        if len(all_values) <= 1:
            return dedup_keys

        # Get header indices from actual sheet headers
        actual_headers = [h.lower().strip() for h in all_values[0]] if all_values else []

        # Find indices
        dedup_key_idx = None
        place_id_idx = None
        name_idx = None
        phone_idx = None
        address_idx = None

        for idx, header in enumerate(actual_headers):
            if header == "dedup_key":
                dedup_key_idx = idx
            elif header == "place_id":
                place_id_idx = idx
            elif header == "name":
                name_idx = idx
            elif header == "phone":
                phone_idx = idx
            elif header == "address":
                address_idx = idx

        # Process data rows
        for row in all_values[1:]:
            # Prefer explicit dedup_key column
            if dedup_key_idx is not None and dedup_key_idx < len(row):
                key = row[dedup_key_idx].strip()
                if key:
                    dedup_keys.add(key)
                    continue

            # Fallback: compute from fields
            place_id = row[place_id_idx] if place_id_idx is not None and place_id_idx < len(row) else ""
            name = row[name_idx] if name_idx is not None and name_idx < len(row) else ""
            phone = row[phone_idx] if phone_idx is not None and phone_idx < len(row) else ""
            address = row[address_idx] if address_idx is not None and address_idx < len(row) else ""

            # Strip leading apostrophe from phone (Sheets formatting)
            if phone.startswith("'"):
                phone = phone[1:]

            dedup_key = compute_dedup_key(
                place_id=place_id,
                name=name,
                phone=phone,
                address=address,
            )
            dedup_keys.add(dedup_key)

        return dedup_keys

    def _format_row_for_sheets(
        self, lead: Dict[str, Any], headers: List[str]
    ) -> List[str]:
        """
        Format a lead dict as a row for Google Sheets.

        Applies special formatting:
        - Phone numbers: Prepend apostrophe to prevent formula interpretation

        Args:
            lead: Lead dict.
            headers: Column headers in order.

        Returns:
            List of string values in header order.
        """
        row = []
        for h in headers:
            value = lead.get(h, "")

            # Apply phone formatting for Sheets
            if h.lower() == "phone":
                value = format_phone_for_sheets(value)
            else:
                value = str(value) if value is not None else ""

            row.append(value)

        return row

    def _export_to_files(
        self,
        leads: List[Dict[str, Any]],
        summary: Dict[str, Any],
        query: str,
        location: str,
    ) -> Dict[str, str]:
        """
        Export leads to JSON and CSV files as backup.

        Args:
            leads: List of lead dicts.
            summary: Summary dict.
            query: Search query.
            location: Location string.

        Returns:
            Dict with json_path and csv_path.
        """
        ensure_export_dir(self.export_path)

        # Generate filename
        timestamp = get_timestamp()
        run_id = uuid.uuid4().hex[:8]
        location_safe = sanitize_filename(location) if location else "unknown"
        query_safe = sanitize_filename(query)
        filename_base = f"{query_safe}_{location_safe}_{timestamp}_{run_id}"

        # JSON export
        json_path = self.export_path / f"{filename_base}.json"
        output = {"summary": summary, "leads": leads}
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        # CSV export
        csv_path = self.export_path / f"{filename_base}.csv"
        if leads:
            headers = list(leads[0].keys())
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(leads)
        else:
            headers = self._get_default_headers()
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

        return {
            "json_path": str(json_path),
            "csv_path": str(csv_path),
        }

    def _log_export_summary(self, export_status: Dict[str, Any]) -> None:
        """Log structured export summary."""
        logger.info(f"Export completed: {export_status['total_exported']}/{export_status['total_leads']} leads")
        logger.info(f"  Total skipped (duplicates): {export_status['total_skipped']}")

        per_sheet = export_status.get("per_sheet_stats", {})
        for route, stats in per_sheet.items():
            if isinstance(stats, dict):
                logger.info(
                    f"  [{route}] {stats.get('sheet_name', 'unknown')}: "
                    f"{stats.get('exported', 0)} exported, {stats.get('skipped', 0)} skipped"
                )

        if export_status.get("sheet_url"):
            logger.info(f"  Sheet URL: {export_status['sheet_url']}")
        if export_status.get("json_path"):
            logger.info(f"  JSON backup: {export_status['json_path']}")
        if export_status.get("csv_path"):
            logger.info(f"  CSV backup: {export_status['csv_path']}")
