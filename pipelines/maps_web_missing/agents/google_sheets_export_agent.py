"""Google Sheets export agent for Maps No-Website Pipeline."""

import json
import csv
import hashlib
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

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


class GoogleSheetsExportAgent(BaseAgent):
    """
    Agent that exports formatted leads to Google Sheets.

    Features:
    - Authenticates via service account JSON
    - Auto-creates worksheets with dynamic naming
    - Appends header row only if sheet is empty
    - Batch appends leads efficiently
    - Idempotency: deduplicates by lead hash to prevent duplicate rows
    - Optional file backup (JSON/CSV) alongside Sheets export

    Input: formatted_leads, summary, query, location, spreadsheet_id (optional)
    Output: export_status with sheet URL, counts, and optional file paths
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
        Export formatted leads to Google Sheets and optionally to files.

        Args:
            input_data: Dict with:
                - formatted_leads: List of lead dicts
                - summary: Summary dict from formatter
                - query: Search query string
                - location: Location string
                - spreadsheet_id: Optional Google Sheets ID
                - sheet_name: Optional worksheet name override

        Returns:
            Dict with 'export_status' containing:
                - total_leads: Total leads processed
                - success_count: Leads successfully exported
                - sheet_url: URL to the Google Sheet (or None in mock mode)
                - sheet_name: Name of the worksheet
                - new_leads_added: Count of new leads (not duplicates)
                - duplicate_count: Count of skipped duplicates
                - json_path: Path to JSON backup (if enabled)
                - csv_path: Path to CSV backup (if enabled)
        """
        leads = input_data.get("formatted_leads", [])
        summary = input_data.get("summary", {})
        query = input_data.get("query", "export")
        location = input_data.get("location", "")
        spreadsheet_id = input_data.get("spreadsheet_id")
        sheet_name_override = input_data.get("sheet_name")

        # Generate default sheet name: {query}_{location}_{YYYY-MM-DD}
        if sheet_name_override:
            sheet_name = sheet_name_override
        else:
            date_str = datetime.now().strftime("%Y-%m-%d")
            location_safe = sanitize_filename(location) if location else "unknown"
            query_safe = sanitize_filename(query)
            sheet_name = f"{query_safe}_{location_safe}_{date_str}"

        export_status = {
            "total_leads": len(leads),
            "success_count": 0,
            "sheet_url": None,
            "sheet_name": sheet_name,
            "new_leads_added": 0,
            "duplicate_count": 0,
            "json_path": None,
            "csv_path": None,
        }

        # Export to Google Sheets (or mock)
        if MOCK_SHEETS or not spreadsheet_id:
            if not spreadsheet_id:
                logger.warning("No spreadsheet_id provided, skipping Google Sheets export")
            else:
                logger.info("MOCK_SHEETS enabled - simulating Google Sheets export")

            export_status["success_count"] = len(leads)
            export_status["new_leads_added"] = len(leads)
            export_status["sheet_url"] = f"https://docs.google.com/spreadsheets/d/MOCK_ID/edit#gid=0"
        else:
            # Real Google Sheets export
            try:
                sheets_result = self._export_to_sheets(
                    leads=leads,
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name,
                )
                export_status.update(sheets_result)
            except Exception as e:
                logger.error(f"Google Sheets export failed: {e}")
                export_status["error"] = str(e)

        # Optional file backup
        if self.enable_file_backup:
            file_result = self._export_to_files(leads, summary, query, location)
            export_status["json_path"] = file_result.get("json_path")
            export_status["csv_path"] = file_result.get("csv_path")

        logger.info(f"Export completed: {export_status['success_count']}/{len(leads)} leads")
        if export_status.get("sheet_url"):
            logger.info(f"  Sheet: {export_status['sheet_name']}")
        if export_status.get("json_path"):
            logger.info(f"  JSON backup: {export_status['json_path']}")

        return {"export_status": export_status}

    def _export_to_sheets(
        self,
        leads: List[Dict[str, Any]],
        spreadsheet_id: str,
        sheet_name: str,
    ) -> Dict[str, Any]:
        """
        Export leads to Google Sheets with idempotency.

        Args:
            leads: List of lead dicts to export.
            spreadsheet_id: Google Sheets document ID.
            sheet_name: Name of the worksheet.

        Returns:
            Dict with export results.
        """
        import gspread

        # Initialize client if needed
        if self._client is None:
            self._client = _get_gspread_client(self.credentials_path)

        # Open spreadsheet
        try:
            spreadsheet = self._client.open_by_key(spreadsheet_id)
        except gspread.SpreadsheetNotFound:
            raise ValueError(f"Spreadsheet not found: {spreadsheet_id}")

        # Get or create worksheet
        worksheet = self._get_or_create_worksheet(spreadsheet, sheet_name)

        # Prepare headers
        if not leads:
            return {
                "success_count": 0,
                "new_leads_added": 0,
                "duplicate_count": 0,
                "sheet_url": spreadsheet.url,
            }

        headers = list(leads[0].keys())

        # Get current sheet data
        current_values = worksheet.get_all_values()
        # Filter out completely empty rows
        current_values = [row for row in current_values if any(cell.strip() for cell in row)]

        # Check if sheet needs headers (first row should match expected headers)
        needs_headers = True
        if current_values:
            first_row = [str(c).lower().strip() for c in current_values[0]]
            expected_headers_lower = [h.lower() for h in headers]
            if first_row == expected_headers_lower:
                needs_headers = False
            else:
                logger.warning(f"First row doesn't match expected headers, will insert headers")

        if needs_headers:
            if current_values:
                # Sheet has data but no headers - insert at row 1
                worksheet.insert_row(headers, index=1, value_input_option="USER_ENTERED")
                logger.info(f"Inserted header row at top of sheet '{sheet_name}'")
                # Re-fetch values with new header
                current_values = worksheet.get_all_values()
                current_values = [row for row in current_values if any(cell.strip() for cell in row)]
            else:
                # Empty sheet - add headers
                worksheet.append_row(headers, value_input_option="USER_ENTERED")
                logger.info(f"Added header row to sheet '{sheet_name}'")
                current_values = [headers]

        # Get existing row hashes for idempotency check
        existing_hashes = self._compute_existing_hashes(current_values, headers)
        data_row_count = len(current_values) - 1 if current_values else 0
        if data_row_count > 0:
            logger.info(f"Found {data_row_count} existing rows for deduplication")
        logger.info(f"Found {len(existing_hashes)} existing data rows for deduplication")

        # Filter out duplicates and prepare rows
        new_rows = []
        duplicate_count = 0
        collision_prevented = 0

        for lead in leads:
            # Use pre-computed dedup_key from BusinessNormalizeAgent (single source of truth)
            dedup_key = lead.get("dedup_key")
            if not dedup_key:
                raise RuntimeError(
                    f"Exporter contract violation: dedup_key missing. "
                    f"Lead id={lead.get('place_id', 'unknown')}"
                )
            if dedup_key in existing_hashes:
                duplicate_count += 1
                logger.debug(f"Skipped duplicate: {dedup_key[:50]}...")
                continue

            # Convert lead to row values in header order with phone formatting
            row = self._format_row_for_sheets(lead, headers)
            new_rows.append(row)
            existing_hashes.add(dedup_key)

        # Log collision stats
        if collision_prevented > 0:
            logger.info(f"Prevented {collision_prevented} hash collisions")

        # Batch append new rows
        if new_rows:
            worksheet.append_rows(new_rows, value_input_option="USER_ENTERED")
            logger.info(f"Appended {len(new_rows)} new rows to sheet '{sheet_name}'")

        if duplicate_count > 0:
            logger.info(f"Skipped {duplicate_count} duplicate rows (idempotency)")

        return {
            "success_count": len(new_rows),
            "new_leads_added": len(new_rows),
            "duplicate_count": duplicate_count,
            "sheet_url": spreadsheet.url,
        }

    def _get_or_create_worksheet(self, spreadsheet, sheet_name: str):
        """
        Get existing worksheet or create new one.

        Args:
            spreadsheet: gspread Spreadsheet object.
            sheet_name: Name of worksheet to get/create.

        Returns:
            gspread Worksheet object.
        """
        import gspread

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            logger.info(f"Using existing worksheet: '{sheet_name}'")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=20,
            )
            logger.info(f"Created new worksheet: '{sheet_name}'")

        return worksheet

    def _compute_existing_hashes(
        self, all_values: List[List[str]], headers: List[str]
    ) -> Set[str]:
        """
        Compute dedup keys of existing rows for idempotency checking.

        Uses place_id as primary key, or hash of (name + phone + address) as fallback.
        This matches the compute_dedup_key logic for consistency.

        Args:
            all_values: All values from worksheet (including header row).
            headers: Expected header list for column mapping.

        Returns:
            Set of dedup key strings for existing data rows.
        """
        dedup_keys = set()

        if len(all_values) <= 1:
            # Only header or empty
            return dedup_keys

        # Get header indices for key fields from actual sheet headers
        actual_headers = [h.lower().strip() for h in all_values[0]] if all_values else []

        # Find indices - be flexible with header naming
        place_id_idx = None
        name_idx = None
        phone_idx = None
        address_idx = None

        for idx, header in enumerate(actual_headers):
            if header == "place_id":
                place_id_idx = idx
            elif header == "name":
                name_idx = idx
            elif header == "phone":
                phone_idx = idx
            elif header == "address":
                address_idx = idx

        # Skip header row, compute dedup keys for data rows
        for row in all_values[1:]:
            # Extract values safely
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

    def _compute_dedup_key(self, lead: Dict[str, Any]) -> str:
        """
        Compute deterministic dedup key for a lead dict.

        Uses place_id as primary key, or hash of normalized (name + phone + address).

        Args:
            lead: Lead dict.

        Returns:
            Dedup key string (format: "pid:<id>" or "hash:<sha256>").
        """
        return compute_dedup_key(
            place_id=lead.get("place_id"),
            name=lead.get("name"),
            phone=lead.get("phone"),
            address=lead.get("address"),
        )

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
            headers = [
                "rank", "name", "website", "description", "source",
                "location", "phone", "rating", "reviews", "address", "has_website"
            ]
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)

        return {
            "json_path": str(json_path),
            "csv_path": str(csv_path),
        }
