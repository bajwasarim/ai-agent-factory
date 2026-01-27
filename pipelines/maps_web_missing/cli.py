#!/usr/bin/env python
"""
CLI entry point for Maps No-Website Pipeline.

Run this script directly to test the pipeline:
    python pipelines/maps_web_missing/cli.py

Options via environment variables:
    MOCK_MAPS=1              Use mock data instead of real Maps API
    MOCK_SHEETS=1            Simulate Google Sheets export (for tests)
    MAPS_MAX_SEGMENTS=9      Max geographic segments to search
    MAPS_RADIUS_KM=5         Radius for each segment
    MAPS_API_DELAY=0.5       Delay between API calls (rate limiting)
    GOOGLE_SPREADSHEET_ID    Google Sheets document ID for export
    GOOGLE_CREDENTIALS_PATH  Path to service account JSON (default: credentials/service_account.json)

Command line arguments:
    --query, -q              Search query (default: dentist)
    --location, -l           Location to search (default: New York)
    --spreadsheet-id, -s     Google Sheets document ID
    --sheet-name             Custom worksheet name (default: auto-generated)
    --no-file-backup         Disable JSON/CSV file backup
    --segments               Number of geographic segments (default: 3)

This avoids module reload warnings that occur with -m mode.
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path for direct script execution
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pipelines.maps_web_missing.pipeline import build_pipeline, PIPELINE_NAME
from pipelines.maps_web_missing.config import DEFAULT_SPREADSHEET_ID
from core.logger import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Maps No-Website Pipeline - Find businesses without websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-q", "--query",
        default="dentist",
        help="Search query (default: dentist)",
    )
    parser.add_argument(
        "-l", "--location",
        default="New York",
        help="Location to search (default: New York)",
    )
    parser.add_argument(
        "-s", "--spreadsheet-id",
        default=DEFAULT_SPREADSHEET_ID or None,
        help="Google Sheets document ID for export",
    )
    parser.add_argument(
        "--sheet-name",
        default=None,
        help="Custom worksheet name (default: auto-generated as query_location_date)",
    )
    parser.add_argument(
        "--no-file-backup",
        action="store_true",
        help="Disable JSON/CSV file backup",
    )
    parser.add_argument(
        "--segments",
        type=int,
        default=3,
        help="Number of geographic segments to search (default: 3)",
    )
    parser.add_argument(
        "--radius",
        type=float,
        default=5.0,
        help="Search radius in km per segment (default: 5.0)",
    )

    return parser.parse_args()


def main():
    """
    Main entry point for the pipeline.

    MapsSearchAgent handles the search with:
    - Radius expansion: Searches multiple geographic segments
    - Deduplication: Removes duplicate businesses

    GoogleSheetsExportAgent handles export with:
    - Google Sheets: Writes to specified spreadsheet (if ID provided)
    - Idempotency: Skips duplicate rows on re-runs
    - File backup: Optional JSON/CSV backup

    Set MOCK_MAPS=1 to test without consuming Maps API credits.
    Set MOCK_SHEETS=1 to test without calling Google Sheets API.
    """
    args = parse_args()

    logger.info("=" * 60)
    logger.info(f"Running {PIPELINE_NAME}")
    logger.info("=" * 60)
    logger.info(f"  Query: {args.query}")
    logger.info(f"  Location: {args.location}")
    logger.info(f"  Segments: {args.segments}")
    logger.info(f"  Radius: {args.radius}km")
    logger.info(f"  Spreadsheet ID: {args.spreadsheet_id or '(none - file export only)'}")
    logger.info(f"  Sheet Name: {args.sheet_name or '(auto-generated)'}")
    logger.info(f"  File Backup: {not args.no_file_backup}")
    logger.info("=" * 60)

    # Build pipeline context
    context = {
        "query": args.query,
        "location": args.location,
        "radius_km": args.radius,
        "max_segments": args.segments,
        # Google Sheets export settings
        "spreadsheet_id": args.spreadsheet_id,
        "sheet_name": args.sheet_name,
    }

    try:
        # Build pipeline with file backup setting
        pipeline = build_pipeline(enable_file_backup=not args.no_file_backup)
        result = pipeline.run(context)

        logger.info("-" * 60)
        logger.info("PIPELINE RESULTS:")
        logger.info(f"  Search Metadata: {result.get('search_metadata', {})}")
        logger.info(f"  Summary: {result.get('summary', {})}")

        export_status = result.get("export_status", {})
        logger.info("  Export Status:")
        logger.info(f"    Total Leads: {export_status.get('total_leads', 0)}")
        logger.info(f"    Success Count: {export_status.get('success_count', 0)}")
        logger.info(f"    New Leads Added: {export_status.get('new_leads_added', 0)}")
        logger.info(f"    Duplicates Skipped: {export_status.get('duplicate_count', 0)}")

        if export_status.get("sheet_url"):
            logger.info(f"    Sheet URL: {export_status['sheet_url']}")
            logger.info(f"    Sheet Name: {export_status.get('sheet_name', '')}")

        if export_status.get("json_path"):
            logger.info(f"    JSON Backup: {export_status['json_path']}")
        if export_status.get("csv_path"):
            logger.info(f"    CSV Backup: {export_status['csv_path']}")

        logger.info("-" * 60)
        logger.info("✓ Pipeline completed successfully")

        return 0

    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    sys.exit(main() or 0)
