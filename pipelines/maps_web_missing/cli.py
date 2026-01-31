#!/usr/bin/env python
"""
CLI entry point for Maps No-Website Pipeline.

Run this script directly to test the pipeline:
    python pipelines/maps_web_missing/cli.py

Execution Modes:
    --mode normal    Fresh ingestion from Maps API (default)
    --mode retry     Re-process failed website validations from Google Sheets

Options via environment variables:
    PIPELINE_MODE=retry      Override execution mode (CLI takes priority)
    MOCK_MAPS=1              Use mock data instead of real Maps API
    MOCK_SHEETS=1            Simulate Google Sheets export (for tests)
    MAPS_MAX_SEGMENTS=9      Max geographic segments to search
    MAPS_RADIUS_KM=5         Radius for each segment
    MAPS_API_DELAY=0.5       Delay between API calls (rate limiting)
    GOOGLE_SPREADSHEET_ID    Google Sheets document ID for export
    GOOGLE_CREDENTIALS_PATH  Path to service account JSON (default: credentials/service_account.json)
    PIPELINE_MAX_RETRIES     Max retry attempts for retry mode (default: 3)

Command line arguments:
    --mode, -m               Execution mode: normal | retry (default: normal)
    --query, -q              Search query (default: dentist) [normal mode only]
    --location, -l           Location to search (default: New York) [normal mode only]
    --spreadsheet-id, -s     Google Sheets document ID
    --sheet-name             Custom worksheet name (default: auto-generated)
    --retry-sheet-name       Sheet to read retry candidates from (default: WEBSITE_CHECK_ERRORS)
    --no-file-backup         Disable JSON/CSV file backup
    --segments               Number of geographic segments (default: 3) [normal mode only]

This avoids module reload warnings that occur with -m mode.
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path for direct script execution
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pipelines.maps_web_missing.pipeline import (
    build_pipeline,
    PIPELINE_NAME,
    VALID_MODES,
    get_pipeline_mode,
)
from pipelines.maps_web_missing.config import DEFAULT_SPREADSHEET_ID
from core.logger import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Maps No-Website Pipeline - Find businesses without websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modes:
  normal    Fresh ingestion from Maps API (default)
  retry     Re-process failed website validations from Google Sheets

Examples:
  # Normal mode - search for dentists in New York
  python cli.py --query dentist --location "New York"

  # Retry mode - reprocess failed validations
  python cli.py --mode retry --spreadsheet-id YOUR_SHEET_ID

  # Retry mode with custom retry sheet
  python cli.py --mode retry -s YOUR_SHEET_ID --retry-sheet-name CUSTOM_ERRORS
        """,
    )

    # Mode selection
    parser.add_argument(
        "-m", "--mode",
        choices=sorted(VALID_MODES),
        default=None,  # Will use get_pipeline_mode() for resolution
        help="Execution mode (default: normal, or PIPELINE_MODE env)",
    )

    # Normal mode arguments
    parser.add_argument(
        "-q", "--query",
        default="dentist",
        help="Search query (default: dentist) [normal mode only]",
    )
    parser.add_argument(
        "-l", "--location",
        default="New York",
        help="Location to search (default: New York) [normal mode only]",
    )
    parser.add_argument(
        "--segments",
        type=int,
        default=3,
        help="Number of geographic segments to search (default: 3) [normal mode only]",
    )
    parser.add_argument(
        "--radius",
        type=float,
        default=5.0,
        help="Search radius in km per segment (default: 5.0) [normal mode only]",
    )

    # Shared arguments
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

    # Retry mode arguments
    parser.add_argument(
        "--retry-sheet-name",
        default="WEBSITE_CHECK_ERRORS",
        help="Sheet to read retry candidates from (default: WEBSITE_CHECK_ERRORS) [retry mode only]",
    )

    return parser.parse_args()


def main():
    """
    Main entry point for the pipeline.

    Supports two execution modes:
        NORMAL: MapsSearchAgent → Normalize → Validate → Route → Format → Export
        RETRY:  RetryInputLoaderAgent → Validate → Route → Format → Export

    Set MOCK_MAPS=1 to test without consuming Maps API credits.
    Set MOCK_SHEETS=1 to test without calling Google Sheets API.
    """
    args = parse_args()

    # Resolve execution mode (CLI > ENV > default)
    try:
        mode = get_pipeline_mode(args.mode)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Print startup banner
    logger.info("=" * 60)
    logger.info(f"Running {PIPELINE_NAME}")
    logger.info(f"PIPELINE MODE: {mode.upper()}")
    logger.info("=" * 60)

    if mode == "normal":
        _run_normal_mode(args)
    else:
        _run_retry_mode(args)


def _run_normal_mode(args):
    """Run pipeline in normal (fresh ingestion) mode."""
    logger.info("Normal ingestion pipeline initialized")
    logger.info(f"  Query: {args.query}")
    logger.info(f"  Location: {args.location}")
    logger.info(f"  Segments: {args.segments}")
    logger.info(f"  Radius: {args.radius}km")
    logger.info(f"  Spreadsheet ID: {args.spreadsheet_id or '(none - file export only)'}")
    logger.info(f"  Sheet Name: {args.sheet_name or '(auto-generated)'}")
    logger.info(f"  File Backup: {not args.no_file_backup}")
    logger.info("=" * 60)

    # Build pipeline context for normal mode
    context = {
        "query": args.query,
        "location": args.location,
        "radius_km": args.radius,
        "max_segments": args.segments,
        "spreadsheet_id": args.spreadsheet_id,
        "sheet_name": args.sheet_name,
    }

    try:
        pipeline = build_pipeline(mode="normal", enable_file_backup=not args.no_file_backup)
        result = pipeline.run(context)
        _print_normal_summary(result)
        return 0
    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}")
        raise


def _run_retry_mode(args):
    """Run pipeline in retry mode."""
    logger.info("Retry pipeline initialized")
    logger.info(f"  Spreadsheet ID: {args.spreadsheet_id or '(REQUIRED)'}")
    logger.info(f"  Retry Sheet: {args.retry_sheet_name}")
    logger.info(f"  File Backup: {not args.no_file_backup}")
    logger.info("=" * 60)

    # Validate required arguments for retry mode
    if not args.spreadsheet_id:
        logger.error("Retry mode requires --spreadsheet-id (-s)")
        sys.exit(1)

    # Build pipeline context for retry mode
    context = {
        "spreadsheet_id": args.spreadsheet_id,
        "retry_sheet_name": args.retry_sheet_name,
        "sheet_name": args.sheet_name,
    }

    try:
        pipeline = build_pipeline(mode="retry", enable_file_backup=not args.no_file_backup)
        result = pipeline.run(context)
        _print_retry_summary(result)
        return 0
    except Exception as e:
        logger.error(f"✗ Pipeline failed: {e}")
        raise


def _print_normal_summary(result: dict):
    """Print summary for normal mode execution."""
    logger.info("-" * 60)
    logger.info("PIPELINE RESULTS:")
    logger.info(f"  Search Metadata: {result.get('search_metadata', {})}")
    logger.info(f"  Summary: {result.get('summary', {})}")

    _print_export_status(result)

    logger.info("-" * 60)
    logger.info("✓ Pipeline completed successfully")


def _print_retry_summary(result: dict):
    """Print summary for retry mode execution."""
    logger.info("-" * 60)
    logger.info("RETRY SUMMARY:")

    # Retry stats from RetryInputLoaderAgent
    retry_stats = result.get("retry_stats", {})
    logger.info(f"  Loaded Rows: {retry_stats.get('total_rows', 0)}")
    logger.info(f"  Retry Candidates: {retry_stats.get('loaded', 0)}")
    logger.info(f"  Skipped (Max Retries): {retry_stats.get('skipped_max_retry', 0)}")
    logger.info(f"  Skipped (Missing Fields): {retry_stats.get('skipped_missing_fields', 0)}")

    # Routing stats from LeadRouterAgent
    routing_stats = result.get("routing_stats", {})
    logger.info(f"  Routed to TARGET: {routing_stats.get('target_count', 0)}")
    logger.info(f"  Routed to EXCLUDED: {routing_stats.get('excluded_count', 0)}")
    logger.info(f"  Routed to RETRY: {routing_stats.get('retry_count', 0)}")

    _print_export_status(result)

    logger.info("-" * 60)
    logger.info("✓ Retry pipeline completed successfully")


def _print_export_status(result: dict):
    """Print export status (shared by both modes)."""
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


if __name__ == "__main__":
    sys.exit(main() or 0)
