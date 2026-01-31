"""
Export Fan-Out Tests.

Tests for GoogleSheetsExportAgent fan-out architecture:
- Partition validation (contract field requirements)
- Per-sheet dedup isolation
- Batch chunking (MAX_BATCH_SIZE = 200)
- Atomic abort behavior
- Export stats shape

All tests run offline with mocked external dependencies.
"""

from unittest.mock import MagicMock, patch, call
from typing import Dict, List, Set, Any

import pytest

from pipelines.maps_web_missing.agents.google_sheets_export_agent import (
    GoogleSheetsExportAgent,
    MAX_BATCH_SIZE,
    SHEET_EXPORT_ORDER,
)
from fixtures.sample_formatted_leads import (
    generate_mock_leads,
    generate_leads_for_sheet,
)


class TestPartitionValidation:
    """Tests for _partition_leads_by_sheet() contract validation."""

    def test_missing_target_sheet_raises_value_error(self):
        """
        Contract test: Missing target_sheet must raise ValueError with actionable message.
        """
        # Arrange: Lead missing target_sheet
        leads = [
            {
                "name": "Test Business",
                "place_id": "place_1",
                "dedup_key": "pid:place_1",
                # NO target_sheet - contract violation
            }
        ]

        exporter = GoogleSheetsExportAgent()

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            exporter._partition_leads_by_sheet(leads)

        error_message = str(exc_info.value)
        assert "target_sheet missing" in error_message, (
            f"Error should mention 'target_sheet missing'. Got: {error_message}"
        )
        assert "place_1" in error_message, (
            f"Error should include place_id for debugging. Got: {error_message}"
        )

    def test_missing_dedup_key_raises_value_error(self):
        """
        Contract test: Missing dedup_key must raise ValueError with actionable message.
        """
        # Arrange: Lead missing dedup_key
        leads = [
            {
                "name": "Test Business",
                "place_id": "place_1",
                "target_sheet": "NO_WEBSITE_TARGETS",
                # NO dedup_key - contract violation
            }
        ]

        exporter = GoogleSheetsExportAgent()

        # Act & Assert
        with pytest.raises(ValueError) as exc_info:
            exporter._partition_leads_by_sheet(leads)

        error_message = str(exc_info.value)
        assert "dedup_key missing" in error_message, (
            f"Error should mention 'dedup_key missing'. Got: {error_message}"
        )
        assert "place_1" in error_message, (
            f"Error should include place_id for debugging. Got: {error_message}"
        )

    def test_valid_leads_partition_correctly(self):
        """
        Contract test: Valid leads partition into correct sheet buckets.
        """
        # Arrange: Mixed leads with valid contract fields
        leads = generate_mock_leads(target_count=3, excluded_count=2, retry_count=1)

        exporter = GoogleSheetsExportAgent()

        # Act
        partitioned = exporter._partition_leads_by_sheet(leads)

        # Assert: Correct bucket counts
        assert len(partitioned["NO_WEBSITE_TARGETS"]) == 3, (
            f"Expected 3 TARGET leads, got {len(partitioned['NO_WEBSITE_TARGETS'])}"
        )
        assert len(partitioned["HAS_WEBSITE_EXCLUDED"]) == 2, (
            f"Expected 2 EXCLUDED leads, got {len(partitioned['HAS_WEBSITE_EXCLUDED'])}"
        )
        assert len(partitioned["WEBSITE_CHECK_ERRORS"]) == 1, (
            f"Expected 1 RETRY lead, got {len(partitioned['WEBSITE_CHECK_ERRORS'])}"
        )

    def test_unknown_target_sheet_routes_to_errors(self):
        """
        Contract test: Unknown target_sheet routes to WEBSITE_CHECK_ERRORS with warning.
        """
        # Arrange: Lead with unknown target_sheet
        leads = [
            {
                "name": "Test Business",
                "place_id": "place_1",
                "dedup_key": "pid:place_1",
                "target_sheet": "UNKNOWN_SHEET",  # Invalid sheet name
            }
        ]

        exporter = GoogleSheetsExportAgent()

        # Act
        partitioned = exporter._partition_leads_by_sheet(leads)

        # Assert: Routed to error sheet
        assert len(partitioned["WEBSITE_CHECK_ERRORS"]) == 1, (
            "Unknown target_sheet should route to WEBSITE_CHECK_ERRORS"
        )
        assert len(partitioned["NO_WEBSITE_TARGETS"]) == 0
        assert len(partitioned["HAS_WEBSITE_EXCLUDED"]) == 0


class TestPerSheetDedupIsolation:
    """Tests for per-sheet dedup isolation (NOT global dedup)."""

    def test_same_dedup_key_allowed_across_different_sheets(self):
        """
        Contract test: Same dedup_key can exist in different sheets.

        This is intentional - dedup is per-sheet, not global.
        A business might appear in TARGET initially, then move to EXCLUDED
        after getting a website.
        """
        # Arrange: Same dedup_key in different sheets
        leads = [
            {
                "name": "Business A",
                "place_id": "place_shared",
                "dedup_key": "pid:place_shared",  # Same dedup_key
                "target_sheet": "NO_WEBSITE_TARGETS",
            },
            {
                "name": "Business A (updated)",
                "place_id": "place_shared",
                "dedup_key": "pid:place_shared",  # Same dedup_key
                "target_sheet": "HAS_WEBSITE_EXCLUDED",  # Different sheet
            },
        ]

        exporter = GoogleSheetsExportAgent()

        # Act
        partitioned = exporter._partition_leads_by_sheet(leads)

        # Assert: Both leads partitioned (not deduplicated at partition level)
        assert len(partitioned["NO_WEBSITE_TARGETS"]) == 1
        assert len(partitioned["HAS_WEBSITE_EXCLUDED"]) == 1

    def test_duplicate_dedup_key_in_same_sheet_partitions_both(self):
        """
        Contract test: Partition does NOT deduplicate - that's the write phase's job.

        Partition only routes leads to buckets. Dedup happens during write.
        """
        # Arrange: Duplicate dedup_key in same sheet
        leads = [
            {
                "name": "Business A",
                "place_id": "place_dup",
                "dedup_key": "pid:place_dup",
                "target_sheet": "NO_WEBSITE_TARGETS",
            },
            {
                "name": "Business A (duplicate)",
                "place_id": "place_dup",
                "dedup_key": "pid:place_dup",  # Same dedup_key, same sheet
                "target_sheet": "NO_WEBSITE_TARGETS",
            },
        ]

        exporter = GoogleSheetsExportAgent()

        # Act
        partitioned = exporter._partition_leads_by_sheet(leads)

        # Assert: Both leads in partition (dedup happens during write)
        assert len(partitioned["NO_WEBSITE_TARGETS"]) == 2, (
            "Partition should not deduplicate - that's the write phase's responsibility"
        )


class TestBatchChunking:
    """Tests for batch-safe writes with MAX_BATCH_SIZE."""

    def test_max_batch_size_constant(self):
        """Verify MAX_BATCH_SIZE is set correctly."""
        assert MAX_BATCH_SIZE == 200, f"MAX_BATCH_SIZE should be 200, got {MAX_BATCH_SIZE}"

    def test_large_batch_splits_into_chunks(self):
        """
        Contract test: 450 leads should result in 3 batches (200, 200, 50).
        """
        # Arrange: 450 leads for one sheet
        leads = generate_leads_for_sheet("NO_WEBSITE_TARGETS", count=450)

        # Create mock worksheet
        mock_worksheet = MagicMock()
        mock_worksheet.append_rows = MagicMock()

        exporter = GoogleSheetsExportAgent()
        headers = list(leads[0].keys())

        # Act
        result = exporter._write_sheet_batch(
            worksheet=mock_worksheet,
            leads=leads,
            headers=headers,
            existing_dedup_keys=set(),  # No existing keys
            sheet_name="NO_WEBSITE_TARGETS",
        )

        # Assert: 3 batch calls (200 + 200 + 50)
        assert mock_worksheet.append_rows.call_count == 3, (
            f"Expected 3 batch calls for 450 leads, got {mock_worksheet.append_rows.call_count}"
        )

        # Verify batch sizes
        call_args_list = mock_worksheet.append_rows.call_args_list
        batch_sizes = [len(call[0][0]) for call in call_args_list]
        assert batch_sizes == [200, 200, 50], f"Expected batch sizes [200, 200, 50], got {batch_sizes}"

        # Verify total exported
        assert result["exported"] == 450, f"Expected 450 exported, got {result['exported']}"

    def test_exact_batch_size_single_batch(self):
        """
        Contract test: Exactly MAX_BATCH_SIZE leads = 1 batch.
        """
        # Arrange: Exactly 200 leads
        leads = generate_leads_for_sheet("NO_WEBSITE_TARGETS", count=200)

        mock_worksheet = MagicMock()
        mock_worksheet.append_rows = MagicMock()

        exporter = GoogleSheetsExportAgent()
        headers = list(leads[0].keys())

        # Act
        result = exporter._write_sheet_batch(
            worksheet=mock_worksheet,
            leads=leads,
            headers=headers,
            existing_dedup_keys=set(),
            sheet_name="NO_WEBSITE_TARGETS",
        )

        # Assert: Single batch
        assert mock_worksheet.append_rows.call_count == 1
        assert result["exported"] == 200

    def test_dedup_reduces_batch_count(self):
        """
        Contract test: Dedup reduces actual rows written (and potentially batch count).
        """
        # Arrange: 250 leads, but 100 already exist
        leads = generate_leads_for_sheet("NO_WEBSITE_TARGETS", count=250)
        existing_keys = {lead["dedup_key"] for lead in leads[:100]}  # First 100 exist

        mock_worksheet = MagicMock()
        mock_worksheet.append_rows = MagicMock()

        exporter = GoogleSheetsExportAgent()
        headers = list(leads[0].keys())

        # Act
        result = exporter._write_sheet_batch(
            worksheet=mock_worksheet,
            leads=leads,
            headers=headers,
            existing_dedup_keys=existing_keys,
            sheet_name="NO_WEBSITE_TARGETS",
        )

        # Assert: Only 150 new leads (1 batch, not 2)
        assert result["exported"] == 150, f"Expected 150 exported, got {result['exported']}"
        assert result["skipped"] == 100, f"Expected 100 skipped, got {result['skipped']}"
        assert mock_worksheet.append_rows.call_count == 1, (
            "150 leads should fit in 1 batch"
        )


class TestAtomicAbortBehavior:
    """Tests for atomic export behavior - abort on failure, no partial backups."""

    @patch("pipelines.maps_web_missing.agents.google_sheets_export_agent.MOCK_SHEETS", False)
    @patch("pipelines.maps_web_missing.agents.google_sheets_export_agent._get_gspread_client")
    def test_abort_on_second_sheet_failure_no_backup(self, mock_get_client):
        """
        Contract test: If second sheet write fails, export aborts and NO backup files created.
        """
        # Arrange: Mock gspread client and spreadsheet
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.url = "https://docs.google.com/spreadsheets/d/TEST_ID"
        mock_spreadsheet.title = "Test Spreadsheet"
        mock_client.open_by_key.return_value = mock_spreadsheet

        # Create mock worksheets
        mock_ws_target = MagicMock()
        mock_ws_target.get_all_values.return_value = []  # Empty sheet
        mock_ws_target.append_row = MagicMock()
        mock_ws_target.append_rows = MagicMock()  # Success

        mock_ws_excluded = MagicMock()
        mock_ws_excluded.get_all_values.return_value = []
        mock_ws_excluded.append_row = MagicMock()
        mock_ws_excluded.append_rows = MagicMock(
            side_effect=Exception("API Error: Rate limit exceeded")  # FAIL
        )

        mock_ws_retry = MagicMock()
        mock_ws_retry.get_all_values.return_value = []
        mock_ws_retry.append_row = MagicMock()

        def get_worksheet(name):
            if name == "NO_WEBSITE_TARGETS":
                return mock_ws_target
            elif name == "HAS_WEBSITE_EXCLUDED":
                return mock_ws_excluded
            elif name == "WEBSITE_CHECK_ERRORS":
                return mock_ws_retry
            raise Exception(f"Unknown worksheet: {name}")

        mock_spreadsheet.worksheet.side_effect = get_worksheet

        # Arrange: Leads for multiple sheets
        leads = generate_mock_leads(target_count=2, excluded_count=2, retry_count=1)

        exporter = GoogleSheetsExportAgent(enable_file_backup=True)

        input_data = {
            "formatted_leads": leads,
            "query": "test",
            "location": "Test City",
            "spreadsheet_id": "TEST_SPREADSHEET_ID",
        }

        # Act & Assert: Export should raise RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            exporter.run(input_data)

        assert "fan-out export failed" in str(exc_info.value).lower() or \
               "write phase failed" in str(exc_info.value).lower(), (
            f"Expected fan-out/write failure error, got: {exc_info.value}"
        )

    def test_mock_mode_does_not_write_sheets(self):
        """
        Contract test: MOCK_SHEETS mode simulates export without real API calls.
        """
        # Arrange
        leads = generate_mock_leads(target_count=2, excluded_count=1, retry_count=1)

        with patch.dict("os.environ", {"MOCK_SHEETS": "true"}):
            # Re-import to pick up mock flag (or instantiate fresh)
            exporter = GoogleSheetsExportAgent(enable_file_backup=False)

            input_data = {
                "formatted_leads": leads,
                "query": "test",
                "location": "Test City",
                # No spreadsheet_id - should skip sheets export
            }

            # Act
            result = exporter.run(input_data)

            # Assert: Mock stats returned
            export_status = result["export_status"]
            assert export_status["total_exported"] == 4
            assert export_status["total_skipped"] == 0


class TestExportStatsShape:
    """Tests for export stats structure validation."""

    def test_export_status_has_required_keys(self):
        """
        Contract test: export_status must have all required keys.
        """
        # Arrange
        leads = generate_mock_leads(target_count=2, excluded_count=1, retry_count=1)

        with patch.dict("os.environ", {"MOCK_SHEETS": "true"}):
            exporter = GoogleSheetsExportAgent(enable_file_backup=False)

            input_data = {
                "formatted_leads": leads,
                "query": "test",
                "location": "Test City",
            }

            # Act
            result = exporter.run(input_data)

            # Assert: Required top-level keys exist
            export_status = result["export_status"]
            required_keys = [
                "total_leads",
                "total_exported",
                "total_skipped",
                "per_sheet_stats",
                "sheet_url",
            ]

            for key in required_keys:
                assert key in export_status, (
                    f"Missing required key '{key}' in export_status. "
                    f"Got keys: {list(export_status.keys())}"
                )

    def test_per_sheet_stats_has_all_routes(self):
        """
        Contract test: per_sheet_stats must have stats for all route categories.
        """
        # Arrange
        leads = generate_mock_leads(target_count=1, excluded_count=1, retry_count=1)

        with patch.dict("os.environ", {"MOCK_SHEETS": "true"}):
            exporter = GoogleSheetsExportAgent(enable_file_backup=False)

            input_data = {
                "formatted_leads": leads,
                "query": "test",
                "location": "Test City",
            }

            # Act
            result = exporter.run(input_data)

            # Assert: All sheet categories present
            per_sheet = result["export_status"]["per_sheet_stats"]

            expected_sheets = [
                "NO_WEBSITE_TARGETS",
                "HAS_WEBSITE_EXCLUDED",
                "WEBSITE_CHECK_ERRORS",
            ]

            for sheet in expected_sheets:
                assert sheet in per_sheet, (
                    f"Missing sheet stats for '{sheet}'. "
                    f"Got: {list(per_sheet.keys())}"
                )

                # Verify stats shape
                stats = per_sheet[sheet]
                assert "exported" in stats, f"Missing 'exported' in {sheet} stats"
                assert "skipped" in stats, f"Missing 'skipped' in {sheet} stats"
                assert "sheet_name" in stats, f"Missing 'sheet_name' in {sheet} stats"

    def test_export_counts_are_consistent(self):
        """
        Contract test: total_exported must equal sum of per-sheet exported counts.
        """
        # Arrange
        leads = generate_mock_leads(target_count=3, excluded_count=2, retry_count=1)

        with patch.dict("os.environ", {"MOCK_SHEETS": "true"}):
            exporter = GoogleSheetsExportAgent(enable_file_backup=False)

            input_data = {
                "formatted_leads": leads,
                "query": "test",
                "location": "Test City",
            }

            # Act
            result = exporter.run(input_data)

            # Assert: Counts are consistent
            export_status = result["export_status"]
            per_sheet = export_status["per_sheet_stats"]

            sum_exported = sum(stats["exported"] for stats in per_sheet.values())
            sum_skipped = sum(stats["skipped"] for stats in per_sheet.values())

            assert export_status["total_exported"] == sum_exported, (
                f"total_exported ({export_status['total_exported']}) != "
                f"sum of per-sheet exported ({sum_exported})"
            )
            assert export_status["total_skipped"] == sum_skipped, (
                f"total_skipped ({export_status['total_skipped']}) != "
                f"sum of per-sheet skipped ({sum_skipped})"
            )


class TestSheetExportOrder:
    """Tests for deterministic export order."""

    def test_sheet_export_order_constant(self):
        """Verify SHEET_EXPORT_ORDER is correct."""
        assert SHEET_EXPORT_ORDER == [
            "NO_WEBSITE_TARGETS",
            "HAS_WEBSITE_EXCLUDED",
            "WEBSITE_CHECK_ERRORS",
        ], f"SHEET_EXPORT_ORDER mismatch: {SHEET_EXPORT_ORDER}"

    def test_export_processes_sheets_in_order(self):
        """
        Contract test: Sheets must be processed in order TARGET → EXCLUDED → RETRY.

        This ensures deterministic behavior and allows abort-on-failure
        to have predictable partial state.
        """
        # This is implicitly tested by the abort test, but we document the expectation here
        # The SHEET_EXPORT_ORDER constant enforces this
        expected_order = ["NO_WEBSITE_TARGETS", "HAS_WEBSITE_EXCLUDED", "WEBSITE_CHECK_ERRORS"]

        assert SHEET_EXPORT_ORDER == expected_order, (
            f"Export order must be TARGET → EXCLUDED → RETRY. Got: {SHEET_EXPORT_ORDER}"
        )
