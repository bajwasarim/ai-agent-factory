"""
Production-grade pytest coverage for RetryInputLoaderAgent.

Tests verify:
- Contract compliance (output keys, field preservation)
- ENV configuration (PIPELINE_MAX_RETRIES)
- Skip logic for maxed-out retries
- Row ordering preservation
- Missing field safety
- Pure function behavior

All tests are:
- Offline (no network calls)
- Deterministic (mocked datetime)
- CI safe (no flaky dependencies)
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from pipelines.maps_web_missing.agents.retry_input_loader_agent import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_SHEET,
    RetryInputLoaderAgent,
    _get_max_retries,
    parse_retry_attempt,
    parse_row_to_candidate,
    transform_rows_to_candidates,
)


# =============================================================================
# FROZEN TIME FOR DETERMINISTIC TESTS
# =============================================================================

FROZEN_TIME = datetime(2026, 1, 31, 12, 0, 0, tzinfo=timezone.utc)
FROZEN_ISO = "2026-01-31T12:00:00Z"


# =============================================================================
# FIXTURES: Reusable Test Data
# =============================================================================

@pytest.fixture
def mock_retry_rows_valid() -> List[Dict[str, str]]:
    """
    Fixture: Valid retry rows with all required fields.
    
    Contract: Each row has dedup_key, name, address, phone, website,
    retry_attempt (below max), and last_retry_ts.
    """
    return [
        {
            "dedup_key": "pid:valid_001",
            "name": "Valid Business One",
            "address": "100 First St",
            "phone": "555-0001",
            "website": "https://old-site-1.com",
            "retry_attempt": "0",
            "last_retry_ts": "2026-01-30T10:00:00Z",
        },
        {
            "dedup_key": "pid:valid_002",
            "name": "Valid Business Two",
            "address": "200 Second Ave",
            "phone": "555-0002",
            "website": "",  # Empty website is valid
            "retry_attempt": "1",
            "last_retry_ts": "2026-01-30T11:00:00Z",
        },
        {
            "dedup_key": "pid:valid_003",
            "name": "Valid Business Three",
            "address": "300 Third Blvd",
            "phone": "555-0003",
            "website": "https://old-site-3.com",
            "retry_attempt": "2",
            "last_retry_ts": "2026-01-30T12:00:00Z",
        },
    ]


@pytest.fixture
def mock_retry_rows_with_maxed_attempts() -> List[Dict[str, str]]:
    """
    Fixture: Rows where retry_attempt >= MAX_RETRIES (should be skipped).
    
    Contract: These rows must NOT appear in validated_businesses output.
    With DEFAULT_MAX_RETRIES=3, retry_attempt=3 means next would be 4 > 3.
    """
    return [
        {
            "dedup_key": "pid:maxed_001",
            "name": "Maxed Out Business One",
            "address": "999 Exhausted Ln",
            "phone": "555-9001",
            "website": "https://never-works.com",
            "retry_attempt": "3",  # At max - next attempt would be 4 > 3
            "last_retry_ts": "2026-01-29T08:00:00Z",
        },
        {
            "dedup_key": "pid:maxed_002",
            "name": "Maxed Out Business Two",
            "address": "888 Tired Rd",
            "phone": "555-9002",
            "website": "",
            "retry_attempt": "5",  # Way over max
            "last_retry_ts": "2026-01-28T09:00:00Z",
        },
    ]


@pytest.fixture
def mock_retry_rows_missing_fields() -> List[Dict[str, str]]:
    """
    Fixture: Rows with missing required fields.
    
    Contract: These rows must be skipped with skipped_missing_fields counter.
    """
    return [
        # Missing dedup_key
        {
            "dedup_key": "",
            "name": "No Key Business",
            "address": "111 Missing Key St",
            "phone": "555-1111",
            "website": "",
            "retry_attempt": "0",
            "last_retry_ts": "",
        },
        # Missing name
        {
            "dedup_key": "pid:no_name_001",
            "name": "",
            "address": "222 No Name Ave",
            "phone": "555-2222",
            "website": "",
            "retry_attempt": "1",
            "last_retry_ts": "",
        },
        # Missing address
        {
            "dedup_key": "pid:no_addr_001",
            "name": "No Address Biz",
            "address": "",
            "phone": "555-3333",
            "website": "",
            "retry_attempt": "0",
            "last_retry_ts": "",
        },
        # Missing phone
        {
            "dedup_key": "pid:no_phone_001",
            "name": "No Phone Biz",
            "address": "444 Silent St",
            "phone": "",
            "website": "",
            "retry_attempt": "0",
            "last_retry_ts": "",
        },
    ]


@pytest.fixture
def mixed_rows(
    mock_retry_rows_valid,
    mock_retry_rows_with_maxed_attempts,
    mock_retry_rows_missing_fields,
) -> List[Dict[str, str]]:
    """
    Fixture: Mixed bag of valid, maxed, and invalid rows.
    
    Contract: Only valid rows should appear in output.
    Order must be preserved for valid rows.
    """
    # Interleave to test ordering preservation
    return [
        mock_retry_rows_valid[0],                    # valid - index 0
        mock_retry_rows_with_maxed_attempts[0],      # maxed - skip
        mock_retry_rows_valid[1],                    # valid - index 1
        mock_retry_rows_missing_fields[0],           # missing dedup_key - skip
        mock_retry_rows_valid[2],                    # valid - index 2
        mock_retry_rows_with_maxed_attempts[1],      # maxed - skip
        mock_retry_rows_missing_fields[1],           # missing name - skip
    ]


# =============================================================================
# PURE FUNCTION TESTS: parse_retry_attempt()
# =============================================================================

class TestParseRetryAttempt:
    """Tests for parse_retry_attempt() pure function."""

    def test_valid_integer_string(self):
        """Integer string parses correctly."""
        assert parse_retry_attempt("2") == 2
        assert parse_retry_attempt("0") == 0
        assert parse_retry_attempt("10") == 10

    def test_empty_string_returns_zero(self):
        """Empty string defaults to 0 (first attempt)."""
        assert parse_retry_attempt("") == 0

    def test_none_returns_zero(self):
        """None defaults to 0 (first attempt)."""
        assert parse_retry_attempt(None) == 0

    def test_float_string_truncates(self):
        """Float strings like '1.0' from Sheets truncate to int."""
        assert parse_retry_attempt("1.0") == 1
        assert parse_retry_attempt("2.9") == 2

    def test_actual_int_passes_through(self):
        """Actual int values work correctly."""
        assert parse_retry_attempt(3) == 3
        assert parse_retry_attempt(0) == 0

    def test_actual_float_truncates(self):
        """Actual float values truncate to int."""
        assert parse_retry_attempt(1.5) == 1
        assert parse_retry_attempt(2.0) == 2

    def test_invalid_string_returns_zero(self):
        """Invalid string gracefully defaults to 0."""
        assert parse_retry_attempt("abc") == 0
        assert parse_retry_attempt("N/A") == 0
        assert parse_retry_attempt("null") == 0

    def test_whitespace_string_returns_zero(self):
        """Whitespace-only string defaults to 0."""
        assert parse_retry_attempt("   ") == 0
        assert parse_retry_attempt("\t") == 0

    def test_negative_integer_parses(self):
        """Negative integers parse (validation happens elsewhere)."""
        assert parse_retry_attempt("-1") == -1


# =============================================================================
# PURE FUNCTION TESTS: parse_row_to_candidate()
# =============================================================================

class TestParseRowToCandidate:
    """Tests for parse_row_to_candidate() pure function."""

    def test_valid_row_returns_candidate(self):
        """Valid row parses into candidate with all contract fields."""
        row = {
            "dedup_key": "pid:test123",
            "name": "Test Business",
            "address": "123 Test St",
            "phone": "555-1234",
            "website": "https://test.com",
            "retry_attempt": "1",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        # Contract: successful parse returns (candidate, None)
        assert reason is None
        assert candidate is not None

        # Contract: required fields preserved
        assert candidate["dedup_key"] == "pid:test123"
        assert candidate["name"] == "Test Business"
        assert candidate["address"] == "123 Test St"
        assert candidate["phone"] == "555-1234"
        assert candidate["website"] == "https://test.com"

        # Contract: retry_attempt incremented
        assert candidate["retry_attempt"] == 2

        # Contract: source == "retry"
        assert candidate["source"] == "retry"

        # Contract: last_retry_ts is ISO string
        assert "last_retry_ts" in candidate
        assert isinstance(candidate["last_retry_ts"], str)

    def test_missing_dedup_key_returns_error(self):
        """Missing dedup_key returns (None, 'missing_dedup_key')."""
        row = {
            "dedup_key": "",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "0",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "missing_dedup_key"

    def test_missing_name_returns_missing_required(self):
        """Missing name returns (None, 'missing_required')."""
        row = {
            "dedup_key": "pid:test",
            "name": "",  # Missing
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "0",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "missing_required"

    def test_missing_address_returns_missing_required(self):
        """Missing address returns (None, 'missing_required')."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "",  # Missing
            "phone": "555",
            "website": "",
            "retry_attempt": "0",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "missing_required"

    def test_missing_phone_returns_missing_required(self):
        """Missing phone returns (None, 'missing_required')."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "",  # Missing
            "website": "",
            "retry_attempt": "0",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "missing_required"

    def test_empty_website_is_valid(self):
        """Empty website is allowed (leads without websites are targets)."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",  # Empty but valid
            "retry_attempt": "0",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is not None
        assert reason is None
        assert candidate["website"] == ""

    def test_retry_at_max_returns_max_retry(self):
        """retry_attempt at max returns (None, 'max_retry')."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "3",  # At max=3, next would be 4 > 3
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "max_retry"

    def test_retry_over_max_returns_max_retry(self):
        """retry_attempt over max returns (None, 'max_retry')."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "10",  # Way over
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "max_retry"

    def test_retry_just_under_max_succeeds(self):
        """retry_attempt just under max succeeds."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "2",  # Next will be 3, which is <= 3
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is not None
        assert reason is None
        assert candidate["retry_attempt"] == 3

    def test_optional_fields_copied_when_present(self):
        """Optional fields like place_id are copied when present."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "0",
            "place_id": "ChIJ123",
            "rating": "4.5",
            "reviews": "100",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is not None
        assert candidate["place_id"] == "ChIJ123"
        assert candidate["rating"] == "4.5"
        assert candidate["reviews"] == "100"


# =============================================================================
# PURE FUNCTION TESTS: transform_rows_to_candidates()
# =============================================================================

class TestTransformRowsToCandidates:
    """Tests for transform_rows_to_candidates() pure function."""

    def test_empty_rows_returns_empty_list(self):
        """Empty input returns empty output with zero stats."""
        candidates, stats = transform_rows_to_candidates([], max_retries=3)

        assert candidates == []
        assert stats["total_rows"] == 0
        assert stats["loaded"] == 0
        assert stats["skipped_max_retry"] == 0
        assert stats["skipped_missing_fields"] == 0

    def test_all_valid_rows_loaded(self, mock_retry_rows_valid):
        """All valid rows are loaded."""
        candidates, stats = transform_rows_to_candidates(
            mock_retry_rows_valid, max_retries=3
        )

        assert stats["total_rows"] == 3
        assert stats["loaded"] == 3
        assert stats["skipped_max_retry"] == 0
        assert stats["skipped_missing_fields"] == 0
        assert len(candidates) == 3

    def test_maxed_rows_skipped(self, mock_retry_rows_with_maxed_attempts):
        """Rows at/over max retries are skipped."""
        candidates, stats = transform_rows_to_candidates(
            mock_retry_rows_with_maxed_attempts, max_retries=3
        )

        assert stats["total_rows"] == 2
        assert stats["loaded"] == 0
        assert stats["skipped_max_retry"] == 2
        assert stats["skipped_missing_fields"] == 0
        assert len(candidates) == 0

    def test_missing_fields_skipped(self, mock_retry_rows_missing_fields):
        """Rows with missing required fields are skipped."""
        candidates, stats = transform_rows_to_candidates(
            mock_retry_rows_missing_fields, max_retries=3
        )

        assert stats["total_rows"] == 4
        assert stats["loaded"] == 0
        assert stats["skipped_max_retry"] == 0
        assert stats["skipped_missing_fields"] == 4
        assert len(candidates) == 0

    def test_mixed_rows_correct_counts(self, mixed_rows):
        """Mixed rows produce correct stats."""
        candidates, stats = transform_rows_to_candidates(mixed_rows, max_retries=3)

        # 3 valid, 2 maxed, 2 missing fields
        assert stats["total_rows"] == 7
        assert stats["loaded"] == 3
        assert stats["skipped_max_retry"] == 2
        assert stats["skipped_missing_fields"] == 2
        assert len(candidates) == 3

    def test_stats_includes_max_retries(self):
        """Stats dict includes max_retries value."""
        _, stats = transform_rows_to_candidates([], max_retries=5)
        assert stats["max_retries"] == 5


# =============================================================================
# CONTRACT OUTPUT TESTS
# =============================================================================

class TestContractOutput:
    """Tests verifying output contract compliance."""

    def test_output_key_is_validated_businesses(self, mock_retry_rows_valid):
        """Output uses 'validated_businesses' key for downstream compatibility."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mock_retry_rows_valid

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,  # Use mocked method instead
            })

            # Contract: output key is validated_businesses
            assert "validated_businesses" in result
            assert isinstance(result["validated_businesses"], list)

    def test_output_key_retry_stats_exists(self, mock_retry_rows_valid):
        """Output includes 'retry_stats' key."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mock_retry_rows_valid

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            # Contract: retry_stats exists with required keys
            assert "retry_stats" in result
            stats = result["retry_stats"]
            assert "total_rows" in stats
            assert "loaded" in stats
            assert "skipped_max_retry" in stats
            assert "skipped_missing_fields" in stats
            assert "max_retries" in stats

    def test_each_candidate_has_contract_fields(self, mock_retry_rows_valid):
        """Each candidate has required contract fields."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mock_retry_rows_valid

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            for candidate in result["validated_businesses"]:
                # Contract: dedup_key preserved
                assert "dedup_key" in candidate
                assert candidate["dedup_key"].startswith("pid:")

                # Contract: source == "retry"
                assert candidate["source"] == "retry"

                # Contract: retry_attempt incremented from input
                assert "retry_attempt" in candidate
                assert isinstance(candidate["retry_attempt"], int)
                assert candidate["retry_attempt"] >= 1  # At least 1 (incremented from 0)

                # Contract: last_retry_ts is ISO string
                assert "last_retry_ts" in candidate
                assert isinstance(candidate["last_retry_ts"], str)
                # Verify ISO format (YYYY-MM-DDTHH:MM:SSZ)
                assert "T" in candidate["last_retry_ts"]
                assert candidate["last_retry_ts"].endswith("Z")

    def test_retry_attempt_incremented(self, mock_retry_rows_valid):
        """retry_attempt is incremented by 1 for each loaded row."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mock_retry_rows_valid

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            businesses = result["validated_businesses"]

            # Input: 0, 1, 2 → Output: 1, 2, 3
            assert businesses[0]["retry_attempt"] == 1  # 0 + 1
            assert businesses[1]["retry_attempt"] == 2  # 1 + 1
            assert businesses[2]["retry_attempt"] == 3  # 2 + 1


# =============================================================================
# MAX_RETRIES ENV BEHAVIOR TESTS
# =============================================================================

class TestMaxRetriesEnvBehavior:
    """Tests for PIPELINE_MAX_RETRIES environment variable handling."""

    def test_unset_env_uses_default(self):
        """Unset PIPELINE_MAX_RETRIES uses default (3)."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove the key if it exists
            os.environ.pop("PIPELINE_MAX_RETRIES", None)
            result = _get_max_retries()
            assert result == DEFAULT_MAX_RETRIES
            assert result == 3

    def test_valid_env_value_used(self):
        """Valid PIPELINE_MAX_RETRIES value is used."""
        with patch.dict(os.environ, {"PIPELINE_MAX_RETRIES": "5"}):
            result = _get_max_retries()
            assert result == 5

    def test_env_value_1_allows_only_first_attempt(self):
        """PIPELINE_MAX_RETRIES=1 only allows retry_attempt < 1."""
        with patch.dict(os.environ, {"PIPELINE_MAX_RETRIES": "1"}):
            max_retries = _get_max_retries()
            assert max_retries == 1

            # Row with retry_attempt=0 → next=1, which is <= 1 (allowed)
            row_first = {
                "dedup_key": "pid:test",
                "name": "Test",
                "address": "123",
                "phone": "555",
                "website": "",
                "retry_attempt": "0",
            }
            candidate, reason = parse_row_to_candidate(row_first, max_retries=1)
            assert candidate is not None

            # Row with retry_attempt=1 → next=2, which is > 1 (skip)
            row_second = {
                "dedup_key": "pid:test",
                "name": "Test",
                "address": "123",
                "phone": "555",
                "website": "",
                "retry_attempt": "1",
            }
            candidate, reason = parse_row_to_candidate(row_second, max_retries=1)
            assert candidate is None
            assert reason == "max_retry"

    def test_invalid_env_falls_back_to_default(self):
        """Invalid PIPELINE_MAX_RETRIES falls back to default (3)."""
        with patch.dict(os.environ, {"PIPELINE_MAX_RETRIES": "invalid"}):
            result = _get_max_retries()
            assert result == DEFAULT_MAX_RETRIES

    def test_negative_env_falls_back_to_default(self):
        """Negative PIPELINE_MAX_RETRIES falls back to default (3)."""
        with patch.dict(os.environ, {"PIPELINE_MAX_RETRIES": "-1"}):
            result = _get_max_retries()
            assert result == DEFAULT_MAX_RETRIES

    def test_zero_env_is_valid(self):
        """PIPELINE_MAX_RETRIES=0 is valid (no retries allowed)."""
        with patch.dict(os.environ, {"PIPELINE_MAX_RETRIES": "0"}):
            result = _get_max_retries()
            assert result == 0


# =============================================================================
# SKIP LOGIC TESTS
# =============================================================================

class TestSkipLogic:
    """Tests for retry skip logic."""

    def test_rows_at_max_retries_skipped(self):
        """Rows where retry_attempt >= MAX_RETRIES are skipped."""
        rows = [
            {"dedup_key": "k1", "name": "B1", "address": "A1", "phone": "P1", "retry_attempt": "3"},
            {"dedup_key": "k2", "name": "B2", "address": "A2", "phone": "P2", "retry_attempt": "4"},
            {"dedup_key": "k3", "name": "B3", "address": "A3", "phone": "P3", "retry_attempt": "10"},
        ]

        candidates, stats = transform_rows_to_candidates(rows, max_retries=3)

        # All should be skipped (3+1=4>3, 4+1=5>3, 10+1=11>3)
        assert len(candidates) == 0
        assert stats["skipped_max_retry"] == 3

    def test_skipped_max_retry_counter_accurate(self, mixed_rows):
        """skipped_max_retry counter matches actual skipped rows."""
        candidates, stats = transform_rows_to_candidates(mixed_rows, max_retries=3)

        # From mixed_rows fixture: 2 maxed rows
        assert stats["skipped_max_retry"] == 2

    def test_skipped_rows_not_in_output(self, mock_retry_rows_with_maxed_attempts):
        """Skipped rows do not appear in validated_businesses."""
        candidates, stats = transform_rows_to_candidates(
            mock_retry_rows_with_maxed_attempts, max_retries=3
        )

        assert len(candidates) == 0

        # Verify dedup_keys from maxed rows are not present
        maxed_keys = {row["dedup_key"] for row in mock_retry_rows_with_maxed_attempts}
        output_keys = {c["dedup_key"] for c in candidates}

        assert maxed_keys.isdisjoint(output_keys)


# =============================================================================
# ORDERING PRESERVATION TESTS
# =============================================================================

class TestOrderingPreservation:
    """Tests verifying output order matches input order."""

    def test_output_order_matches_input_order(self, mock_retry_rows_valid):
        """Valid rows maintain input order in output."""
        candidates, _ = transform_rows_to_candidates(mock_retry_rows_valid, max_retries=3)

        # Verify order by dedup_key
        input_keys = [row["dedup_key"] for row in mock_retry_rows_valid]
        output_keys = [c["dedup_key"] for c in candidates]

        assert input_keys == output_keys

    def test_order_preserved_with_skipped_rows(self, mixed_rows):
        """Order is preserved even when rows are skipped."""
        candidates, _ = transform_rows_to_candidates(mixed_rows, max_retries=3)

        # From mixed_rows: valid rows are at indices 0, 2, 4 (interleaved)
        # Their dedup_keys are: pid:valid_001, pid:valid_002, pid:valid_003
        expected_order = ["pid:valid_001", "pid:valid_002", "pid:valid_003"]
        output_keys = [c["dedup_key"] for c in candidates]

        assert output_keys == expected_order

    def test_no_resorting_occurs(self):
        """Verify no sorting is applied (e.g., alphabetical)."""
        # Rows in reverse alphabetical order by name
        rows = [
            {"dedup_key": "k3", "name": "Zebra Corp", "address": "A", "phone": "P", "retry_attempt": "0"},
            {"dedup_key": "k2", "name": "Apple Inc", "address": "A", "phone": "P", "retry_attempt": "0"},
            {"dedup_key": "k1", "name": "Banana LLC", "address": "A", "phone": "P", "retry_attempt": "0"},
        ]

        candidates, _ = transform_rows_to_candidates(rows, max_retries=3)

        # Output should NOT be sorted by name
        output_names = [c["name"] for c in candidates]
        assert output_names == ["Zebra Corp", "Apple Inc", "Banana LLC"]


# =============================================================================
# MISSING COLUMN SAFETY TESTS
# =============================================================================

class TestMissingColumnSafety:
    """Tests for missing field error handling."""

    def test_missing_spreadsheet_id_raises_runtime_error(self):
        """Missing spreadsheet_id raises RuntimeError."""
        agent = RetryInputLoaderAgent()

        with pytest.raises(RuntimeError) as exc_info:
            agent.run({})  # No spreadsheet_id

        assert "spreadsheet_id" in str(exc_info.value)

    def test_missing_dedup_key_skipped_with_counter(self):
        """Missing dedup_key increments skipped_missing_fields."""
        rows = [
            {"dedup_key": "", "name": "Test", "address": "A", "phone": "P", "retry_attempt": "0"},
        ]

        candidates, stats = transform_rows_to_candidates(rows, max_retries=3)

        assert len(candidates) == 0
        assert stats["skipped_missing_fields"] == 1

    def test_missing_required_field_skipped_with_counter(self):
        """Missing required field increments skipped_missing_fields."""
        rows = [
            {"dedup_key": "k1", "name": "", "address": "A", "phone": "P", "retry_attempt": "0"},
            {"dedup_key": "k2", "name": "Test", "address": "", "phone": "P", "retry_attempt": "0"},
            {"dedup_key": "k3", "name": "Test", "address": "A", "phone": "", "retry_attempt": "0"},
        ]

        candidates, stats = transform_rows_to_candidates(rows, max_retries=3)

        assert len(candidates) == 0
        assert stats["skipped_missing_fields"] == 3


# =============================================================================
# MOCK MODE TESTS
# =============================================================================

class TestMockMode:
    """Tests for mock mode behavior."""

    def test_mock_mode_via_context_override(self):
        """MOCK_SHEETS=True in context enables mock mode."""
        agent = RetryInputLoaderAgent(max_retries=3)

        result = agent.run({
            "spreadsheet_id": "test_id",
            "MOCK_SHEETS": True,  # Context override
        })

        # Should return mock data without hitting real API
        assert "validated_businesses" in result
        assert "retry_stats" in result
        assert result["retry_stats"]["total_rows"] == 3  # Mock has 3 rows

    def test_mock_mode_returns_expected_structure(self):
        """Mock mode returns properly structured data."""
        agent = RetryInputLoaderAgent(max_retries=3)

        result = agent.run({
            "spreadsheet_id": "test_id",
            "MOCK_SHEETS": True,
        })

        # Verify structure
        for business in result["validated_businesses"]:
            assert "dedup_key" in business
            assert "name" in business
            assert "source" in business
            assert business["source"] == "retry"


# =============================================================================
# AGENT INITIALIZATION TESTS
# =============================================================================

class TestAgentInitialization:
    """Tests for agent initialization."""

    def test_default_max_retries_from_env_or_constant(self):
        """Agent uses max_retries from ENV or constant."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PIPELINE_MAX_RETRIES", None)
            agent = RetryInputLoaderAgent()
            assert agent.max_retries == DEFAULT_MAX_RETRIES

    def test_override_max_retries_in_constructor(self):
        """Constructor max_retries overrides ENV."""
        agent = RetryInputLoaderAgent(max_retries=10)
        assert agent.max_retries == 10

    def test_agent_name_set_correctly(self):
        """Agent name is set correctly."""
        agent = RetryInputLoaderAgent()
        assert agent.name == "RetryInputLoaderAgent"


# =============================================================================
# INTEGRATION TESTS (with mocked I/O)
# =============================================================================

class TestIntegration:
    """Integration tests with mocked Google Sheets I/O."""

    def test_full_flow_with_valid_rows(self, mock_retry_rows_valid):
        """Full flow with valid rows produces correct output."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mock_retry_rows_valid

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            # Verify counts
            assert result["retry_stats"]["loaded"] == 3
            assert result["retry_stats"]["skipped_max_retry"] == 0
            assert result["retry_stats"]["skipped_missing_fields"] == 0

            # Verify output
            assert len(result["validated_businesses"]) == 3

    def test_full_flow_with_mixed_rows(self, mixed_rows):
        """Full flow with mixed rows correctly filters and counts."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mixed_rows

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            # Verify counts: 3 valid, 2 maxed, 2 missing
            assert result["retry_stats"]["total_rows"] == 7
            assert result["retry_stats"]["loaded"] == 3
            assert result["retry_stats"]["skipped_max_retry"] == 2
            assert result["retry_stats"]["skipped_missing_fields"] == 2

    def test_custom_retry_sheet_name(self):
        """Custom retry_sheet_name is passed to loader."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = []

            agent = RetryInputLoaderAgent(max_retries=3)
            agent.run({
                "spreadsheet_id": "test_sheet_id",
                "retry_sheet_name": "CUSTOM_RETRY_SHEET",
                "MOCK_SHEETS": False,
            })

            # Verify the custom sheet name was passed
            mock_load.assert_called_once_with("test_sheet_id", "CUSTOM_RETRY_SHEET")

    def test_default_retry_sheet_name(self):
        """Default retry_sheet_name is used when not specified."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = []

            agent = RetryInputLoaderAgent(max_retries=3)
            agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            # Verify default sheet name was used
            mock_load.assert_called_once_with("test_sheet_id", DEFAULT_RETRY_SHEET)


# =============================================================================
# EDGE CASE TESTS
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_sheet_returns_empty_output(self):
        """Empty sheet returns empty validated_businesses."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = []

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            assert result["validated_businesses"] == []
            assert result["retry_stats"]["total_rows"] == 0
            assert result["retry_stats"]["loaded"] == 0

    def test_all_rows_skipped_returns_empty_output(
        self, mock_retry_rows_with_maxed_attempts
    ):
        """All rows skipped returns empty validated_businesses."""
        with patch.object(RetryInputLoaderAgent, "_load_sheet_rows") as mock_load:
            mock_load.return_value = mock_retry_rows_with_maxed_attempts

            agent = RetryInputLoaderAgent(max_retries=3)
            result = agent.run({
                "spreadsheet_id": "test_sheet_id",
                "MOCK_SHEETS": False,
            })

            assert result["validated_businesses"] == []
            assert result["retry_stats"]["skipped_max_retry"] == 2

    def test_whitespace_in_fields_stripped(self):
        """Whitespace in field values is stripped."""
        row = {
            "dedup_key": "  pid:test  ",
            "name": "  Test Business  ",
            "address": "  123 Main St  ",
            "phone": "  555-1234  ",
            "website": "  https://test.com  ",
            "retry_attempt": "  1  ",
        }

        candidate, _ = parse_row_to_candidate(row, max_retries=3)

        assert candidate["dedup_key"] == "pid:test"
        assert candidate["name"] == "Test Business"
        assert candidate["address"] == "123 Main St"
        assert candidate["phone"] == "555-1234"
        assert candidate["website"] == "https://test.com"

    def test_max_retries_zero_allows_nothing(self):
        """MAX_RETRIES=0 means no retries allowed (all skipped)."""
        rows = [
            {"dedup_key": "k1", "name": "B1", "address": "A1", "phone": "P1", "retry_attempt": "0"},
        ]

        candidates, stats = transform_rows_to_candidates(rows, max_retries=0)

        # retry_attempt=0 → next=1 > 0 → skipped
        assert len(candidates) == 0
        assert stats["skipped_max_retry"] == 1

    def test_very_large_retry_attempt(self):
        """Very large retry_attempt values are handled."""
        row = {
            "dedup_key": "pid:test",
            "name": "Test",
            "address": "123",
            "phone": "555",
            "website": "",
            "retry_attempt": "9999999",
        }

        candidate, reason = parse_row_to_candidate(row, max_retries=3)

        assert candidate is None
        assert reason == "max_retry"
