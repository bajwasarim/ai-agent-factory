"""Quick smoke test for RetryInputLoaderAgent."""
import sys
from pathlib import Path

# Ensure project root is in path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pipelines.maps_web_missing.agents.retry_input_loader_agent import (
    parse_retry_attempt,
    parse_row_to_candidate,
    transform_rows_to_candidates,
    RetryInputLoaderAgent,
)

def test_parse_retry_attempt():
    """Test retry_attempt parsing."""
    print("parse_retry_attempt tests:")
    assert parse_retry_attempt("2") == 2, "Failed: '2' -> 2"
    assert parse_retry_attempt("") == 0, "Failed: '' -> 0"
    assert parse_retry_attempt(None) == 0, "Failed: None -> 0"
    assert parse_retry_attempt("1.0") == 1, "Failed: '1.0' -> 1"
    assert parse_retry_attempt("abc") == 0, "Failed: 'abc' -> 0"
    print("  All parse_retry_attempt tests passed!")

def test_parse_row_to_candidate():
    """Test row parsing."""
    print("parse_row_to_candidate tests:")
    
    # Valid row
    valid_row = {
        "name": "Test Business",
        "address": "123 Main St",
        "phone": "555-1234",
        "website": "",
        "dedup_key": "pid:test123",
        "retry_attempt": "1",
    }
    candidate, reason = parse_row_to_candidate(valid_row, 3)
    assert candidate is not None, "Failed: Valid row should return candidate"
    assert candidate["retry_attempt"] == 2, f"Failed: retry should increment to 2, got {candidate['retry_attempt']}"
    assert candidate["source"] == "retry", "Failed: source should be 'retry'"
    print("  Valid row: PASSED")
    
    # Missing dedup_key
    missing_key = {"name": "Test", "address": "123", "phone": "555"}
    result, reason = parse_row_to_candidate(missing_key, 3)
    assert result is None and reason == "missing_dedup_key", "Failed: Missing dedup_key"
    print("  Missing dedup_key: PASSED")
    
    # At max retries (should be skipped)
    max_retry = {
        "name": "Test",
        "address": "123",
        "phone": "555",
        "dedup_key": "pid:test",
        "retry_attempt": "3",
    }
    result, reason = parse_row_to_candidate(max_retry, 3)
    assert result is None and reason == "max_retry", f"Failed: At max retries - got {reason}"
    print("  At max retries: PASSED")

def test_transform_rows():
    """Test batch transformation."""
    print("transform_rows_to_candidates tests:")
    
    rows = [
        {"name": "Biz1", "address": "A1", "phone": "P1", "dedup_key": "k1", "retry_attempt": "0"},
        {"name": "Biz2", "address": "A2", "phone": "P2", "dedup_key": "k2", "retry_attempt": "2"},
        {"name": "Biz3", "address": "A3", "phone": "P3", "dedup_key": "k3", "retry_attempt": "3"},  # Skip
        {"name": "Biz4", "address": "A4", "phone": "P4"},  # Missing dedup_key
    ]
    
    candidates, stats = transform_rows_to_candidates(rows, max_retries=3)
    assert stats["total_rows"] == 4
    assert stats["loaded"] == 2
    assert stats["skipped_max_retry"] == 1
    assert stats["skipped_missing_fields"] == 1
    print(f"  Transform: {stats['loaded']} loaded, {stats['skipped_max_retry']} max_retry, {stats['skipped_missing_fields']} missing: PASSED")

def test_agent_mock_mode():
    """Test agent in mock mode."""
    print("RetryInputLoaderAgent mock mode test:")
    agent = RetryInputLoaderAgent()
    result = agent.run({"spreadsheet_id": "test_sheet_id"})
    
    assert "validated_businesses" in result, "Missing validated_businesses"
    assert "retry_stats" in result, "Missing retry_stats"
    assert result["retry_stats"]["loaded"] == 2, f"Expected 2 loaded, got {result['retry_stats']['loaded']}"
    assert result["retry_stats"]["skipped_max_retry"] == 1, "Expected 1 skipped"
    print(f"  Mock mode: Loaded {result['retry_stats']['loaded']}, skipped {result['retry_stats']['skipped_max_retry']}: PASSED")

def test_agent_missing_spreadsheet_id():
    """Test agent fails without spreadsheet_id."""
    print("Test missing spreadsheet_id:")
    agent = RetryInputLoaderAgent()
    try:
        agent.run({})
        print("  FAILED: Should have raised RuntimeError")
    except RuntimeError as e:
        assert "spreadsheet_id" in str(e)
        print("  Raises RuntimeError: PASSED")

if __name__ == "__main__":
    test_parse_retry_attempt()
    test_parse_row_to_candidate()
    test_transform_rows()
    test_agent_mock_mode()
    test_agent_missing_spreadsheet_id()
    print("\n✅ All smoke tests passed!")
