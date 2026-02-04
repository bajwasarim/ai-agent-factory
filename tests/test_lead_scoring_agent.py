"""Tests for LeadScoringAgent.

These tests validate:
- Deterministic scoring (same input → same output)
- Weight constants sum correctly
- Individual score computations
- Contract field preservation
- Schema compliance
"""

import pytest
from pipelines.maps_web_missing.agents.lead_scoring_agent import (
    LeadScoringAgent,
    score_single_lead,
    score_leads,
    compute_completeness_score,
    compute_confidence_score,
    compute_contactability_score,
    compute_location_confidence,
    _has_value,
    # Weight constants for validation
    WEIGHT_HAS_PHONE,
    WEIGHT_HAS_ADDRESS,
    WEIGHT_HAS_NAME,
    WEIGHT_HAS_CATEGORY,
    WEIGHT_HAS_RATING,
    WEIGHT_HAS_PLACE_ID,
    WEIGHT_WEBSITE_STATUS_CONFIDENCE,
    WEIGHT_PLACE_ID_PRESENT,
    WEIGHT_SOURCE_RELIABILITY,
    WEIGHT_PHONE_PRESENT,
    WEIGHT_ADDRESS_PRESENT,
    WEIGHT_WEBSITE_PRESENT,
    WEIGHT_ADDRESS_EXISTS,
    WEIGHT_PLACE_ID_EXISTS,
    WEIGHT_LOCATION_EXISTS,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def complete_routed_lead():
    """A fully populated routed lead."""
    return {
        "name": "Test Business",
        "website": "https://example.com",
        "phone": "+1-555-123-4567",
        "address": "123 Main St, City, ST 12345",
        "place_id": "ChIJ123456789",
        "dedup_key": "pid:ChIJ123456789",
        "category": "restaurant",
        "rating": "4.5",
        "location": "City, ST",
        "source": "google_maps",
        "has_real_website": False,
        "website_status": "invalid",
        "website_checked_at": "2026-02-04T12:00:00Z",
        "lead_route": "TARGET",
        "target_sheet": "NO_WEBSITE_TARGETS",
    }


@pytest.fixture
def minimal_routed_lead():
    """A minimally populated routed lead."""
    return {
        "name": "Minimal Business",
        "dedup_key": "hash:abc123",
        "lead_route": "TARGET",
        "target_sheet": "NO_WEBSITE_TARGETS",
    }


@pytest.fixture
def routed_leads_batch(complete_routed_lead, minimal_routed_lead):
    """A batch of routed leads for testing."""
    return [complete_routed_lead, minimal_routed_lead]


# =============================================================================
# WEIGHT CONSTANT TESTS
# =============================================================================

class TestWeightConstants:
    """Verify weight constants are properly defined."""

    def test_completeness_weights_sum_to_one(self):
        """Completeness weights must sum to 1.0."""
        total = (
            WEIGHT_HAS_PHONE +
            WEIGHT_HAS_ADDRESS +
            WEIGHT_HAS_NAME +
            WEIGHT_HAS_CATEGORY +
            WEIGHT_HAS_RATING +
            WEIGHT_HAS_PLACE_ID
        )
        assert abs(total - 1.0) < 0.0001, f"Completeness weights sum to {total}"

    def test_confidence_weights_sum_to_one(self):
        """Confidence weights must sum to 1.0."""
        total = (
            WEIGHT_WEBSITE_STATUS_CONFIDENCE +
            WEIGHT_PLACE_ID_PRESENT +
            WEIGHT_SOURCE_RELIABILITY
        )
        assert abs(total - 1.0) < 0.0001, f"Confidence weights sum to {total}"

    def test_contactability_weights_sum_to_one(self):
        """Contactability weights must sum to 1.0."""
        total = (
            WEIGHT_PHONE_PRESENT +
            WEIGHT_ADDRESS_PRESENT +
            WEIGHT_WEBSITE_PRESENT
        )
        assert abs(total - 1.0) < 0.0001, f"Contactability weights sum to {total}"

    def test_location_weights_sum_to_one(self):
        """Location confidence weights must sum to 1.0."""
        total = (
            WEIGHT_ADDRESS_EXISTS +
            WEIGHT_PLACE_ID_EXISTS +
            WEIGHT_LOCATION_EXISTS
        )
        assert abs(total - 1.0) < 0.0001, f"Location weights sum to {total}"


# =============================================================================
# HELPER FUNCTION TESTS
# =============================================================================

class TestHasValue:
    """Tests for _has_value helper."""

    def test_none_returns_false(self):
        assert _has_value(None) is False

    def test_empty_string_returns_false(self):
        assert _has_value("") is False

    def test_whitespace_only_returns_false(self):
        assert _has_value("   ") is False

    def test_non_empty_string_returns_true(self):
        assert _has_value("value") is True

    def test_string_with_spaces_returns_true(self):
        assert _has_value("  value  ") is True

    def test_zero_returns_true(self):
        """Zero is a valid value."""
        assert _has_value(0) is True

    def test_false_boolean_returns_true(self):
        """False is a valid value (not missing)."""
        assert _has_value(False) is True

    def test_empty_list_returns_true(self):
        """Empty list is present (not None)."""
        assert _has_value([]) is True


# =============================================================================
# INDIVIDUAL SCORE FUNCTION TESTS
# =============================================================================

class TestComputeCompletenessScore:
    """Tests for completeness score computation."""

    def test_complete_lead_scores_one(self, complete_routed_lead):
        """Lead with all fields scores 1.0."""
        score = compute_completeness_score(complete_routed_lead)
        assert score == 1.0

    def test_minimal_lead_scores_low(self, minimal_routed_lead):
        """Lead with only name scores partial."""
        score = compute_completeness_score(minimal_routed_lead)
        # Only name is present
        assert score == WEIGHT_HAS_NAME

    def test_empty_lead_scores_zero(self):
        """Empty lead scores 0.0."""
        score = compute_completeness_score({})
        assert score == 0.0

    def test_phone_only_adds_phone_weight(self):
        """Phone field adds phone weight."""
        lead = {"phone": "+1-555-123-4567"}
        score = compute_completeness_score(lead)
        assert score == WEIGHT_HAS_PHONE


class TestComputeConfidenceScore:
    """Tests for confidence score computation."""

    def test_google_maps_source_high_confidence(self, complete_routed_lead):
        """Google Maps source has high reliability."""
        score = compute_confidence_score(complete_routed_lead)
        # Should include all three components
        assert score > 0.8

    def test_unknown_source_lower_confidence(self):
        """Unknown source gets default reliability."""
        lead = {
            "source": "unknown_source",
            "website_status": "valid",
            "place_id": "ChIJ123",
        }
        score = compute_confidence_score(lead)
        # Still has other factors but source is lower
        assert score > 0.5
        assert score < 1.0

    def test_error_status_low_confidence(self):
        """Error website status lowers confidence."""
        lead = {
            "source": "google_maps",
            "website_status": "error",
            "place_id": "ChIJ123",
        }
        score = compute_confidence_score(lead)
        # Error status = 0.3 confidence
        assert score < 0.8

    def test_no_place_id_lower_confidence(self):
        """Missing place_id reduces confidence."""
        lead_with = {"place_id": "ChIJ123", "source": "google_maps", "website_status": "valid"}
        lead_without = {"source": "google_maps", "website_status": "valid"}
        
        score_with = compute_confidence_score(lead_with)
        score_without = compute_confidence_score(lead_without)
        
        assert score_with > score_without


class TestComputeContactabilityScore:
    """Tests for contactability score computation."""

    def test_all_contact_info_scores_one(self, complete_routed_lead):
        """Lead with phone, address, website scores 1.0."""
        score = compute_contactability_score(complete_routed_lead)
        assert score == 1.0

    def test_phone_only_adds_phone_weight(self):
        """Phone only adds phone weight."""
        lead = {"phone": "+1-555-123-4567"}
        score = compute_contactability_score(lead)
        assert score == WEIGHT_PHONE_PRESENT

    def test_no_contact_info_scores_zero(self):
        """No contact info scores 0.0."""
        lead = {"name": "Business"}
        score = compute_contactability_score(lead)
        assert score == 0.0


class TestComputeLocationConfidence:
    """Tests for location confidence computation."""

    def test_all_location_fields_scores_one(self, complete_routed_lead):
        """Lead with address, place_id, location scores 1.0."""
        score = compute_location_confidence(complete_routed_lead)
        assert score == 1.0

    def test_address_only(self):
        """Address only adds address weight."""
        lead = {"address": "123 Main St"}
        score = compute_location_confidence(lead)
        assert score == WEIGHT_ADDRESS_EXISTS

    def test_no_location_info_scores_zero(self):
        """No location info scores 0.0."""
        lead = {"name": "Business"}
        score = compute_location_confidence(lead)
        assert score == 0.0


# =============================================================================
# SCORE SINGLE LEAD TESTS
# =============================================================================

class TestScoreSingleLead:
    """Tests for score_single_lead function."""

    def test_adds_quality_block(self, complete_routed_lead):
        """Scoring adds quality block to lead."""
        scored = score_single_lead(complete_routed_lead)
        assert "quality" in scored
        assert isinstance(scored["quality"], dict)

    def test_quality_block_has_all_scores(self, complete_routed_lead):
        """Quality block contains all four scores."""
        scored = score_single_lead(complete_routed_lead)
        quality = scored["quality"]
        
        assert "completeness_score" in quality
        assert "confidence_score" in quality
        assert "contactability_score" in quality
        assert "location_confidence" in quality

    def test_preserves_original_fields(self, complete_routed_lead):
        """All original fields are preserved."""
        scored = score_single_lead(complete_routed_lead)
        
        for key in complete_routed_lead:
            assert key in scored
            assert scored[key] == complete_routed_lead[key]

    def test_does_not_modify_input(self, complete_routed_lead):
        """Input lead is not mutated."""
        original = dict(complete_routed_lead)
        score_single_lead(complete_routed_lead)
        
        assert complete_routed_lead == original
        assert "quality" not in complete_routed_lead

    def test_scores_are_floats(self, complete_routed_lead):
        """All scores are floats."""
        scored = score_single_lead(complete_routed_lead)
        quality = scored["quality"]
        
        for score_name, score_value in quality.items():
            assert isinstance(score_value, float), f"{score_name} is not float"

    def test_scores_in_valid_range(self, complete_routed_lead):
        """All scores are between 0.0 and 1.0."""
        scored = score_single_lead(complete_routed_lead)
        quality = scored["quality"]
        
        for score_name, score_value in quality.items():
            assert 0.0 <= score_value <= 1.0, f"{score_name}={score_value} out of range"


# =============================================================================
# BATCH SCORING TESTS
# =============================================================================

class TestScoreLeads:
    """Tests for score_leads batch function."""

    def test_scores_all_leads(self, routed_leads_batch):
        """All leads in batch are scored."""
        scored = score_leads(routed_leads_batch)
        assert len(scored) == len(routed_leads_batch)

    def test_preserves_order(self, routed_leads_batch):
        """Lead ordering is preserved."""
        scored = score_leads(routed_leads_batch)
        
        for i, lead in enumerate(routed_leads_batch):
            assert scored[i]["name"] == lead["name"]

    def test_empty_list_returns_empty(self):
        """Empty input returns empty output."""
        scored = score_leads([])
        assert scored == []


# =============================================================================
# DETERMINISM TESTS
# =============================================================================

class TestDeterminism:
    """Verify scoring is deterministic."""

    def test_same_input_same_output(self, complete_routed_lead):
        """Same input always produces same scores."""
        scored1 = score_single_lead(complete_routed_lead)
        scored2 = score_single_lead(complete_routed_lead)
        
        assert scored1["quality"] == scored2["quality"]

    def test_deterministic_across_calls(self, complete_routed_lead):
        """Multiple calls produce identical results."""
        results = [score_single_lead(complete_routed_lead) for _ in range(10)]
        
        first_quality = results[0]["quality"]
        for result in results[1:]:
            assert result["quality"] == first_quality

    def test_order_independent_for_same_lead(self, routed_leads_batch):
        """Batch order doesn't affect individual scores."""
        # Score in original order
        scored_original = score_leads(routed_leads_batch)
        
        # Score in reversed order
        reversed_batch = list(reversed(routed_leads_batch))
        scored_reversed = score_leads(reversed_batch)
        
        # Same lead should have same score regardless of position
        assert scored_original[0]["quality"] == scored_reversed[1]["quality"]
        assert scored_original[1]["quality"] == scored_reversed[0]["quality"]


# =============================================================================
# CONTRACT PRESERVATION TESTS
# =============================================================================

class TestContractPreservation:
    """Verify critical contract fields are preserved."""

    def test_dedup_key_unchanged(self, complete_routed_lead):
        """dedup_key is never modified."""
        original_dedup_key = complete_routed_lead["dedup_key"]
        scored = score_single_lead(complete_routed_lead)
        
        assert scored["dedup_key"] == original_dedup_key

    def test_lead_route_unchanged(self, complete_routed_lead):
        """lead_route is never modified."""
        original_route = complete_routed_lead["lead_route"]
        scored = score_single_lead(complete_routed_lead)
        
        assert scored["lead_route"] == original_route

    def test_target_sheet_unchanged(self, complete_routed_lead):
        """target_sheet is never modified."""
        original_sheet = complete_routed_lead["target_sheet"]
        scored = score_single_lead(complete_routed_lead)
        
        assert scored["target_sheet"] == original_sheet

    def test_website_status_unchanged(self, complete_routed_lead):
        """website_status is never modified."""
        original_status = complete_routed_lead["website_status"]
        scored = score_single_lead(complete_routed_lead)
        
        assert scored["website_status"] == original_status


# =============================================================================
# AGENT TESTS
# =============================================================================

class TestLeadScoringAgent:
    """Tests for LeadScoringAgent class."""

    def test_agent_initialization(self):
        """Agent initializes correctly."""
        agent = LeadScoringAgent()
        assert agent.name == "LeadScoringAgent"

    def test_run_returns_scored_leads(self, routed_leads_batch):
        """Agent run returns scored_leads key."""
        agent = LeadScoringAgent()
        result = agent.run({"routed_leads": routed_leads_batch})
        
        assert "scored_leads" in result
        assert len(result["scored_leads"]) == len(routed_leads_batch)

    def test_run_missing_routed_leads_raises(self):
        """Missing routed_leads raises ValueError."""
        agent = LeadScoringAgent()
        
        with pytest.raises(ValueError) as exc_info:
            agent.run({})
        
        assert "routed_leads" in str(exc_info.value)
        assert "contract violation" in str(exc_info.value).lower()

    def test_run_wrong_type_raises(self):
        """Non-list routed_leads raises ValueError."""
        agent = LeadScoringAgent()
        
        with pytest.raises(ValueError) as exc_info:
            agent.run({"routed_leads": "not a list"})
        
        assert "must be a list" in str(exc_info.value)

    def test_run_empty_list_succeeds(self):
        """Empty lead list is valid input."""
        agent = LeadScoringAgent()
        result = agent.run({"routed_leads": []})
        
        assert result["scored_leads"] == []

    def test_run_preserves_all_contracts(self, complete_routed_lead):
        """Agent run preserves all contract fields."""
        agent = LeadScoringAgent()
        result = agent.run({"routed_leads": [complete_routed_lead]})
        
        scored = result["scored_leads"][0]
        assert scored["dedup_key"] == complete_routed_lead["dedup_key"]
        assert scored["lead_route"] == complete_routed_lead["lead_route"]
        assert scored["target_sheet"] == complete_routed_lead["target_sheet"]
