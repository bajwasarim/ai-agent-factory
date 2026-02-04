"""Tests for EnrichmentAggregatorAgent.

These tests validate:
- Business size estimation heuristics
- Industry detection via keyword matching
- Digital maturity scoring
- Deterministic enrichment
- Contract field preservation
"""

import pytest
from pipelines.maps_web_missing.agents.enrichment_aggregator_agent import (
    EnrichmentAggregatorAgent,
    enrich_single_lead,
    enrich_leads,
    estimate_business_size,
    detect_industry,
    compute_digital_maturity,
    _parse_review_count,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def scored_lead_restaurant():
    """A scored lead for a restaurant business."""
    return {
        "name": "Joe's Pizza Restaurant",
        "website": "https://joespizza.com",
        "phone": "+1-555-123-4567",
        "address": "123 Main St, New York, NY 10001",
        "place_id": "ChIJ123456789",
        "dedup_key": "pid:ChIJ123456789",
        "category": "Italian Restaurant",
        "description": "Family owned pizzeria serving authentic Italian food",
        "rating": "4.5",
        "reviews": "250",
        "location": "New York, NY",
        "source": "google_maps",
        "has_real_website": True,
        "website_status": "valid",
        "website_checked_at": "2026-02-04T12:00:00Z",
        "lead_route": "EXCLUDED",
        "target_sheet": "HAS_WEBSITE_EXCLUDED",
        "quality": {
            "completeness_score": 0.95,
            "confidence_score": 0.92,
            "contactability_score": 1.0,
            "location_confidence": 1.0,
        },
    }


@pytest.fixture
def scored_lead_dental():
    """A scored lead for a dental office."""
    return {
        "name": "Bright Smile Dental Clinic",
        "website": "",
        "phone": "+1-555-987-6543",
        "address": "456 Oak Ave, Austin, TX 78701",
        "place_id": "ChIJ987654321",
        "dedup_key": "pid:ChIJ987654321",
        "category": "Dentist",
        "description": "General dentistry and dental care",
        "rating": "4.8",
        "reviews": "85",
        "location": "Austin, TX",
        "source": "google_maps",
        "has_real_website": False,
        "website_status": "missing",
        "website_checked_at": "2026-02-04T12:00:00Z",
        "lead_route": "TARGET",
        "target_sheet": "NO_WEBSITE_TARGETS",
        "quality": {
            "completeness_score": 0.85,
            "confidence_score": 0.88,
            "contactability_score": 0.8,
            "location_confidence": 1.0,
        },
    }


@pytest.fixture
def scored_lead_minimal():
    """A minimal scored lead with few fields."""
    return {
        "name": "Unknown Business",
        "dedup_key": "hash:abc123",
        "lead_route": "TARGET",
        "target_sheet": "NO_WEBSITE_TARGETS",
        "quality": {
            "completeness_score": 0.2,
            "confidence_score": 0.5,
            "contactability_score": 0.0,
            "location_confidence": 0.0,
        },
    }


@pytest.fixture
def scored_leads_batch(scored_lead_restaurant, scored_lead_dental, scored_lead_minimal):
    """A batch of scored leads."""
    return [scored_lead_restaurant, scored_lead_dental, scored_lead_minimal]


# =============================================================================
# REVIEW COUNT PARSING TESTS
# =============================================================================

class TestParseReviewCount:
    """Tests for _parse_review_count helper."""

    def test_parse_integer(self):
        assert _parse_review_count(150) == 150

    def test_parse_float(self):
        assert _parse_review_count(150.7) == 150

    def test_parse_string_number(self):
        assert _parse_review_count("150") == 150

    def test_parse_string_with_text(self):
        assert _parse_review_count("150 reviews") == 150

    def test_parse_string_with_comma(self):
        assert _parse_review_count("1,500") == 1500

    def test_parse_k_notation(self):
        assert _parse_review_count("1.5K") == 1500

    def test_parse_k_notation_lowercase(self):
        assert _parse_review_count("2k") == 2000

    def test_parse_none_returns_none(self):
        assert _parse_review_count(None) is None

    def test_parse_empty_string_returns_none(self):
        assert _parse_review_count("") is None

    def test_parse_non_numeric_returns_none(self):
        assert _parse_review_count("many reviews") is None


# =============================================================================
# BUSINESS SIZE ESTIMATION TESTS
# =============================================================================

class TestEstimateBusinessSize:
    """Tests for business size estimation."""

    def test_large_business(self):
        """500+ reviews = large."""
        lead = {"reviews": "600"}
        assert estimate_business_size(lead) == "large"

    def test_medium_business(self):
        """100-499 reviews = medium."""
        lead = {"reviews": "250"}
        assert estimate_business_size(lead) == "medium"

    def test_small_business(self):
        """10-99 reviews = small."""
        lead = {"reviews": "45"}
        assert estimate_business_size(lead) == "small"

    def test_unknown_low_reviews(self):
        """< 10 reviews = unknown."""
        lead = {"reviews": "5"}
        assert estimate_business_size(lead) == "unknown"

    def test_unknown_no_reviews(self):
        """No reviews field = unknown."""
        lead = {"name": "Business"}
        assert estimate_business_size(lead) == "unknown"

    def test_unknown_invalid_reviews(self):
        """Invalid reviews = unknown."""
        lead = {"reviews": "not a number"}
        assert estimate_business_size(lead) == "unknown"

    def test_k_notation_large(self):
        """1.5K reviews = large."""
        lead = {"reviews": "1.5K"}
        assert estimate_business_size(lead) == "large"


# =============================================================================
# INDUSTRY DETECTION TESTS
# =============================================================================

class TestDetectIndustry:
    """Tests for industry detection via keyword matching."""

    def test_detect_restaurant(self, scored_lead_restaurant):
        """Restaurant keywords detected."""
        industry, confidence = detect_industry(scored_lead_restaurant)
        assert industry == "restaurant"
        assert confidence > 0.5

    def test_detect_healthcare(self, scored_lead_dental):
        """Healthcare/dental keywords detected."""
        industry, confidence = detect_industry(scored_lead_dental)
        assert industry == "healthcare"
        assert confidence > 0.5

    def test_unknown_for_minimal(self, scored_lead_minimal):
        """Minimal lead has unknown industry."""
        industry, confidence = detect_industry(scored_lead_minimal)
        assert industry == "unknown"
        assert confidence == 0.0

    def test_multiple_keywords_higher_confidence(self):
        """More keyword matches = higher confidence."""
        lead_one_keyword = {"category": "restaurant"}
        lead_many_keywords = {
            "name": "Pizza Kitchen",
            "category": "Italian Restaurant",
            "description": "Authentic pizzeria and cafe",
        }

        _, conf_one = detect_industry(lead_one_keyword)
        _, conf_many = detect_industry(lead_many_keywords)

        assert conf_many > conf_one

    def test_case_insensitive_matching(self):
        """Keyword matching is case insensitive."""
        lead = {"category": "RESTAURANT"}
        industry, _ = detect_industry(lead)
        assert industry == "restaurant"

    def test_automotive_detection(self):
        """Automotive keywords detected."""
        lead = {"name": "Joe's Auto Repair", "category": "Mechanic"}
        industry, confidence = detect_industry(lead)
        assert industry == "automotive"
        assert confidence > 0.5

    def test_retail_detection(self):
        """Retail keywords detected."""
        lead = {"name": "Fashion Boutique", "category": "Clothing Store"}
        industry, confidence = detect_industry(lead)
        assert industry == "retail"
        assert confidence > 0.5


# =============================================================================
# DIGITAL MATURITY TESTS
# =============================================================================

class TestComputeDigitalMaturity:
    """Tests for digital maturity scoring."""

    def test_high_maturity_with_real_website(self, scored_lead_restaurant):
        """Real website increases digital maturity."""
        score = compute_digital_maturity(scored_lead_restaurant)
        assert score >= 0.7

    def test_lower_maturity_no_website(self, scored_lead_dental):
        """No website = lower digital maturity."""
        score = compute_digital_maturity(scored_lead_dental)
        assert score < 0.7

    def test_digital_keywords_increase_score(self):
        """Digital-positive keywords increase score."""
        lead_basic = {"name": "Business", "has_real_website": False}
        lead_digital = {
            "name": "Business",
            "has_real_website": False,
            "description": "Online booking and delivery available",
        }

        score_basic = compute_digital_maturity(lead_basic)
        score_digital = compute_digital_maturity(lead_digital)

        assert score_digital > score_basic

    def test_negative_keywords_decrease_score(self):
        """Digital-negative keywords decrease score."""
        lead_basic = {"name": "Business", "has_real_website": False}
        lead_cash = {
            "name": "Business",
            "has_real_website": False,
            "description": "Cash only, walk-in only",
        }

        score_basic = compute_digital_maturity(lead_basic)
        score_cash = compute_digital_maturity(lead_cash)

        assert score_cash < score_basic

    def test_score_clamped_to_valid_range(self):
        """Score is always 0.0-1.0."""
        lead = {"name": "Test", "has_real_website": True, "website": "https://example.com"}
        score = compute_digital_maturity(lead)
        assert 0.0 <= score <= 1.0


# =============================================================================
# ENRICH SINGLE LEAD TESTS
# =============================================================================

class TestEnrichSingleLead:
    """Tests for enrich_single_lead function."""

    def test_adds_enrichment_block(self, scored_lead_restaurant):
        """Enriching adds enrichment block."""
        enriched = enrich_single_lead(scored_lead_restaurant)
        assert "enrichment" in enriched
        assert isinstance(enriched["enrichment"], dict)

    def test_enrichment_block_has_all_fields(self, scored_lead_restaurant):
        """Enrichment block contains all required fields."""
        enriched = enrich_single_lead(scored_lead_restaurant)
        enrichment = enriched["enrichment"]

        assert "business_size_estimate" in enrichment
        assert "industry_confidence" in enrichment
        assert "digital_maturity_score" in enrichment
        assert "detected_industry" in enrichment

    def test_preserves_original_fields(self, scored_lead_restaurant):
        """All original fields are preserved."""
        enriched = enrich_single_lead(scored_lead_restaurant)

        for key in scored_lead_restaurant:
            assert key in enriched
            assert enriched[key] == scored_lead_restaurant[key]

    def test_preserves_quality_block(self, scored_lead_restaurant):
        """Quality block is preserved unchanged."""
        enriched = enrich_single_lead(scored_lead_restaurant)
        assert enriched["quality"] == scored_lead_restaurant["quality"]

    def test_does_not_modify_input(self, scored_lead_restaurant):
        """Input lead is not mutated."""
        original = dict(scored_lead_restaurant)
        enrich_single_lead(scored_lead_restaurant)

        assert scored_lead_restaurant == original
        assert "enrichment" not in scored_lead_restaurant


# =============================================================================
# BATCH ENRICHMENT TESTS
# =============================================================================

class TestEnrichLeads:
    """Tests for enrich_leads batch function."""

    def test_enriches_all_leads(self, scored_leads_batch):
        """All leads in batch are enriched."""
        enriched = enrich_leads(scored_leads_batch)
        assert len(enriched) == len(scored_leads_batch)

    def test_preserves_order(self, scored_leads_batch):
        """Lead ordering is preserved."""
        enriched = enrich_leads(scored_leads_batch)

        for i, lead in enumerate(scored_leads_batch):
            assert enriched[i]["name"] == lead["name"]

    def test_empty_list_returns_empty(self):
        """Empty input returns empty output."""
        enriched = enrich_leads([])
        assert enriched == []


# =============================================================================
# DETERMINISM TESTS
# =============================================================================

class TestDeterminism:
    """Verify enrichment is deterministic."""

    def test_same_input_same_output(self, scored_lead_restaurant):
        """Same input always produces same enrichment."""
        enriched1 = enrich_single_lead(scored_lead_restaurant)
        enriched2 = enrich_single_lead(scored_lead_restaurant)

        assert enriched1["enrichment"] == enriched2["enrichment"]

    def test_deterministic_across_calls(self, scored_lead_restaurant):
        """Multiple calls produce identical results."""
        results = [enrich_single_lead(scored_lead_restaurant) for _ in range(10)]

        first_enrichment = results[0]["enrichment"]
        for result in results[1:]:
            assert result["enrichment"] == first_enrichment


# =============================================================================
# CONTRACT PRESERVATION TESTS
# =============================================================================

class TestContractPreservation:
    """Verify critical contract fields are preserved."""

    def test_dedup_key_unchanged(self, scored_lead_restaurant):
        """dedup_key is never modified."""
        original_dedup_key = scored_lead_restaurant["dedup_key"]
        enriched = enrich_single_lead(scored_lead_restaurant)

        assert enriched["dedup_key"] == original_dedup_key

    def test_lead_route_unchanged(self, scored_lead_restaurant):
        """lead_route is never modified."""
        original_route = scored_lead_restaurant["lead_route"]
        enriched = enrich_single_lead(scored_lead_restaurant)

        assert enriched["lead_route"] == original_route

    def test_target_sheet_unchanged(self, scored_lead_restaurant):
        """target_sheet is never modified."""
        original_sheet = scored_lead_restaurant["target_sheet"]
        enriched = enrich_single_lead(scored_lead_restaurant)

        assert enriched["target_sheet"] == original_sheet


# =============================================================================
# AGENT TESTS
# =============================================================================

class TestEnrichmentAggregatorAgent:
    """Tests for EnrichmentAggregatorAgent class."""

    def test_agent_initialization(self):
        """Agent initializes correctly."""
        agent = EnrichmentAggregatorAgent()
        assert agent.name == "EnrichmentAggregatorAgent"

    def test_run_returns_enriched_leads(self, scored_leads_batch):
        """Agent run returns enriched_leads key."""
        agent = EnrichmentAggregatorAgent()
        result = agent.run({"scored_leads": scored_leads_batch})

        assert "enriched_leads" in result
        assert len(result["enriched_leads"]) == len(scored_leads_batch)

    def test_run_missing_scored_leads_raises(self):
        """Missing scored_leads raises ValueError."""
        agent = EnrichmentAggregatorAgent()

        with pytest.raises(ValueError) as exc_info:
            agent.run({})

        assert "scored_leads" in str(exc_info.value)
        assert "contract violation" in str(exc_info.value).lower()

    def test_run_wrong_type_raises(self):
        """Non-list scored_leads raises ValueError."""
        agent = EnrichmentAggregatorAgent()

        with pytest.raises(ValueError) as exc_info:
            agent.run({"scored_leads": "not a list"})

        assert "must be a list" in str(exc_info.value)

    def test_run_empty_list_succeeds(self):
        """Empty lead list is valid input."""
        agent = EnrichmentAggregatorAgent()
        result = agent.run({"scored_leads": []})

        assert result["enriched_leads"] == []

    def test_run_preserves_all_contracts(self, scored_lead_restaurant):
        """Agent run preserves all contract fields."""
        agent = EnrichmentAggregatorAgent()
        result = agent.run({"scored_leads": [scored_lead_restaurant]})

        enriched = result["enriched_leads"][0]
        assert enriched["dedup_key"] == scored_lead_restaurant["dedup_key"]
        assert enriched["lead_route"] == scored_lead_restaurant["lead_route"]
        assert enriched["target_sheet"] == scored_lead_restaurant["target_sheet"]
        assert enriched["quality"] == scored_lead_restaurant["quality"]
