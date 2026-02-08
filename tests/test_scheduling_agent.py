"""Tests for SchedulingAgent.

These tests validate:
- Deterministic inference (same input → same output)
- Correct category-to-scheduling mappings
- Precedence order (healthcare > home_services > auto_services)
- Contract field preservation
- Error handling for invalid inputs
"""

import copy
import pytest

from pipelines.maps_web_missing.agents.scheduling_agent import (
    SchedulingAgent,
    infer_scheduling_for_lead,
    infer_scheduling_for_leads,
    _infer_scheduling_category,
    _normalize_text,
    _check_keywords,
    HEALTHCARE_KEYWORDS,
    HOME_SERVICES_KEYWORDS,
    AUTO_SERVICES_KEYWORDS,
    CONFIDENCE_HEALTHCARE,
    CONFIDENCE_HOME_SERVICES,
    CONFIDENCE_AUTO_SERVICES,
)


# =============================================================================
# PURE INFERENCE TESTS - HEALTHCARE
# =============================================================================

class TestHealthcareInference:
    """Tests for healthcare category inference."""

    def test_healthcare_keyword_in_name_attaches_scheduling(self):
        """Healthcare keyword in name triggers scheduling block."""
        lead = {
            "name": "Downtown Dental Clinic",
            "dedup_key": "pid:123",
            "lead_route": "TARGET",
            "target_sheet": "NO_WEBSITE_TARGETS",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["required"] is True
        assert result["scheduling"]["mode"] == "appointment"
        assert result["scheduling"]["urgency"] == "standard"
        assert result["scheduling"]["location_mode"] == "on_site"
        assert result["scheduling"]["inference_confidence"] == CONFIDENCE_HEALTHCARE

    def test_healthcare_keyword_in_primary_category(self):
        """Healthcare keyword in primary_category triggers scheduling."""
        lead = {
            "name": "Smith & Associates",
            "primary_category": "Medical Clinic",
            "dedup_key": "pid:456",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["mode"] == "appointment"

    def test_healthcare_keyword_in_enrichment_detected_industry(self):
        """Healthcare keyword in enrichment.detected_industry triggers scheduling."""
        lead = {
            "name": "Wellness Center",
            "enrichment": {"detected_industry": "healthcare"},
            "dedup_key": "pid:789",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["required"] is True


# =============================================================================
# PURE INFERENCE TESTS - HOME SERVICES
# =============================================================================

class TestHomeServicesInference:
    """Tests for home services category inference."""

    def test_home_services_keyword_attaches_scheduling(self):
        """Home services keyword triggers estimate_request scheduling."""
        lead = {
            "name": "ABC Plumbing Services",
            "dedup_key": "pid:home1",
            "lead_route": "TARGET",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["required"] is True
        assert result["scheduling"]["mode"] == "estimate_request"
        assert result["scheduling"]["urgency"] == "flexible"
        assert result["scheduling"]["location_mode"] == "off_site"
        assert result["scheduling"]["inference_confidence"] == CONFIDENCE_HOME_SERVICES

    def test_home_services_landscaping_keyword(self):
        """Landscaping keyword triggers home services scheduling."""
        lead = {
            "name": "Green Lawn Landscaping",
            "dedup_key": "pid:home2",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["mode"] == "estimate_request"


# =============================================================================
# PURE INFERENCE TESTS - AUTO SERVICES
# =============================================================================

class TestAutoServicesInference:
    """Tests for auto services category inference."""

    def test_auto_services_keyword_attaches_scheduling(self):
        """Auto services keyword triggers dropoff scheduling."""
        lead = {
            "name": "Quick Oil Change Center",
            "dedup_key": "pid:auto1",
            "lead_route": "TARGET",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["required"] is True
        assert result["scheduling"]["mode"] == "dropoff"
        assert result["scheduling"]["urgency"] == "standard"
        assert result["scheduling"]["location_mode"] == "on_site"
        assert result["scheduling"]["inference_confidence"] == CONFIDENCE_AUTO_SERVICES

    def test_auto_services_mechanic_keyword(self):
        """Mechanic keyword triggers auto services scheduling."""
        lead = {
            "name": "Joe's Auto Mechanic Shop",
            "dedup_key": "pid:auto2",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["mode"] == "dropoff"


# =============================================================================
# NO INFERENCE TESTS
# =============================================================================

class TestNoInference:
    """Tests for leads that should NOT get scheduling blocks."""

    def test_no_keyword_match_returns_lead_unchanged(self):
        """Lead with no matching keywords has no scheduling block."""
        lead = {
            "name": "Generic Retail Store",
            "primary_category": "Shopping",
            "dedup_key": "pid:retail1",
            "lead_route": "TARGET",
            "target_sheet": "NO_WEBSITE_TARGETS",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" not in result
        # All original fields preserved
        assert result["name"] == lead["name"]
        assert result["primary_category"] == lead["primary_category"]
        assert result["dedup_key"] == lead["dedup_key"]

    def test_empty_lead_returns_unchanged(self):
        """Empty lead dict returns unchanged."""
        lead = {}

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" not in result
        assert result == {}


# =============================================================================
# PRECEDENCE TESTS
# =============================================================================

class TestPrecedence:
    """Tests for inference precedence order."""

    def test_healthcare_takes_precedence_over_home_services(self):
        """When both healthcare and home services keywords present, healthcare wins."""
        lead = {
            "name": "Dental Cleaning Services",  # "dental" (healthcare) + "cleaning" (home)
            "dedup_key": "pid:precedence1",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        # Healthcare wins
        assert result["scheduling"]["mode"] == "appointment"
        assert result["scheduling"]["urgency"] == "standard"
        assert "healthcare" in result["scheduling"]["inferred_from"]

    def test_healthcare_takes_precedence_over_auto_services(self):
        """When both healthcare and auto keywords present, healthcare wins."""
        lead = {
            "name": "Veterinary Auto Clinic",  # "vet" (healthcare) + "auto" (auto)
            "primary_category": "veterinary",
            "dedup_key": "pid:precedence2",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        assert result["scheduling"]["mode"] == "appointment"
        assert "healthcare" in result["scheduling"]["inferred_from"]

    def test_home_services_takes_precedence_over_auto_services(self):
        """When both home and auto keywords present, home services wins."""
        lead = {
            "name": "Mobile Detailing and Cleaning",  # "detailing" (auto) + "cleaning" (home)
            "dedup_key": "pid:precedence3",
        }

        result = infer_scheduling_for_lead(lead)

        assert "scheduling" in result
        # Home services wins over auto
        assert result["scheduling"]["mode"] == "estimate_request"
        assert "home_services" in result["scheduling"]["inferred_from"]


# =============================================================================
# DETERMINISM TESTS
# =============================================================================

class TestDeterminism:
    """Tests for deterministic behavior."""

    def test_same_input_produces_identical_output(self):
        """Same lead produces deep-equal output on multiple runs."""
        lead = {
            "name": "City Dental Care",
            "phone": "555-1234",
            "address": "100 Main St",
            "dedup_key": "pid:determ1",
            "lead_route": "TARGET",
        }

        result1 = infer_scheduling_for_lead(lead)
        result2 = infer_scheduling_for_lead(lead)

        assert result1 == result2
        assert result1["scheduling"] == result2["scheduling"]

    def test_input_dict_not_mutated(self):
        """Input lead dict is not mutated by inference."""
        lead = {
            "name": "Downtown Dentist",
            "dedup_key": "pid:immutable1",
        }
        original = copy.deepcopy(lead)

        _ = infer_scheduling_for_lead(lead)

        assert lead == original
        assert "scheduling" not in lead

    def test_non_inferred_lead_not_mutated(self):
        """Non-inferred lead is not mutated."""
        lead = {
            "name": "Coffee Shop",
            "dedup_key": "pid:immutable2",
        }
        original = copy.deepcopy(lead)

        result = infer_scheduling_for_lead(lead)

        assert lead == original
        # Result should be equal but not same object for safety
        assert result == lead


# =============================================================================
# BATCH INFERENCE TESTS
# =============================================================================

class TestBatchInference:
    """Tests for batch lead processing."""

    def test_batch_preserves_list_length(self):
        """Batch inference preserves number of leads."""
        leads = [
            {"name": "Dental Clinic", "dedup_key": "1"},
            {"name": "Coffee Shop", "dedup_key": "2"},
            {"name": "Plumber Pro", "dedup_key": "3"},
        ]

        results = infer_scheduling_for_leads(leads)

        assert len(results) == 3

    def test_batch_preserves_ordering(self):
        """Batch inference preserves lead ordering."""
        leads = [
            {"name": "First Dental", "dedup_key": "first"},
            {"name": "Second Retail", "dedup_key": "second"},
            {"name": "Third Mechanic", "dedup_key": "third"},
        ]

        results = infer_scheduling_for_leads(leads)

        assert results[0]["dedup_key"] == "first"
        assert results[1]["dedup_key"] == "second"
        assert results[2]["dedup_key"] == "third"

    def test_batch_mixed_inference(self):
        """Batch with mixed inferred/non-inferred leads."""
        leads = [
            {"name": "Dental Office", "dedup_key": "1"},     # inferred
            {"name": "Book Store", "dedup_key": "2"},        # not inferred
            {"name": "Auto Repair", "dedup_key": "3"},       # inferred
            {"name": "Art Gallery", "dedup_key": "4"},       # not inferred
        ]

        results = infer_scheduling_for_leads(leads)

        assert "scheduling" in results[0]  # dental
        assert "scheduling" not in results[1]  # book store
        assert "scheduling" in results[2]  # auto repair
        assert "scheduling" not in results[3]  # art gallery

    def test_empty_batch_returns_empty_list(self):
        """Empty input list returns empty output list."""
        results = infer_scheduling_for_leads([])

        assert results == []


# =============================================================================
# AGENT CONTRACT TESTS
# =============================================================================

class TestAgentContract:
    """Tests for SchedulingAgent.run() contract."""

    def test_missing_enriched_leads_raises_value_error(self):
        """Missing enriched_leads key raises ValueError."""
        agent = SchedulingAgent()

        with pytest.raises(ValueError, match="enriched_leads.*missing"):
            agent.run({})

    def test_non_list_enriched_leads_raises_value_error(self):
        """Non-list enriched_leads raises ValueError."""
        agent = SchedulingAgent()

        with pytest.raises(ValueError, match="must be a list"):
            agent.run({"enriched_leads": "not a list"})

    def test_valid_input_returns_scheduled_leads(self):
        """Valid input returns dict with scheduled_leads key."""
        agent = SchedulingAgent()
        input_data = {
            "enriched_leads": [
                {"name": "Test Dental", "dedup_key": "1"},
            ]
        }

        result = agent.run(input_data)

        assert "scheduled_leads" in result
        assert isinstance(result["scheduled_leads"], list)
        assert len(result["scheduled_leads"]) == 1

    def test_agent_processes_enriched_leads_correctly(self):
        """Agent correctly processes leads and attaches scheduling."""
        agent = SchedulingAgent()
        input_data = {
            "enriched_leads": [
                {"name": "Family Dentist", "dedup_key": "d1"},
                {"name": "Grocery Store", "dedup_key": "g1"},
            ]
        }

        result = agent.run(input_data)

        scheduled = result["scheduled_leads"]
        assert "scheduling" in scheduled[0]  # dentist
        assert "scheduling" not in scheduled[1]  # grocery


# =============================================================================
# CONTRACT PRESERVATION TESTS
# =============================================================================

class TestContractPreservation:
    """Tests for pipeline contract field preservation."""

    def test_dedup_key_preserved(self):
        """dedup_key is preserved exactly."""
        lead = {
            "name": "Test Dental",
            "dedup_key": "pid:ChIJ_exact_value_123",
        }

        result = infer_scheduling_for_lead(lead)

        assert result["dedup_key"] == "pid:ChIJ_exact_value_123"

    def test_lead_route_preserved(self):
        """lead_route is preserved exactly."""
        lead = {
            "name": "Test Dental",
            "dedup_key": "1",
            "lead_route": "TARGET",
        }

        result = infer_scheduling_for_lead(lead)

        assert result["lead_route"] == "TARGET"

    def test_target_sheet_preserved(self):
        """target_sheet is preserved exactly."""
        lead = {
            "name": "Test Dental",
            "dedup_key": "1",
            "target_sheet": "NO_WEBSITE_TARGETS",
        }

        result = infer_scheduling_for_lead(lead)

        assert result["target_sheet"] == "NO_WEBSITE_TARGETS"

    def test_all_contract_fields_preserved_together(self):
        """All contract fields preserved in combination."""
        lead = {
            "name": "Complete Dental Practice",
            "dedup_key": "pid:complete_123",
            "lead_route": "TARGET",
            "target_sheet": "NO_WEBSITE_TARGETS",
            "has_real_website": False,
            "website_status": "missing",
            "quality": {"completeness_score": 0.9},
            "enrichment": {"detected_industry": "healthcare"},
        }

        result = infer_scheduling_for_lead(lead)

        # All original fields preserved
        assert result["dedup_key"] == lead["dedup_key"]
        assert result["lead_route"] == lead["lead_route"]
        assert result["target_sheet"] == lead["target_sheet"]
        assert result["has_real_website"] == lead["has_real_website"]
        assert result["website_status"] == lead["website_status"]
        assert result["quality"] == lead["quality"]
        assert result["enrichment"] == lead["enrichment"]
        # Plus scheduling block added
        assert "scheduling" in result


# =============================================================================
# INFERRED_FROM TRACEABILITY TESTS
# =============================================================================

class TestInferredFromTraceability:
    """Tests for inferred_from field format and content."""

    def test_inferred_from_format_healthcare(self):
        """Healthcare inferred_from follows format: industry:healthcare:<keyword>."""
        lead = {
            "name": "City Dentist",
            "dedup_key": "1",
        }

        result = infer_scheduling_for_lead(lead)

        inferred_from = result["scheduling"]["inferred_from"]
        assert inferred_from.startswith("industry:healthcare:")
        assert "dentist" in inferred_from

    def test_inferred_from_format_home_services(self):
        """Home services inferred_from follows format: industry:home_services:<keyword>."""
        lead = {
            "name": "Pro Plumbing",
            "dedup_key": "1",
        }

        result = infer_scheduling_for_lead(lead)

        inferred_from = result["scheduling"]["inferred_from"]
        assert inferred_from.startswith("industry:home_services:")
        assert "plumbing" in inferred_from

    def test_inferred_from_format_auto_services(self):
        """Auto services inferred_from follows format: industry:auto_services:<keyword>."""
        lead = {
            "name": "Quick Tire Shop",
            "dedup_key": "1",
        }

        result = infer_scheduling_for_lead(lead)

        inferred_from = result["scheduling"]["inferred_from"]
        assert inferred_from.startswith("industry:auto_services:")
        assert "tire" in inferred_from

    def test_inferred_from_contains_matched_keyword(self):
        """inferred_from contains the exact matched keyword."""
        lead = {
            "name": "Family Orthodontist Office",
            "dedup_key": "1",
        }

        result = infer_scheduling_for_lead(lead)

        inferred_from = result["scheduling"]["inferred_from"]
        # Should contain "orthodontist" as matched keyword
        assert "orthodontist" in inferred_from
