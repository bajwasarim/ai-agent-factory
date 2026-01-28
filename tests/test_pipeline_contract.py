"""
Pipeline Contract Tests.

Verifies that critical fields pass through the pipeline unchanged.
This test guards the pipeline ABI - any future agent change that breaks
dedup_key, lead_route, or target_sheet propagation will fail CI.
"""

import pytest
from pipelines.maps_web_missing.agents.lead_formatter_agent import LeadFormatterAgent


class TestLeadFormatterContract:
    """Tests for LeadFormatterAgent contract preservation."""

    def test_contract_fields_pass_through_unchanged(self):
        """
        Contract test: dedup_key, lead_route, target_sheet must pass through
        the formatter unchanged with no mutation or recomputation.
        """
        # Arrange: Input lead with contract fields from upstream agents
        input_lead = {
            "name": "Test Business",
            "phone": "555-123-4567",
            "address": "123 Test St",
            "place_id": "ChIJ_test_id",
            "website": "",
            # Contract fields (must be preserved exactly)
            "dedup_key": "pid:ChIJ_test_id",
            "lead_route": "TARGET",
            "target_sheet": "NO_WEBSITE_TARGETS",
            # Website validation fields
            "has_real_website": False,
            "website_status": "missing",
            "website_checked_at": "2026-01-28T12:00:00Z",
        }

        input_data = {
            "routed_leads": [input_lead],
            "query": "test query",
            "location": "Test City",
        }

        # Act
        formatter = LeadFormatterAgent()
        result = formatter.run(input_data)

        # Assert: formatted_leads exists
        assert "formatted_leads" in result
        assert len(result["formatted_leads"]) == 1

        output_lead = result["formatted_leads"][0]

        # Assert: Contract fields unchanged
        assert output_lead["dedup_key"] == "pid:ChIJ_test_id", "dedup_key must be preserved unchanged"
        assert output_lead["lead_route"] == "TARGET", "lead_route must be preserved unchanged"
        assert output_lead["target_sheet"] == "NO_WEBSITE_TARGETS", "target_sheet must be preserved unchanged"

    def test_missing_dedup_key_raises_contract_violation(self):
        """
        Contract test: Missing dedup_key must raise ValueError.
        This is a pipeline logic fault that must fail fast.
        """
        # Arrange: Lead missing dedup_key (contract violation)
        input_lead = {
            "name": "Test Business",
            "phone": "555-123-4567",
            "address": "123 Test St",
            "place_id": "ChIJ_test_id",
            # NO dedup_key - contract violation
            "lead_route": "TARGET",
            "target_sheet": "NO_WEBSITE_TARGETS",
        }

        input_data = {
            "routed_leads": [input_lead],
            "query": "test query",
            "location": "Test City",
        }

        # Act & Assert
        formatter = LeadFormatterAgent()
        with pytest.raises(ValueError) as exc_info:
            formatter.run(input_data)

        assert "Pipeline contract violation" in str(exc_info.value)
        assert "dedup_key missing" in str(exc_info.value)
        assert "ChIJ_test_id" in str(exc_info.value)  # Should include place_id for debugging

    def test_all_routing_categories_preserve_contract(self):
        """
        Contract test: All routing categories (TARGET, EXCLUDED, RETRY)
        must preserve contract fields.
        """
        # Arrange: Leads from all three routing categories
        input_leads = [
            {
                "name": "Target Business",
                "place_id": "place_1",
                "dedup_key": "pid:place_1",
                "lead_route": "TARGET",
                "target_sheet": "NO_WEBSITE_TARGETS",
                "has_real_website": False,
                "website_status": "missing",
            },
            {
                "name": "Excluded Business",
                "place_id": "place_2",
                "dedup_key": "pid:place_2",
                "lead_route": "EXCLUDED",
                "target_sheet": "HAS_WEBSITE_EXCLUDED",
                "has_real_website": True,
                "website_status": "valid",
            },
            {
                "name": "Retry Business",
                "place_id": "place_3",
                "dedup_key": "pid:place_3",
                "lead_route": "RETRY",
                "target_sheet": "WEBSITE_CHECK_ERRORS",
                "has_real_website": False,
                "website_status": "error",
            },
        ]

        input_data = {
            "routed_leads": input_leads,
            "query": "test query",
            "location": "Test City",
        }

        # Act
        formatter = LeadFormatterAgent()
        result = formatter.run(input_data)

        # Assert: All leads processed
        assert len(result["formatted_leads"]) == 3

        # Assert: Each lead preserves its contract fields
        for i, output_lead in enumerate(result["formatted_leads"]):
            input_lead = input_leads[i]
            assert output_lead["dedup_key"] == input_lead["dedup_key"], f"Lead {i}: dedup_key mutated"
            assert output_lead["lead_route"] == input_lead["lead_route"], f"Lead {i}: lead_route mutated"
            assert output_lead["target_sheet"] == input_lead["target_sheet"], f"Lead {i}: target_sheet mutated"

    def test_empty_routed_leads_returns_empty_formatted_leads(self):
        """Contract test: Empty input returns empty output without errors."""
        input_data = {
            "routed_leads": [],
            "query": "test query",
            "location": "Test City",
        }

        formatter = LeadFormatterAgent()
        result = formatter.run(input_data)

        assert result["formatted_leads"] == []
        assert result["summary"]["total_leads"] == 0
