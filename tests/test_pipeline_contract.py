"""
Pipeline Contract Tests.

Verifies that critical fields pass through the pipeline unchanged.
This test guards the pipeline ABI - any future agent change that breaks
dedup_key, lead_route, or target_sheet propagation will fail CI.

Contract Invariants:
    1. dedup_key is computed ONCE upstream, never recomputed downstream
    2. lead_route and target_sheet pass through formatter unchanged
    3. routed_leads ordering is stable: TARGET → EXCLUDED → RETRY
"""

import pytest

from pipelines.maps_web_missing.agents.lead_formatter_agent import LeadFormatterAgent
from pipelines.maps_web_missing.agents.lead_router_agent import (
    LeadRouterAgent,
    route_leads,
)
from fixtures.sample_formatted_leads import (
    generate_mock_leads,
    generate_validated_businesses,
)


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

    def test_formatter_preserves_routing_fields_from_fixtures(self):
        """
        Contract test: Formatter must preserve dedup_key, lead_route, target_sheet
        from fixture-generated leads (simulates real pipeline data).
        """
        # Arrange: Use fixture-generated leads
        input_leads = generate_mock_leads(target_count=2, excluded_count=2, retry_count=1)

        input_data = {
            "routed_leads": input_leads,
            "query": "fixture test",
            "location": "Fixture City",
        }

        # Act
        formatter = LeadFormatterAgent()
        result = formatter.run(input_data)

        # Assert: All leads processed
        assert len(result["formatted_leads"]) == 5

        # Assert: Contract fields preserved exactly
        for i, output_lead in enumerate(result["formatted_leads"]):
            input_lead = input_leads[i]
            assert output_lead["dedup_key"] == input_lead["dedup_key"], (
                f"Lead {i}: dedup_key mutated from '{input_lead['dedup_key']}' "
                f"to '{output_lead['dedup_key']}'"
            )
            assert output_lead["lead_route"] == input_lead["lead_route"], (
                f"Lead {i}: lead_route mutated from '{input_lead['lead_route']}' "
                f"to '{output_lead['lead_route']}'"
            )
            assert output_lead["target_sheet"] == input_lead["target_sheet"], (
                f"Lead {i}: target_sheet mutated from '{input_lead['target_sheet']}' "
                f"to '{output_lead['target_sheet']}'"
            )


class TestLeadRouterContract:
    """Tests for LeadRouterAgent contract preservation."""

    def test_router_outputs_stable_ordering_target_excluded_retry(self):
        """
        Contract test: routed_leads must be ordered TARGET → EXCLUDED → RETRY.

        This ordering is critical for:
        - Deterministic exports
        - Debug traceability
        - Fan-out export sheet ordering
        """
        # Arrange: Mixed input order (intentionally scrambled)
        businesses = generate_validated_businesses(
            target_count=2,
            excluded_count=2,
            retry_count=2,
        )

        # Scramble the order
        import random
        random.seed(42)  # Deterministic shuffle for test reproducibility
        scrambled = businesses.copy()
        random.shuffle(scrambled)

        input_data = {"validated_businesses": scrambled}

        # Act
        router = LeadRouterAgent()
        result = router.run(input_data)

        # Assert: routed_leads is a flat list
        routed_leads = result["routed_leads"]
        assert isinstance(routed_leads, list), "routed_leads must be a flat list"

        # Assert: Order is TARGET → EXCLUDED → RETRY
        routes_in_order = [lead["lead_route"] for lead in routed_leads]

        # Find transition points
        target_end = None
        excluded_end = None

        for i, route in enumerate(routes_in_order):
            if route == "TARGET":
                if target_end is not None and i > target_end:
                    pytest.fail(
                        f"TARGET lead found at index {i} after non-TARGET leads. "
                        f"Order must be TARGET → EXCLUDED → RETRY. Got: {routes_in_order}"
                    )
            elif route == "EXCLUDED":
                if target_end is None:
                    target_end = i
                if excluded_end is not None and i > excluded_end:
                    pytest.fail(
                        f"EXCLUDED lead found at index {i} after RETRY leads. "
                        f"Order must be TARGET → EXCLUDED → RETRY. Got: {routes_in_order}"
                    )
            elif route == "RETRY":
                if target_end is None:
                    target_end = i
                if excluded_end is None:
                    excluded_end = i

        # Verify we have leads from all categories
        assert "TARGET" in routes_in_order, "Expected TARGET leads in output"
        assert "EXCLUDED" in routes_in_order, "Expected EXCLUDED leads in output"
        assert "RETRY" in routes_in_order, "Expected RETRY leads in output"

    def test_router_preserves_dedup_key_unchanged(self):
        """
        Contract test: Router must preserve dedup_key from input unchanged.
        dedup_key is computed ONCE by BusinessNormalizeAgent.
        """
        # Arrange
        businesses = generate_validated_businesses(target_count=1, excluded_count=1, retry_count=1)
        original_dedup_keys = {b["place_id"]: b["dedup_key"] for b in businesses}

        input_data = {"validated_businesses": businesses}

        # Act
        router = LeadRouterAgent()
        result = router.run(input_data)

        # Assert: dedup_key unchanged for each lead
        for lead in result["routed_leads"]:
            place_id = lead["place_id"]
            assert lead["dedup_key"] == original_dedup_keys[place_id], (
                f"Router mutated dedup_key for place_id={place_id}. "
                f"Expected '{original_dedup_keys[place_id]}', got '{lead['dedup_key']}'"
            )

    def test_router_appends_lead_route_and_target_sheet(self):
        """
        Contract test: Router must append lead_route and target_sheet to each lead.
        """
        # Arrange
        businesses = generate_validated_businesses(target_count=1, excluded_count=1, retry_count=1)
        input_data = {"validated_businesses": businesses}

        # Act
        router = LeadRouterAgent()
        result = router.run(input_data)

        # Assert: Each lead has routing fields
        for lead in result["routed_leads"]:
            assert "lead_route" in lead, f"Missing lead_route for {lead.get('name')}"
            assert "target_sheet" in lead, f"Missing target_sheet for {lead.get('name')}"
            assert lead["lead_route"] in ("TARGET", "EXCLUDED", "RETRY"), (
                f"Invalid lead_route '{lead['lead_route']}' for {lead.get('name')}"
            )
            assert lead["target_sheet"] in (
                "NO_WEBSITE_TARGETS",
                "HAS_WEBSITE_EXCLUDED",
                "WEBSITE_CHECK_ERRORS",
            ), f"Invalid target_sheet '{lead['target_sheet']}' for {lead.get('name')}"

    def test_router_returns_routing_stats(self):
        """
        Contract test: Router must return routing_stats with correct counts.
        """
        # Arrange
        businesses = generate_validated_businesses(target_count=3, excluded_count=2, retry_count=1)
        input_data = {"validated_businesses": businesses}

        # Act
        router = LeadRouterAgent()
        result = router.run(input_data)

        # Assert: routing_stats exists and has correct structure
        assert "routing_stats" in result, "Missing routing_stats in router output"

        stats = result["routing_stats"]
        assert stats["target"] == 3, f"Expected 3 targets, got {stats['target']}"
        assert stats["excluded"] == 2, f"Expected 2 excluded, got {stats['excluded']}"
        assert stats["retry"] == 1, f"Expected 1 retry, got {stats['retry']}"
        assert stats["total"] == 6, f"Expected 6 total, got {stats['total']}"

    def test_route_leads_pure_function_ordering(self):
        """
        Contract test: route_leads() pure function maintains TARGET → EXCLUDED → RETRY order.
        """
        # Arrange: Create leads that will route to different categories
        leads = [
            {"has_real_website": False, "website_status": "error", "name": "Retry1"},
            {"has_real_website": True, "website_status": "valid", "name": "Excluded1"},
            {"has_real_website": False, "website_status": "missing", "name": "Target1"},
            {"has_real_website": False, "website_status": "invalid", "name": "Target2"},
            {"has_real_website": True, "website_status": "valid", "name": "Excluded2"},
        ]

        # Act
        result = route_leads(leads)

        # Assert: Correct categorization
        assert len(result["targets"]) == 2, "Expected 2 targets"
        assert len(result["excluded"]) == 2, "Expected 2 excluded"
        assert len(result["retry"]) == 1, "Expected 1 retry"

        # Assert: All targets have correct route
        for lead in result["targets"]:
            assert lead["lead_route"] == "TARGET"
            assert lead["target_sheet"] == "NO_WEBSITE_TARGETS"

        for lead in result["excluded"]:
            assert lead["lead_route"] == "EXCLUDED"
            assert lead["target_sheet"] == "HAS_WEBSITE_EXCLUDED"

        for lead in result["retry"]:
            assert lead["lead_route"] == "RETRY"
            assert lead["target_sheet"] == "WEBSITE_CHECK_ERRORS"
