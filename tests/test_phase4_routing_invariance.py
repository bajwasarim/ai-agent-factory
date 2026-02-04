"""Tests for Phase 4 routing invariance.

These tests verify that Phase 4 agents (LeadScoringAgent, EnrichmentAggregatorAgent)
do NOT modify routing fields. This is a critical invariant:

    Routing is based on VALIDATION, not enrichment.

If routing logic needs to change based on enrichment, that's a new milestone.
"""

import pytest
from pipelines.maps_web_missing.agents.lead_scoring_agent import LeadScoringAgent
from pipelines.maps_web_missing.agents.enrichment_aggregator_agent import EnrichmentAggregatorAgent
from pipelines.maps_web_missing.agents.lead_router_agent import LeadRouterAgent


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def validated_leads_target():
    """Validated leads that should route to TARGET."""
    return [
        {
            "name": "No Website Business",
            "website": "",
            "phone": "+1-555-123-4567",
            "address": "123 Main St",
            "place_id": "ChIJ111",
            "dedup_key": "pid:ChIJ111",
            "has_real_website": False,
            "website_status": "missing",
            "website_checked_at": "2026-02-04T12:00:00Z",
        },
        {
            "name": "Invalid Website Business",
            "website": "https://facebook.com/page",
            "phone": "+1-555-222-3333",
            "address": "456 Oak Ave",
            "place_id": "ChIJ222",
            "dedup_key": "pid:ChIJ222",
            "has_real_website": False,
            "website_status": "invalid",
            "website_checked_at": "2026-02-04T12:00:00Z",
        },
    ]


@pytest.fixture
def validated_leads_excluded():
    """Validated leads that should route to EXCLUDED."""
    return [
        {
            "name": "Has Website Business",
            "website": "https://example.com",
            "phone": "+1-555-333-4444",
            "address": "789 Pine Blvd",
            "place_id": "ChIJ333",
            "dedup_key": "pid:ChIJ333",
            "has_real_website": True,
            "website_status": "valid",
            "website_checked_at": "2026-02-04T12:00:00Z",
        },
    ]


@pytest.fixture
def validated_leads_retry():
    """Validated leads that should route to RETRY."""
    return [
        {
            "name": "Error Business",
            "website": "https://error.example.com",
            "phone": "+1-555-444-5555",
            "address": "999 Error Ln",
            "place_id": "ChIJ444",
            "dedup_key": "pid:ChIJ444",
            "has_real_website": False,
            "website_status": "error",
            "website_checked_at": "2026-02-04T12:00:00Z",
        },
    ]


@pytest.fixture
def all_validated_leads(validated_leads_target, validated_leads_excluded, validated_leads_retry):
    """All validated leads combined."""
    return validated_leads_target + validated_leads_excluded + validated_leads_retry


# =============================================================================
# ROUTING INVARIANCE TESTS
# =============================================================================

class TestRoutingInvarianceThroughPhase4:
    """Verify that Phase 4 agents do NOT modify routing fields."""

    def test_lead_route_unchanged_after_scoring(self, all_validated_leads):
        """lead_route is identical before and after scoring."""
        # First route the leads
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        # Record original routes
        original_routes = [(lead["dedup_key"], lead["lead_route"]) for lead in routed_leads]

        # Score the leads
        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        # Verify routes unchanged
        for i, lead in enumerate(scored_leads):
            expected_key, expected_route = original_routes[i]
            assert lead["dedup_key"] == expected_key
            assert lead["lead_route"] == expected_route, (
                f"Lead {lead['dedup_key']}: lead_route changed from "
                f"{expected_route} to {lead['lead_route']} after scoring"
            )

    def test_lead_route_unchanged_after_enrichment(self, all_validated_leads):
        """lead_route is identical before and after enrichment."""
        # Route the leads
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        # Score the leads
        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        # Record routes after scoring
        routes_after_scoring = [(lead["dedup_key"], lead["lead_route"]) for lead in scored_leads]

        # Enrich the leads
        enricher = EnrichmentAggregatorAgent()
        enriched_result = enricher.run({"scored_leads": scored_leads})
        enriched_leads = enriched_result["enriched_leads"]

        # Verify routes unchanged after enrichment
        for i, lead in enumerate(enriched_leads):
            expected_key, expected_route = routes_after_scoring[i]
            assert lead["dedup_key"] == expected_key
            assert lead["lead_route"] == expected_route, (
                f"Lead {lead['dedup_key']}: lead_route changed from "
                f"{expected_route} to {lead['lead_route']} after enrichment"
            )

    def test_target_sheet_unchanged_through_phase4(self, all_validated_leads):
        """target_sheet is identical through entire Phase 4."""
        # Route
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        original_sheets = {lead["dedup_key"]: lead["target_sheet"] for lead in routed_leads}

        # Score
        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        # Enrich
        enricher = EnrichmentAggregatorAgent()
        enriched_result = enricher.run({"scored_leads": scored_leads})
        enriched_leads = enriched_result["enriched_leads"]

        # Verify
        for lead in enriched_leads:
            expected_sheet = original_sheets[lead["dedup_key"]]
            assert lead["target_sheet"] == expected_sheet, (
                f"Lead {lead['dedup_key']}: target_sheet changed from "
                f"{expected_sheet} to {lead['target_sheet']} through Phase 4"
            )

    def test_dedup_key_unchanged_through_phase4(self, all_validated_leads):
        """dedup_key is never modified through Phase 4."""
        original_keys = [lead["dedup_key"] for lead in all_validated_leads]

        # Route
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        # Score
        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        # Enrich
        enricher = EnrichmentAggregatorAgent()
        enriched_result = enricher.run({"scored_leads": scored_leads})
        enriched_leads = enriched_result["enriched_leads"]

        # Verify all keys preserved in order
        final_keys = [lead["dedup_key"] for lead in enriched_leads]
        assert final_keys == original_keys

    def test_routing_categories_preserved(self, all_validated_leads):
        """Each routing category (TARGET/EXCLUDED/RETRY) is preserved."""
        # Route
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        # Count by route
        original_counts = {}
        for lead in routed_leads:
            route = lead["lead_route"]
            original_counts[route] = original_counts.get(route, 0) + 1

        # Score + Enrich
        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        enricher = EnrichmentAggregatorAgent()
        enriched_result = enricher.run({"scored_leads": scored_leads})
        enriched_leads = enriched_result["enriched_leads"]

        # Count after Phase 4
        final_counts = {}
        for lead in enriched_leads:
            route = lead["lead_route"]
            final_counts[route] = final_counts.get(route, 0) + 1

        assert final_counts == original_counts


# =============================================================================
# PHASE 4 ADDITIVE-ONLY TESTS
# =============================================================================

class TestPhase4IsAdditive:
    """Verify Phase 4 only ADDS fields, never modifies existing ones."""

    def test_scoring_adds_quality_block(self, all_validated_leads):
        """LeadScoringAgent adds quality block, doesn't modify other fields."""
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        # Deep copy for comparison (excluding quality which will be added)
        original_fields = []
        for lead in routed_leads:
            original_fields.append({k: v for k, v in lead.items() if k != "quality"})

        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        # Verify original fields unchanged
        for i, lead in enumerate(scored_leads):
            for key, value in original_fields[i].items():
                assert lead[key] == value, f"Field '{key}' was modified by scoring"

        # Verify quality was added
        for lead in scored_leads:
            assert "quality" in lead
            assert isinstance(lead["quality"], dict)

    def test_enrichment_adds_enrichment_block(self, all_validated_leads):
        """EnrichmentAggregatorAgent adds enrichment block, doesn't modify other fields."""
        router = LeadRouterAgent()
        routed_result = router.run({"validated_businesses": all_validated_leads})
        routed_leads = routed_result["routed_leads"]

        scorer = LeadScoringAgent()
        scored_result = scorer.run({"routed_leads": routed_leads})
        scored_leads = scored_result["scored_leads"]

        # Deep copy for comparison (excluding enrichment which will be added)
        original_fields = []
        for lead in scored_leads:
            original_fields.append({k: v for k, v in lead.items() if k != "enrichment"})

        enricher = EnrichmentAggregatorAgent()
        enriched_result = enricher.run({"scored_leads": scored_leads})
        enriched_leads = enriched_result["enriched_leads"]

        # Verify original fields unchanged (including quality)
        for i, lead in enumerate(enriched_leads):
            for key, value in original_fields[i].items():
                assert lead[key] == value, f"Field '{key}' was modified by enrichment"

        # Verify enrichment was added
        for lead in enriched_leads:
            assert "enrichment" in lead
            assert isinstance(lead["enrichment"], dict)


# =============================================================================
# PIPELINE WIRING TESTS
# =============================================================================

class TestPhase4PipelineWiring:
    """Verify Phase 4 agents are wired correctly in pipeline."""

    def test_normal_pipeline_has_ten_agents(self):
        """Normal pipeline has 10 agents after Phases 4+5+6."""
        from pipelines.maps_web_missing.pipeline import _build_normal_pipeline

        pipeline = _build_normal_pipeline()
        assert len(pipeline.agents) == 10

    def test_retry_pipeline_has_nine_agents(self):
        """Retry pipeline has 9 agents after Phases 4+5+6."""
        from pipelines.maps_web_missing.pipeline import _build_retry_pipeline

        pipeline = _build_retry_pipeline()
        assert len(pipeline.agents) == 9

    def test_scoring_agent_after_router(self):
        """LeadScoringAgent comes immediately after LeadRouterAgent."""
        from pipelines.maps_web_missing.pipeline import _build_normal_pipeline

        pipeline = _build_normal_pipeline()
        agent_names = [agent.name for agent in pipeline.agents]

        router_idx = agent_names.index("LeadRouterAgent")
        scoring_idx = agent_names.index("LeadScoringAgent")

        assert scoring_idx == router_idx + 1

    def test_enrichment_agent_after_scoring(self):
        """EnrichmentAggregatorAgent comes immediately after LeadScoringAgent."""
        from pipelines.maps_web_missing.pipeline import _build_normal_pipeline

        pipeline = _build_normal_pipeline()
        agent_names = [agent.name for agent in pipeline.agents]

        scoring_idx = agent_names.index("LeadScoringAgent")
        enrichment_idx = agent_names.index("EnrichmentAggregatorAgent")

        assert enrichment_idx == scoring_idx + 1

    def test_formatter_after_enrichment(self):
        """LeadFormatterAgent comes after EnrichmentAggregatorAgent."""
        from pipelines.maps_web_missing.pipeline import _build_normal_pipeline

        pipeline = _build_normal_pipeline()
        agent_names = [agent.name for agent in pipeline.agents]

        enrichment_idx = agent_names.index("EnrichmentAggregatorAgent")
        formatter_idx = agent_names.index("LeadFormatterAgent")

        assert formatter_idx == enrichment_idx + 1

    def test_outreach_orchestrator_is_last(self):
        """OutreachOrchestrator is the last agent (Phase 6, post-landing)."""
        from pipelines.maps_web_missing.pipeline import _build_normal_pipeline

        pipeline = _build_normal_pipeline()
        assert pipeline.agents[-1].name == "OutreachOrchestrator"

    def test_landing_page_agent_before_outreach(self):
        """LandingPageGeneratorAgent is second-to-last, before outreach."""
        from pipelines.maps_web_missing.pipeline import _build_normal_pipeline

        pipeline = _build_normal_pipeline()
        assert pipeline.agents[-2].name == "LandingPageGeneratorAgent"
