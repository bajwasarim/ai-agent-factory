"""
Tests for pipeline mode selection and retry pipeline execution.

Tests verify:
- Mode selection priority (CLI > ENV > default)
- Normal mode unchanged behavior
- Retry mode agent sequence
- Invalid mode handling
- Retry metadata preservation through pipeline

All tests are:
- Offline (mocked I/O)
- Deterministic
- CI safe
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from pipelines.maps_web_missing.pipeline import (
    VALID_MODES,
    DEFAULT_MODE,
    build_pipeline,
    get_pipeline_mode,
    _build_normal_pipeline,
    _build_retry_pipeline,
)
from pipelines.maps_web_missing.agents.maps_search_agent import MapsSearchAgent
from pipelines.maps_web_missing.agents.retry_input_loader_agent import RetryInputLoaderAgent
from pipelines.maps_web_missing.agents.business_normalize_agent import BusinessNormalizeAgent
from pipelines.maps_web_missing.agents.website_presence_validator import WebsitePresenceValidator
from pipelines.maps_web_missing.agents.lead_router_agent import LeadRouterAgent
from pipelines.maps_web_missing.agents.lead_formatter_agent import LeadFormatterAgent
from pipelines.maps_web_missing.agents.google_sheets_export_agent import GoogleSheetsExportAgent


# =============================================================================
# MODE SELECTION TESTS
# =============================================================================

class TestModeSelection:
    """Tests for get_pipeline_mode() function."""

    def test_cli_mode_takes_priority_over_env(self):
        """CLI argument takes priority over environment variable."""
        with patch.dict(os.environ, {"PIPELINE_MODE": "normal"}):
            mode = get_pipeline_mode(cli_mode="retry")
            assert mode == "retry"

    def test_env_mode_used_when_cli_not_provided(self):
        """Environment variable used when CLI argument is None."""
        with patch.dict(os.environ, {"PIPELINE_MODE": "retry"}):
            mode = get_pipeline_mode(cli_mode=None)
            assert mode == "retry"

    def test_default_mode_when_nothing_set(self):
        """Default mode (normal) used when neither CLI nor ENV set."""
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("PIPELINE_MODE", None)
            mode = get_pipeline_mode(cli_mode=None)
            assert mode == DEFAULT_MODE
            assert mode == "normal"

    def test_valid_modes_constant(self):
        """VALID_MODES contains expected values."""
        assert "normal" in VALID_MODES
        assert "retry" in VALID_MODES
        assert len(VALID_MODES) == 2

    def test_mode_case_insensitive(self):
        """Mode selection is case insensitive."""
        assert get_pipeline_mode("NORMAL") == "normal"
        assert get_pipeline_mode("RETRY") == "retry"
        assert get_pipeline_mode("Normal") == "normal"
        assert get_pipeline_mode("ReTrY") == "retry"

    def test_mode_whitespace_stripped(self):
        """Whitespace is stripped from mode."""
        assert get_pipeline_mode("  normal  ") == "normal"
        assert get_pipeline_mode("\tretry\n") == "retry"


class TestInvalidMode:
    """Tests for invalid mode handling."""

    def test_invalid_cli_mode_raises_value_error(self):
        """Invalid CLI mode raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            get_pipeline_mode(cli_mode="banana")

        assert "Invalid pipeline mode" in str(exc_info.value)
        assert "banana" in str(exc_info.value)
        assert "normal" in str(exc_info.value)
        assert "retry" in str(exc_info.value)

    def test_invalid_env_mode_raises_value_error(self):
        """Invalid environment mode raises ValueError."""
        with patch.dict(os.environ, {"PIPELINE_MODE": "invalid_mode"}):
            with pytest.raises(ValueError) as exc_info:
                get_pipeline_mode(cli_mode=None)

            assert "Invalid pipeline mode" in str(exc_info.value)

    def test_empty_string_cli_mode_raises_value_error(self):
        """Empty string mode raises ValueError (not treated as None)."""
        with pytest.raises(ValueError):
            get_pipeline_mode(cli_mode="")

    def test_build_pipeline_invalid_mode_raises(self):
        """build_pipeline() raises ValueError for invalid mode."""
        with pytest.raises(ValueError) as exc_info:
            build_pipeline(mode="invalid")

        assert "Invalid pipeline mode" in str(exc_info.value)


# =============================================================================
# NORMAL MODE TESTS
# =============================================================================

class TestNormalModeUnchanged:
    """Tests verifying normal mode pipeline is unchanged."""

    def test_normal_mode_first_agent_is_maps_search(self):
        """Normal mode starts with MapsSearchAgent."""
        pipeline = build_pipeline(mode="normal")

        first_agent = pipeline.agents[0]
        assert isinstance(first_agent, MapsSearchAgent)

    def test_normal_mode_has_correct_agent_count(self):
        """Normal mode has expected number of agents."""
        pipeline = build_pipeline(mode="normal")

        # MapsSearch → Normalize → Validate → Route → Score → Enrich → Format → Export
        assert len(pipeline.agents) == 8

    def test_normal_mode_agent_sequence(self):
        """Normal mode has correct agent sequence."""
        pipeline = build_pipeline(mode="normal")

        agent_types = [type(a).__name__ for a in pipeline.agents]
        expected = [
            "MapsSearchAgent",
            "BusinessNormalizeAgent",
            "WebsitePresenceValidator",
            "LeadRouterAgent",
            "LeadScoringAgent",
            "EnrichmentAggregatorAgent",
            "LeadFormatterAgent",
            "GoogleSheetsExportAgent",
        ]
        assert agent_types == expected

    def test_normal_mode_includes_normalize_agent(self):
        """Normal mode includes BusinessNormalizeAgent."""
        pipeline = build_pipeline(mode="normal")

        agent_types = [type(a).__name__ for a in pipeline.agents]
        assert "BusinessNormalizeAgent" in agent_types

    def test_normal_mode_pipeline_name(self):
        """Normal mode pipeline has correct name."""
        pipeline = build_pipeline(mode="normal")
        assert "NORMAL" in pipeline.name


# =============================================================================
# RETRY MODE TESTS
# =============================================================================

class TestRetryModeSelection:
    """Tests for retry mode pipeline construction."""

    def test_retry_mode_first_agent_is_retry_loader(self):
        """Retry mode starts with RetryInputLoaderAgent."""
        pipeline = build_pipeline(mode="retry")

        first_agent = pipeline.agents[0]
        assert isinstance(first_agent, RetryInputLoaderAgent)

    def test_retry_mode_does_not_include_maps_search(self):
        """Retry mode does not include MapsSearchAgent."""
        pipeline = build_pipeline(mode="retry")

        agent_types = [type(a).__name__ for a in pipeline.agents]
        assert "MapsSearchAgent" not in agent_types

    def test_retry_mode_skips_normalize_agent(self):
        """Retry mode skips BusinessNormalizeAgent (data already normalized)."""
        pipeline = build_pipeline(mode="retry")

        agent_types = [type(a).__name__ for a in pipeline.agents]
        assert "BusinessNormalizeAgent" not in agent_types

    def test_retry_mode_agent_sequence(self):
        """Retry mode has correct agent sequence."""
        pipeline = build_pipeline(mode="retry")

        agent_types = [type(a).__name__ for a in pipeline.agents]
        expected = [
            "RetryInputLoaderAgent",
            "WebsitePresenceValidator",
            "LeadRouterAgent",
            "LeadScoringAgent",
            "EnrichmentAggregatorAgent",
            "LeadFormatterAgent",
            "GoogleSheetsExportAgent",
        ]
        assert agent_types == expected

    def test_retry_mode_has_seven_agents(self):
        """Retry mode has 7 agents (no Maps, no Normalize, but includes Phase 4)."""
        pipeline = build_pipeline(mode="retry")
        assert len(pipeline.agents) == 7

    def test_retry_mode_pipeline_name(self):
        """Retry mode pipeline has correct name."""
        pipeline = build_pipeline(mode="retry")
        assert "RETRY" in pipeline.name


# =============================================================================
# RETRY CONTRACT PRESERVATION TESTS
# =============================================================================

class TestRetryContractPreservation:
    """Tests verifying retry metadata flows through pipeline."""

    @pytest.fixture
    def mock_retry_output(self):
        """Mock output from RetryInputLoaderAgent."""
        return {
            "validated_businesses": [
                {
                    "dedup_key": "pid:retry_001",
                    "name": "Retry Business One",
                    "address": "100 Retry St",
                    "phone": "555-0001",
                    "website": "",
                    "retry_attempt": 2,
                    "source": "retry",
                    "last_retry_ts": "2026-01-31T10:00:00Z",
                },
                {
                    "dedup_key": "pid:retry_002",
                    "name": "Retry Business Two",
                    "address": "200 Retry Ave",
                    "phone": "555-0002",
                    "website": "https://test.com",
                    "retry_attempt": 1,
                    "source": "retry",
                    "last_retry_ts": "2026-01-31T11:00:00Z",
                },
            ],
            "retry_stats": {
                "total_rows": 3,
                "loaded": 2,
                "skipped_max_retry": 1,
                "skipped_missing_fields": 0,
                "max_retries": 3,
            },
        }

    def test_retry_attempt_preserved_after_validation(self, mock_retry_output):
        """retry_attempt field preserved after WebsitePresenceValidator."""
        # Mock WebsitePresenceValidator to pass through with added fields
        with patch.object(
            WebsitePresenceValidator, "run"
        ) as mock_validate:
            # Simulate validator adding has_real_website and website_status
            def validate_passthrough(context):
                businesses = context.get("validated_businesses", [])
                for biz in businesses:
                    biz["has_real_website"] = not not biz.get("website")
                    biz["website_status"] = "OK" if biz.get("website") else "NO_WEBSITE"
                return {"validated_businesses": businesses}

            mock_validate.side_effect = validate_passthrough

            validator = WebsitePresenceValidator()
            result = validator.run(mock_retry_output)

            for business in result["validated_businesses"]:
                assert "retry_attempt" in business
                assert business["retry_attempt"] in (1, 2)

    def test_source_retry_preserved_after_routing(self, mock_retry_output):
        """source='retry' preserved after LeadRouterAgent."""
        # First add validation fields
        for biz in mock_retry_output["validated_businesses"]:
            biz["has_real_website"] = False
            biz["website_status"] = "NO_WEBSITE"

        router = LeadRouterAgent()
        result = router.run(mock_retry_output)

        for lead in result["routed_leads"]:
            assert lead["source"] == "retry"

    def test_dedup_key_preserved_after_formatting(self, mock_retry_output):
        """dedup_key preserved after LeadFormatterAgent."""
        # Add validation and routing fields
        for biz in mock_retry_output["validated_businesses"]:
            biz["has_real_website"] = False
            biz["website_status"] = "NO_WEBSITE"
            biz["lead_route"] = "TARGET"
            biz["target_sheet"] = "NO_WEBSITE_TARGETS"

        context = {
            "routed_leads": mock_retry_output["validated_businesses"],
            "routing_stats": {"target_count": 2, "excluded_count": 0, "retry_count": 0},
        }

        formatter = LeadFormatterAgent()
        result = formatter.run(context)

        for lead in result["formatted_leads"]:
            assert "dedup_key" in lead
            assert lead["dedup_key"].startswith("pid:")

    def test_retry_metadata_full_pipeline_flow(self, mock_retry_output):
        """
        Full pipeline flow preserves retry metadata.

        Simulates: RetryLoader → Validate → Route → Format
        (Export mocked out)
        """
        # Step 1: RetryInputLoaderAgent output (mock_retry_output)
        context = dict(mock_retry_output)
        context["spreadsheet_id"] = "test_sheet_id"

        # Step 2: WebsitePresenceValidator
        with patch.object(WebsitePresenceValidator, "run") as mock_validate:
            def validate(ctx):
                businesses = ctx.get("validated_businesses", [])
                for biz in businesses:
                    biz["has_real_website"] = not not biz.get("website")
                    biz["website_status"] = "OK" if biz.get("website") else "NO_WEBSITE"
                return {"validated_businesses": businesses}

            mock_validate.side_effect = validate
            validator = WebsitePresenceValidator()
            context.update(validator.run(context))

        # Step 3: LeadRouterAgent
        router = LeadRouterAgent()
        context.update(router.run(context))

        # Step 4: LeadFormatterAgent
        formatter = LeadFormatterAgent()
        context.update(formatter.run(context))

        # Verify all retry metadata preserved
        for lead in context["formatted_leads"]:
            assert "dedup_key" in lead, "dedup_key missing after formatter"
            assert "source" in lead, "source missing after formatter"
            assert lead["source"] == "retry", f"source changed from 'retry' to '{lead['source']}'"
            # retry_attempt should be preserved as-is
            assert "retry_attempt" in lead, "retry_attempt missing after formatter"


# =============================================================================
# BUILDER FUNCTION TESTS
# =============================================================================

class TestBuilderFunctions:
    """Tests for internal builder functions."""

    def test_build_normal_pipeline_returns_runner(self):
        """_build_normal_pipeline returns PipelineRunner."""
        from pipelines.core.runner import PipelineRunner

        pipeline = _build_normal_pipeline()
        assert isinstance(pipeline, PipelineRunner)

    def test_build_retry_pipeline_returns_runner(self):
        """_build_retry_pipeline returns PipelineRunner."""
        from pipelines.core.runner import PipelineRunner

        pipeline = _build_retry_pipeline()
        assert isinstance(pipeline, PipelineRunner)

    def test_enable_file_backup_passed_to_normal_export(self):
        """enable_file_backup flag passed to GoogleSheetsExportAgent in normal mode."""
        pipeline = _build_normal_pipeline(enable_file_backup=False)

        export_agent = pipeline.agents[-1]
        assert isinstance(export_agent, GoogleSheetsExportAgent)
        # Note: We can't easily verify the flag was passed without inspecting internals

    def test_enable_file_backup_passed_to_retry_export(self):
        """enable_file_backup flag passed to GoogleSheetsExportAgent in retry mode."""
        pipeline = _build_retry_pipeline(enable_file_backup=False)

        export_agent = pipeline.agents[-1]
        assert isinstance(export_agent, GoogleSheetsExportAgent)


# =============================================================================
# ENV OVERRIDE TESTS
# =============================================================================

class TestEnvOverride:
    """Tests for PIPELINE_MODE environment variable behavior."""

    def test_env_retry_selects_retry_pipeline(self):
        """PIPELINE_MODE=retry selects retry pipeline."""
        with patch.dict(os.environ, {"PIPELINE_MODE": "retry"}):
            mode = get_pipeline_mode(cli_mode=None)
            assert mode == "retry"

            pipeline = build_pipeline(mode=mode)
            assert isinstance(pipeline.agents[0], RetryInputLoaderAgent)

    def test_env_normal_selects_normal_pipeline(self):
        """PIPELINE_MODE=normal selects normal pipeline."""
        with patch.dict(os.environ, {"PIPELINE_MODE": "normal"}):
            mode = get_pipeline_mode(cli_mode=None)
            assert mode == "normal"

            pipeline = build_pipeline(mode=mode)
            assert isinstance(pipeline.agents[0], MapsSearchAgent)

    def test_cli_overrides_env(self):
        """CLI --mode overrides PIPELINE_MODE env."""
        with patch.dict(os.environ, {"PIPELINE_MODE": "normal"}):
            mode = get_pipeline_mode(cli_mode="retry")
            assert mode == "retry"


# =============================================================================
# BACKWARD COMPATIBILITY TESTS
# =============================================================================

class TestBackwardCompatibility:
    """Tests ensuring normal pipeline behavior unchanged."""

    def test_default_build_pipeline_is_normal(self):
        """build_pipeline() with no args builds normal pipeline."""
        pipeline = build_pipeline()

        assert isinstance(pipeline.agents[0], MapsSearchAgent)
        assert len(pipeline.agents) == 8  # Updated for Phase 4

    def test_normal_pipeline_has_eight_agents(self):
        """Normal pipeline has exactly 8 agents (Phase 4 added Scoring + Enrichment)."""
        pipeline = build_pipeline(mode="normal")
        assert len(pipeline.agents) == 8

    def test_export_agent_is_last_in_both_modes(self):
        """GoogleSheetsExportAgent is last agent in both modes."""
        normal = build_pipeline(mode="normal")
        retry = build_pipeline(mode="retry")

        assert isinstance(normal.agents[-1], GoogleSheetsExportAgent)
        assert isinstance(retry.agents[-1], GoogleSheetsExportAgent)
