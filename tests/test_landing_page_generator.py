"""Tests for LandingPageGeneratorAgent.

Verifies Phase 5 landing page generation:
    1. Post-export only (reads exported_leads)
    2. TARGET-only filtering
    3. Idempotent via uuid5
    4. Template rendering
    5. File system operations
"""

import pytest
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

from pipelines.maps_web_missing.agents.landing_page_generator_agent import (
    LandingPageGeneratorAgent,
    LANDING_PAGE_NAMESPACE,
    DEFAULT_HTML_TEMPLATE,
    DEFAULT_MARKDOWN_TEMPLATE,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for landing page output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def target_lead():
    """A lead routed to TARGET with enrichment."""
    return {
        "name": "Joe's Plumbing",
        "phone": "+1-555-123-4567",
        "address": "123 Main St, Anytown, USA",
        "place_id": "ChIJ111",
        "dedup_key": "pid:ChIJ111",
        "lead_route": "TARGET",
        "target_sheet": "WebsiteLeads",
        "has_real_website": False,
        "website_status": "missing",
        "quality": {
            "overall_score": 0.85,
            "completeness_score": 0.9,
        },
        "enrichment": {
            "industry": {"primary": "Plumbing", "confidence": 0.8},
            "size": {"category": "small", "employee_range": "1-10"},
        },
    }


@pytest.fixture
def excluded_lead():
    """A lead routed to EXCLUDED."""
    return {
        "name": "Has Website Inc",
        "phone": "+1-555-222-3333",
        "address": "456 Oak Ave",
        "place_id": "ChIJ222",
        "dedup_key": "pid:ChIJ222",
        "lead_route": "EXCLUDED",
        "target_sheet": "ExcludedLeads",
        "has_real_website": True,
    }


@pytest.fixture
def retry_lead():
    """A lead routed to RETRY."""
    return {
        "name": "Error Business",
        "phone": "+1-555-333-4444",
        "address": "789 Pine Blvd",
        "place_id": "ChIJ333",
        "dedup_key": "pid:ChIJ333",
        "lead_route": "RETRY",
        "target_sheet": "RetryLeads",
        "website_status": "error",
    }


@pytest.fixture
def multiple_target_leads():
    """Multiple leads for batch testing."""
    return [
        {
            "name": f"Business {i}",
            "phone": f"+1-555-{i:03d}-1111",
            "address": f"{i} Test St",
            "place_id": f"ChIJ{i:03d}",
            "dedup_key": f"pid:ChIJ{i:03d}",
            "lead_route": "TARGET",
            "target_sheet": "WebsiteLeads",
        }
        for i in range(5)
    ]


# =============================================================================
# INITIALIZATION TESTS
# =============================================================================

class TestLandingPageGeneratorInit:
    """Tests for agent initialization."""

    def test_default_initialization(self, temp_output_dir):
        """Agent initializes with sensible defaults."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)

        assert agent.name == "LandingPageGeneratorAgent"
        assert agent.format == "html"
        assert agent.template == DEFAULT_HTML_TEMPLATE

    def test_markdown_format(self, temp_output_dir):
        """Agent uses markdown template when format is markdown."""
        agent = LandingPageGeneratorAgent(
            output_dir=temp_output_dir,
            format="markdown",
        )

        assert agent.format == "markdown"
        assert agent.template == DEFAULT_MARKDOWN_TEMPLATE

    def test_custom_template(self, temp_output_dir):
        """Agent accepts custom template."""
        custom = "<h1>{business_name}</h1>"
        agent = LandingPageGeneratorAgent(
            output_dir=temp_output_dir,
            template=custom,
        )

        assert agent.template == custom

    def test_format_case_insensitive(self, temp_output_dir):
        """Format parameter is case-insensitive."""
        agent = LandingPageGeneratorAgent(
            output_dir=temp_output_dir,
            format="MARKDOWN",
        )

        assert agent.format == "markdown"


# =============================================================================
# PAGE ID GENERATION TESTS (IDEMPOTENCY)
# =============================================================================

class TestPageIdGeneration:
    """Tests for uuid5-based page ID generation."""

    def test_page_id_is_deterministic(self, temp_output_dir):
        """Same dedup_key always produces same page_id."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)

        id1 = agent._generate_page_id("pid:ChIJ111")
        id2 = agent._generate_page_id("pid:ChIJ111")

        assert id1 == id2

    def test_different_keys_different_ids(self, temp_output_dir):
        """Different dedup_keys produce different page_ids."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)

        id1 = agent._generate_page_id("pid:ChIJ111")
        id2 = agent._generate_page_id("pid:ChIJ222")

        assert id1 != id2

    def test_page_id_is_valid_uuid(self, temp_output_dir):
        """Generated page_id is a valid UUID string."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)

        page_id = agent._generate_page_id("pid:ChIJ111")

        # Should not raise
        parsed = uuid.UUID(page_id)
        assert parsed.version == 5

    def test_page_id_uses_correct_namespace(self, temp_output_dir):
        """Page ID uses the defined LANDING_PAGE_NAMESPACE."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        dedup_key = "pid:ChIJ111"

        expected = str(uuid.uuid5(LANDING_PAGE_NAMESPACE, dedup_key))
        actual = agent._generate_page_id(dedup_key)

        assert actual == expected


# =============================================================================
# TARGET-ONLY FILTERING TESTS
# =============================================================================

class TestTargetOnlyFiltering:
    """Tests for filtering to TARGET leads only."""

    def test_target_lead_processed(self, temp_output_dir, target_lead):
        """TARGET lead gets a landing page generated."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        assert len(result["landing_pages"]["generated"]) == 1
        assert result["landing_pages"]["generated"][0]["dedup_key"] == "pid:ChIJ111"

    def test_excluded_lead_skipped(self, temp_output_dir, excluded_lead):
        """EXCLUDED lead is skipped."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [excluded_lead]}

        result = agent.run(context)

        assert len(result["landing_pages"]["generated"]) == 0
        assert len(result["landing_pages"]["skipped_non_target"]) == 1

    def test_retry_lead_skipped(self, temp_output_dir, retry_lead):
        """RETRY lead is skipped."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [retry_lead]}

        result = agent.run(context)

        assert len(result["landing_pages"]["generated"]) == 0
        assert len(result["landing_pages"]["skipped_non_target"]) == 1

    def test_mixed_leads_filtered(self, temp_output_dir, target_lead, excluded_lead, retry_lead):
        """Only TARGET leads get landing pages from mixed input."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead, excluded_lead, retry_lead]}

        result = agent.run(context)

        assert len(result["landing_pages"]["generated"]) == 1
        assert len(result["landing_pages"]["skipped_non_target"]) == 2


# =============================================================================
# IDEMPOTENCY TESTS
# =============================================================================

class TestIdempotency:
    """Tests for idempotent page generation."""

    def test_existing_page_not_overwritten(self, temp_output_dir, target_lead):
        """Existing page is not regenerated."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        # First run - generates
        result1 = agent.run(context)
        assert len(result1["landing_pages"]["generated"]) == 1

        # Second run - skips existing
        result2 = agent.run(context)
        assert len(result2["landing_pages"]["generated"]) == 0
        assert len(result2["landing_pages"]["skipped_existing"]) == 1

    def test_skipped_existing_includes_path(self, temp_output_dir, target_lead):
        """Skipped existing record includes file path."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        agent.run(context)
        result = agent.run(context)

        skipped = result["landing_pages"]["skipped_existing"][0]
        assert "path" in skipped
        assert skipped["path"].endswith(".html")

    def test_partial_regeneration(self, temp_output_dir, multiple_target_leads):
        """Only new leads get pages when some already exist."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)

        # Generate first 3
        context1 = {"exported_leads": multiple_target_leads[:3]}
        result1 = agent.run(context1)
        assert len(result1["landing_pages"]["generated"]) == 3

        # Run with all 5 - should only generate 2 new ones
        context2 = {"exported_leads": multiple_target_leads}
        result2 = agent.run(context2)
        assert len(result2["landing_pages"]["generated"]) == 2
        assert len(result2["landing_pages"]["skipped_existing"]) == 3


# =============================================================================
# FILE GENERATION TESTS
# =============================================================================

class TestFileGeneration:
    """Tests for actual file creation."""

    def test_html_file_created(self, temp_output_dir, target_lead):
        """HTML file is created in output directory."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        generated = result["landing_pages"]["generated"][0]
        path = Path(generated["path"])
        assert path.exists()
        assert path.suffix == ".html"

    def test_markdown_file_created(self, temp_output_dir, target_lead):
        """Markdown file is created when format is markdown."""
        agent = LandingPageGeneratorAgent(
            output_dir=temp_output_dir,
            format="markdown",
        )
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        generated = result["landing_pages"]["generated"][0]
        path = Path(generated["path"])
        assert path.exists()
        assert path.suffix == ".md"

    def test_output_dir_created(self, temp_output_dir, target_lead):
        """Output directory is created if it doesn't exist."""
        nested_dir = Path(temp_output_dir) / "nested" / "dir"
        agent = LandingPageGeneratorAgent(output_dir=str(nested_dir))
        context = {"exported_leads": [target_lead]}

        agent.run(context)

        assert nested_dir.exists()


# =============================================================================
# TEMPLATE RENDERING TESTS
# =============================================================================

class TestTemplateRendering:
    """Tests for template variable substitution."""

    def test_business_name_in_output(self, temp_output_dir, target_lead):
        """Business name appears in generated page."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        path = Path(result["landing_pages"]["generated"][0]["path"])
        content = path.read_text()
        assert "Joe's Plumbing" in content

    def test_address_in_output(self, temp_output_dir, target_lead):
        """Address appears in generated page."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        path = Path(result["landing_pages"]["generated"][0]["path"])
        content = path.read_text()
        assert "123 Main St, Anytown, USA" in content

    def test_phone_in_output(self, temp_output_dir, target_lead):
        """Phone number appears in generated page."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        path = Path(result["landing_pages"]["generated"][0]["path"])
        content = path.read_text()
        assert "+1-555-123-4567" in content

    def test_page_id_in_output(self, temp_output_dir, target_lead):
        """Page ID appears in generated page footer."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        page_id = result["landing_pages"]["generated"][0]["page_id"]
        path = Path(result["landing_pages"]["generated"][0]["path"])
        content = path.read_text()
        assert page_id in content

    def test_industry_from_enrichment(self, temp_output_dir, target_lead):
        """Industry from enrichment appears in output."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        path = Path(result["landing_pages"]["generated"][0]["path"])
        content = path.read_text()
        assert "Plumbing" in content

    def test_fallback_when_no_enrichment(self, temp_output_dir):
        """Default industry used when no enrichment present."""
        lead = {
            "name": "Simple Business",
            "phone": "555-0000",
            "address": "1 Test St",
            "dedup_key": "pid:simple",
            "lead_route": "TARGET",
        }
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [lead]}

        result = agent.run(context)

        path = Path(result["landing_pages"]["generated"][0]["path"])
        content = path.read_text()
        assert "Local Business" in content


# =============================================================================
# EDGE CASES AND ERROR HANDLING
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_exported_leads(self, temp_output_dir):
        """Empty input produces empty results without error."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": []}

        result = agent.run(context)

        assert result["landing_pages"]["generated"] == []
        assert result["landing_pages"]["skipped_existing"] == []

    def test_missing_exported_leads_key(self, temp_output_dir):
        """Missing exported_leads key handled gracefully."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {}

        result = agent.run(context)

        assert "landing_pages" in result
        assert result["landing_pages"]["generated"] == []

    def test_lead_with_missing_fields(self, temp_output_dir):
        """Lead with minimal fields still generates page."""
        lead = {
            "dedup_key": "pid:minimal",
            "lead_route": "TARGET",
        }
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [lead]}

        result = agent.run(context)

        # Should still generate with defaults
        assert len(result["landing_pages"]["generated"]) == 1

    def test_context_passthrough(self, temp_output_dir, target_lead):
        """Original context fields are preserved."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {
            "exported_leads": [target_lead],
            "other_field": "preserved",
        }

        result = agent.run(context)

        assert result["other_field"] == "preserved"

    def test_result_includes_business_name(self, temp_output_dir, target_lead):
        """Generated result includes business name for logging."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": [target_lead]}

        result = agent.run(context)

        generated = result["landing_pages"]["generated"][0]
        assert generated["business_name"] == "Joe's Plumbing"


# =============================================================================
# BATCH PROCESSING TESTS
# =============================================================================

class TestBatchProcessing:
    """Tests for processing multiple leads."""

    def test_multiple_pages_generated(self, temp_output_dir, multiple_target_leads):
        """Multiple pages generated for multiple leads."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": multiple_target_leads}

        result = agent.run(context)

        assert len(result["landing_pages"]["generated"]) == 5

    def test_all_pages_unique(self, temp_output_dir, multiple_target_leads):
        """Each lead gets a unique page file."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        context = {"exported_leads": multiple_target_leads}

        result = agent.run(context)

        paths = [g["path"] for g in result["landing_pages"]["generated"]]
        assert len(paths) == len(set(paths))  # All unique

    def test_error_doesnt_stop_batch(self, temp_output_dir, multiple_target_leads):
        """Error in one lead doesn't stop processing others."""
        agent = LandingPageGeneratorAgent(output_dir=temp_output_dir)
        
        # Add a bad lead in the middle
        leads = multiple_target_leads[:2] + [{"dedup_key": None, "lead_route": "TARGET"}] + multiple_target_leads[2:]
        context = {"exported_leads": leads}

        result = agent.run(context)

        # Should still process the good leads
        assert len(result["landing_pages"]["generated"]) >= 4
