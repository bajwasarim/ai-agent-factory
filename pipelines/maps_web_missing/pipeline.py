"""Maps No-Website Pipeline construction.

Supports two execution modes:
    NORMAL: Fresh ingestion from Maps API
    RETRY:  Re-process failed website validations from Google Sheets
"""

import os
from typing import Literal

from pipelines.core.runner import PipelineRunner
from pipelines.maps_web_missing.agents.maps_search_agent import MapsSearchAgent
from pipelines.maps_web_missing.agents.business_normalize_agent import BusinessNormalizeAgent
from pipelines.maps_web_missing.agents.website_presence_validator import WebsitePresenceValidator
from pipelines.maps_web_missing.agents.lead_router_agent import LeadRouterAgent
from pipelines.maps_web_missing.agents.lead_scoring_agent import LeadScoringAgent
from pipelines.maps_web_missing.agents.enrichment_aggregator_agent import EnrichmentAggregatorAgent
from pipelines.maps_web_missing.agents.lead_formatter_agent import LeadFormatterAgent
from pipelines.maps_web_missing.agents.google_sheets_export_agent import GoogleSheetsExportAgent
from pipelines.maps_web_missing.agents.retry_input_loader_agent import RetryInputLoaderAgent
from pipelines.maps_web_missing.agents.landing_page_generator_agent import LandingPageGeneratorAgent
from pipelines.maps_web_missing.config import PIPELINE_NAME
from core.logger import get_logger

logger = get_logger(__name__)


__all__ = [
    "build_pipeline",
    "PIPELINE_NAME",
    "VALID_MODES",
    "get_pipeline_mode",
]


# =============================================================================
# MODE CONFIGURATION
# =============================================================================

# Valid execution modes
VALID_MODES = frozenset(["normal", "retry"])

# Default mode if not specified
DEFAULT_MODE = "normal"

# Type alias for mode
PipelineMode = Literal["normal", "retry"]


def get_pipeline_mode(cli_mode: str | None = None) -> PipelineMode:
    """
    Determine pipeline execution mode from CLI or environment.

    Priority order:
        1. CLI argument (if provided)
        2. PIPELINE_MODE environment variable
        3. Default fallback: "normal"

    Args:
        cli_mode: Mode from command line argument (highest priority).

    Returns:
        Validated pipeline mode.

    Raises:
        ValueError: If mode is not in VALID_MODES.
    """
    # Priority 1: CLI argument
    if cli_mode is not None:
        mode = cli_mode.lower().strip()
    # Priority 2: Environment variable
    elif os.getenv("PIPELINE_MODE"):
        mode = os.getenv("PIPELINE_MODE", "").lower().strip()
    # Priority 3: Default
    else:
        mode = DEFAULT_MODE

    # Validate mode
    if mode not in VALID_MODES:
        raise ValueError(
            f"Invalid pipeline mode: '{mode}'. "
            f"Allowed modes: {sorted(VALID_MODES)}"
        )

    return mode  # type: ignore


# =============================================================================
# PIPELINE BUILDERS
# =============================================================================

def _build_normal_pipeline(enable_file_backup: bool = True) -> PipelineRunner:
    """
    Build the normal Maps ingestion pipeline.

    Pipeline flow:
        MapsSearchAgent → raw_search_results
        BusinessNormalizeAgent → normalized_businesses (with dedup_key)
        WebsitePresenceValidator → validated_businesses
        LeadRouterAgent → routed_leads, routing_stats
        LeadScoringAgent → scored_leads (Phase 4)
        EnrichmentAggregatorAgent → enriched_leads (Phase 4)
        LeadFormatterAgent → formatted_leads
        GoogleSheetsExportAgent → exported_leads, export_status
        LandingPageGeneratorAgent → landing_pages (Phase 5, post-export)

    Args:
        enable_file_backup: Whether to also export to JSON/CSV files.

    Returns:
        Configured PipelineRunner instance.
    """
    logger.info("Building NORMAL ingestion pipeline")

    return PipelineRunner(
        name=f"{PIPELINE_NAME}_NORMAL",
        agents=[
            MapsSearchAgent(),
            BusinessNormalizeAgent(),
            WebsitePresenceValidator(),
            LeadRouterAgent(),
            LeadScoringAgent(),              # Phase 4
            EnrichmentAggregatorAgent(),     # Phase 4
            LeadFormatterAgent(),
            GoogleSheetsExportAgent(enable_file_backup=enable_file_backup),
            LandingPageGeneratorAgent(),     # Phase 5 (post-export)
        ],
    )


def _build_retry_pipeline(enable_file_backup: bool = True) -> PipelineRunner:
    """
    Build the retry pipeline for re-processing failed validations.

    Pipeline flow:
        RetryInputLoaderAgent → validated_businesses, retry_stats
        (skips BusinessNormalizeAgent - data already normalized in sheet)
        WebsitePresenceValidator → validated_businesses (updated)
        LeadRouterAgent → routed_leads, routing_stats
        LeadScoringAgent → scored_leads (Phase 4)
        EnrichmentAggregatorAgent → enriched_leads (Phase 4)
        LeadFormatterAgent → formatted_leads
        GoogleSheetsExportAgent → exported_leads, export_status
        LandingPageGeneratorAgent → landing_pages (Phase 5, post-export)

    Note:
        RetryInputLoaderAgent outputs `validated_businesses` directly,
        matching the contract expected by WebsitePresenceValidator.
        BusinessNormalizeAgent is skipped since retry data from sheets
        is already normalized with dedup_key.

    Args:
        enable_file_backup: Whether to also export to JSON/CSV files.

    Returns:
        Configured PipelineRunner instance.
    """
    logger.info("Building RETRY pipeline for failed website validations")

    return PipelineRunner(
        name=f"{PIPELINE_NAME}_RETRY",
        agents=[
            RetryInputLoaderAgent(),
            # Note: BusinessNormalizeAgent skipped - retry data already normalized
            WebsitePresenceValidator(),
            LeadRouterAgent(),
            LeadScoringAgent(),              # Phase 4
            EnrichmentAggregatorAgent(),     # Phase 4
            LeadFormatterAgent(),
            GoogleSheetsExportAgent(enable_file_backup=enable_file_backup),
            LandingPageGeneratorAgent(),     # Phase 5 (post-export)
        ],
    )


def build_pipeline(
    mode: PipelineMode = "normal",
    enable_file_backup: bool = True,
) -> PipelineRunner:
    """
    Build the Maps No-Website pipeline in the specified mode.

    Modes:
        normal: Fresh ingestion from Maps API (default)
        retry:  Re-process failed website validations from Google Sheets

    Pipeline flow (NORMAL):
        Input (query, location, spreadsheet_id, ...)
        ┌─────────────────────────────────────────────┐
        │  MapsSearchAgent                            │
        │  → raw_search_results                       │
        ├─────────────────────────────────────────────┤
        │  BusinessNormalizeAgent                     │
        │  → normalized_businesses (with dedup_key)   │
        ├─────────────────────────────────────────────┤
        │  WebsitePresenceValidator                   │
        │  → validated_businesses                     │
        ├─────────────────────────────────────────────┤
        │  LeadRouterAgent                            │
        │  → routed_leads, routing_stats             │
        ├─────────────────────────────────────────────┤
        │  LeadFormatterAgent                         │
        │  → formatted_leads                          │
        ├─────────────────────────────────────────────┤
        │  GoogleSheetsExportAgent                    │
        │  → export_status                            │
        └─────────────────────────────────────────────┘

    Pipeline flow (RETRY):
        Input (spreadsheet_id, retry_sheet_name, ...)
        ┌─────────────────────────────────────────────┐
        │  RetryInputLoaderAgent                      │
        │  → validated_businesses, retry_stats        │
        ├─────────────────────────────────────────────┤
        │  WebsitePresenceValidator                   │
        │  → validated_businesses (re-validated)      │
        ├─────────────────────────────────────────────┤
        │  LeadRouterAgent                            │
        │  → routed_leads, routing_stats             │
        ├─────────────────────────────────────────────┤
        │  LeadFormatterAgent                         │
        │  → formatted_leads                          │
        ├─────────────────────────────────────────────┤
        │  GoogleSheetsExportAgent                    │
        │  → export_status                            │
        └─────────────────────────────────────────────┘

    Args:
        mode: Execution mode - "normal" or "retry".
        enable_file_backup: Whether to also export to JSON/CSV files.

    Returns:
        Configured PipelineRunner instance.

    Raises:
        ValueError: If mode is not valid.
    """
    # Validate mode
    if mode not in VALID_MODES:
        raise ValueError(
            f"Invalid pipeline mode: '{mode}'. "
            f"Allowed modes: {sorted(VALID_MODES)}"
        )

    logger.info(f"PIPELINE MODE: {mode.upper()}")

    if mode == "retry":
        return _build_retry_pipeline(enable_file_backup=enable_file_backup)
    else:
        return _build_normal_pipeline(enable_file_backup=enable_file_backup)
