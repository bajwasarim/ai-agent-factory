"""Maps No-Website Pipeline construction.

Supports two execution modes:
    NORMAL: Fresh ingestion from Maps API
    RETRY:  Re-process failed website validations from Google Sheets
"""

import os
from typing import Literal

from core.infrastructure import MessageBus, StateStore
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
from pipelines.maps_web_missing.agents.outreach_orchestrator import OutreachOrchestrator
from pipelines.maps_web_missing.agents.email_outreach_agent import EmailOutreachAgent
from pipelines.maps_web_missing.agents.whatsapp_outreach_agent import WhatsAppOutreachAgent
from pipelines.maps_web_missing.config import PIPELINE_NAME
from core.logger import get_logger

logger = get_logger(__name__)


__all__ = [
    "build_pipeline",
    "build_outreach_agents",
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
        OutreachOrchestrator → outreach_results (Phase 6, queues leads for outreach)

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
            OutreachOrchestrator(),          # Phase 6 (queues leads for outreach)
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
        OutreachOrchestrator → outreach_results (Phase 6, queues leads for outreach)

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
            OutreachOrchestrator(),          # Phase 6 (queues leads for outreach)
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


def build_outreach_agents(
    state_store: StateStore | None = None,
    message_bus: MessageBus | None = None,
) -> tuple[OutreachOrchestrator, EmailOutreachAgent, WhatsAppOutreachAgent]:
    """
    Build and configure the Phase 6 outreach agents.

    The outreach system consists of:
        - OutreachOrchestrator: State machine that queues leads for outreach
        - EmailOutreachAgent: Subscribes to EMAIL_SEND events, sends emails
        - WhatsAppOutreachAgent: Subscribes to WHATSAPP_SEND events

    Note:
        The OutreachOrchestrator is also included in the main pipeline.
        Call this function to get the channel agents which operate via
        event subscription.

    Usage:
        orchestrator, email_agent, whatsapp_agent = build_outreach_agents()
        email_agent.start()
        whatsapp_agent.start()
        
        # Run pipeline (includes orchestrator)
        pipeline = build_pipeline()
        result = pipeline.run(context)
        
        # Channel agents process events during/after pipeline run
        
        # Cleanup
        email_agent.stop()
        whatsapp_agent.stop()

    Args:
        state_store: StateStore instance (creates new if None)
        message_bus: MessageBus instance (creates new if None)

    Returns:
        Tuple of (OutreachOrchestrator, EmailOutreachAgent, WhatsAppOutreachAgent)
    """
    logger.info("Building Phase 6 outreach agents")

    # Create shared infrastructure if not provided
    store = state_store or StateStore()
    bus = message_bus or MessageBus()

    # Create orchestrator with shared infrastructure
    orchestrator = OutreachOrchestrator(
        state_store=store,
        message_bus=bus,
    )

    # Create channel agents
    email_agent = EmailOutreachAgent(
        orchestrator=orchestrator,
        message_bus=bus,
    )

    whatsapp_agent = WhatsAppOutreachAgent(
        orchestrator=orchestrator,
        message_bus=bus,
    )

    logger.info("Outreach agents configured (not yet started)")
    return orchestrator, email_agent, whatsapp_agent
