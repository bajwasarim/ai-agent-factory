"""Business lead generation pipeline configuration."""

from typing import Any, Dict

from pipelines.runner import PipelineRunner
from projects.business_leadgen.agents import (
    BusinessNormalizeAgent,
    BusinessSearchAgent,
    GoogleSheetsExportAgent,
    WebsiteFilterAgent,
)


def build_pipeline() -> PipelineRunner:
    """
    Build the business lead generation pipeline with Google Sheets export.

    Pipeline flow:
        Input (query, location, spreadsheet_id)
        → BusinessSearchAgent (raw_search_results)
        → BusinessNormalizeAgent (normalized_businesses)
        → WebsiteFilterAgent (leads)
        → GoogleSheetsExportAgent (export_status)
        → Output

    Returns:
        Configured PipelineRunner instance.
    """
    return PipelineRunner(
        agents=[
            BusinessSearchAgent(),
            BusinessNormalizeAgent(),
            WebsiteFilterAgent(),
            GoogleSheetsExportAgent(),
        ]
    )


def build_enriched_pipeline() -> PipelineRunner:
    """
    Build the enriched business lead generation pipeline with LLM insights.

    Pipeline flow:
        Input (query, location)
        → BusinessSearchAgent (raw_search_results)
        → LeadExtractorAgent (extracted_leads)
        → LeadEnricherAgent (enriched_leads)
        → LeadFormatterAgent (formatted_leads, summary)
        → Output

    Returns:
        Configured PipelineRunner instance with LLM enrichment.
    """
    from projects.business_leadgen.agents import (
        LeadEnricherAgent,
        LeadExtractorAgent,
        LeadFormatterAgent,
    )

    return PipelineRunner(
        agents=[
            BusinessSearchAgent(),
            LeadExtractorAgent(),
            LeadEnricherAgent(),
            LeadFormatterAgent(),
        ]
    )


def run_leadgen(query: str, location: str = "", **kwargs: Any) -> Dict[str, Any]:
    """
    Convenience function to run the basic lead generation pipeline.

    Args:
        query: Business search query (e.g., "plumbers", "restaurants").
        location: Location to search in (e.g., "New York", "London").
        **kwargs: Additional context parameters (e.g., num_results).

    Returns:
        Pipeline output dict with 'leads' key containing filtered results.
    """
    # Ensure serper tool is registered
    import core.tools.serper_tool  # noqa: F401

    pipeline = build_pipeline()
    return pipeline.run({"query": query, "location": location, **kwargs})
