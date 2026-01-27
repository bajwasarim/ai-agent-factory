"""Maps No-Website Pipeline construction."""

from pipelines.core.runner import PipelineRunner
from pipelines.maps_web_missing.agents.maps_search_agent import MapsSearchAgent
from pipelines.maps_web_missing.agents.business_normalize_agent import BusinessNormalizeAgent
from pipelines.maps_web_missing.agents.lead_formatter_agent import LeadFormatterAgent
from pipelines.maps_web_missing.agents.google_sheets_export_agent import GoogleSheetsExportAgent
from pipelines.maps_web_missing.config import PIPELINE_NAME

__all__ = ["build_pipeline", "PIPELINE_NAME"]


def build_pipeline(enable_file_backup: bool = True) -> PipelineRunner:
    """
    Build the Maps No-Website pipeline.

    Pipeline flow:
        Input (query, location, spreadsheet_id, ...)
        → MapsSearchAgent (raw_search_results)
        → BusinessNormalizeAgent (normalized_businesses)
        → LeadFormatterAgent (formatted_leads, summary)
        → GoogleSheetsExportAgent (export_status)
        → Output

    Args:
        enable_file_backup: Whether to also export to JSON/CSV files.

    Returns:
        Configured PipelineRunner instance.
    """
    return PipelineRunner(
        name=PIPELINE_NAME,
        agents=[
            MapsSearchAgent(),
            BusinessNormalizeAgent(),
            LeadFormatterAgent(),
            GoogleSheetsExportAgent(enable_file_backup=enable_file_backup),
        ],
    )
