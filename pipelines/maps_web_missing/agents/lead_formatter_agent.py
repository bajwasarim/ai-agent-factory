"""Lead formatting agent for Maps No-Website Pipeline."""

from typing import Any, Dict, List

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


class LeadFormatterAgent(BaseAgent):
    """
    Agent that formats normalized businesses into final lead structure.

    Prepares leads for export with consistent formatting and adds
    summary metadata.

    Input: normalized_businesses, query, location
    Output: formatted_leads, summary
    """

    def __init__(self) -> None:
        """Initialize the lead formatter agent."""
        super().__init__(name="LeadFormatterAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format normalized businesses into final lead structure.

        Args:
            input_data: Dict with 'normalized_businesses' from normalizer.

        Returns:
            Dict with 'formatted_leads' and 'summary'.
        """
        businesses = input_data.get("normalized_businesses", [])
        query = input_data.get("query", "")
        location = input_data.get("location", "")

        formatted_leads: List[Dict[str, Any]] = []

        for idx, business in enumerate(businesses):
            formatted_leads.append({
                "rank": idx + 1,
                "name": business.get("name", ""),
                "website": business.get("website", ""),
                "description": business.get("description", ""),
                "source": business.get("source", ""),
                "location": business.get("location", location),
                # Maps-specific fields
                "phone": business.get("phone", ""),
                "rating": business.get("rating", ""),
                "reviews": business.get("reviews", ""),
                "address": business.get("address", ""),
                # Metadata
                "has_website": bool(business.get("website", "")),
            })

        summary = {
            "query": query,
            "location": location,
            "total_leads": len(formatted_leads),
            "with_website": sum(1 for l in formatted_leads if l.get("has_website")),
            "without_website": sum(1 for l in formatted_leads if not l.get("has_website")),
        }

        logger.info(
            f"Formatted {len(formatted_leads)} leads "
            f"({summary['with_website']} with website, {summary['without_website']} without)"
        )
        return {"formatted_leads": formatted_leads, "summary": summary}
