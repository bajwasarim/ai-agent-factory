"""Lead formatting agent for Maps No-Website Pipeline."""

from typing import Any, Dict, List

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


class LeadFormatterAgent(BaseAgent):
    """
    Agent that formats validated businesses into final lead structure.

    Prepares leads for export with consistent formatting and adds
    summary metadata. Uses website validation results from
    WebsitePresenceValidator.

    Input: validated_businesses (or normalized_businesses), query, location
    Output: formatted_leads, summary
    """

    def __init__(self) -> None:
        """Initialize the lead formatter agent."""
        super().__init__(name="LeadFormatterAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format validated businesses into final lead structure.

        Args:
            input_data: Dict with 'validated_businesses' from validator
                       (falls back to 'normalized_businesses' for compatibility).

        Returns:
            Dict with 'formatted_leads' and 'summary'.
        """
        # Accept validated_businesses (new) or normalized_businesses (fallback)
        businesses = input_data.get(
            "validated_businesses",
            input_data.get("normalized_businesses", [])
        )
        query = input_data.get("query", "")
        location = input_data.get("location", "")

        formatted_leads: List[Dict[str, Any]] = []

        for idx, business in enumerate(businesses):
            # Use validated has_real_website if available, else fallback to checking website field
            has_real_website = business.get(
                "has_real_website",
                bool(business.get("website", ""))
            )

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
                "place_id": business.get("place_id", ""),
                # Website validation fields (from WebsitePresenceValidator)
                "has_website": bool(business.get("website", "")),
                "has_real_website": has_real_website,
                "website_status": business.get("website_status", ""),
                "website_checked_at": business.get("website_checked_at", ""),
            })

        # Count based on validated website status
        real_website_count = sum(
            1 for l in formatted_leads if l.get("has_real_website")
        )
        no_real_website_count = len(formatted_leads) - real_website_count

        summary = {
            "query": query,
            "location": location,
            "total_leads": len(formatted_leads),
            "with_website": sum(1 for l in formatted_leads if l.get("has_website")),
            "without_website": sum(1 for l in formatted_leads if not l.get("has_website")),
            "with_real_website": real_website_count,
            "without_real_website": no_real_website_count,
        }

        logger.info(
            f"Formatted {len(formatted_leads)} leads "
            f"({summary['with_website']} with website, {summary['without_website']} without)"
        )
        logger.info(
            f"  Website validation: {real_website_count} real, "
            f"{no_real_website_count} invalid/missing"
        )
        return {"formatted_leads": formatted_leads, "summary": summary}
