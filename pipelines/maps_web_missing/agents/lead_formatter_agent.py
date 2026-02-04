"""Lead formatting agent for Maps No-Website Pipeline."""

from typing import Any, Dict, List

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


class LeadFormatterAgent(BaseAgent):
    """
    Agent that formats enriched leads into final lead structure.

    Prepares leads for export with consistent formatting and adds
    summary metadata. Preserves quality and enrichment blocks from
    Phase 4 agents.

    Input: enriched_leads (from EnrichmentAggregatorAgent) or routed_leads (fallback)
    Output: formatted_leads, summary

    Contract:
        - dedup_key, lead_route, target_sheet MUST be preserved unchanged
        - quality and enrichment blocks MUST be passed through
    """

    def __init__(self) -> None:
        """Initialize the lead formatter agent."""
        super().__init__(name="LeadFormatterAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format enriched leads into final lead structure.

        Transparent transformer: field rename/formatting only.
        No filtering, no routing logic, no dedup generation.

        Args:
            input_data: Dict with 'enriched_leads' from EnrichmentAggregatorAgent
                       or 'routed_leads' from LeadRouterAgent (backward compat).

        Returns:
            Dict with 'formatted_leads' and 'summary'.

        Raises:
            ValueError: If dedup_key is missing (pipeline contract violation).
        """
        # Read from enriched_leads (new Phase 4 contract) or routed_leads (fallback)
        businesses = input_data.get("enriched_leads") or input_data.get("routed_leads", [])
        query = input_data.get("query", "")
        location = input_data.get("location", "")

        formatted_leads: List[Dict[str, Any]] = []

        for idx, business in enumerate(businesses):
            # Contract invariant: dedup_key must exist from BusinessNormalizeAgent
            if "dedup_key" not in business:
                raise ValueError(
                    f"Pipeline contract violation: dedup_key missing in LeadFormatterAgent input. "
                    f"Lead id={business.get('place_id', 'unknown')}"
                )

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
                # Dedup key (pass-through from BusinessNormalizeAgent)
                "dedup_key": business.get("dedup_key"),
                # Website validation fields (from WebsitePresenceValidator)
                "has_website": bool(business.get("website", "")),
                "has_real_website": has_real_website,
                "website_status": business.get("website_status", ""),
                "website_checked_at": business.get("website_checked_at", ""),
                # Routing fields (from LeadRouterAgent)
                "lead_route": business.get("lead_route", ""),
                "target_sheet": business.get("target_sheet", ""),
                # Retry fields (from RetryInputLoaderAgent - preserve if present)
                "retry_attempt": business.get("retry_attempt"),
                "last_retry_ts": business.get("last_retry_ts"),
                # Phase 4 fields (from LeadScoringAgent + EnrichmentAggregatorAgent)
                "quality": business.get("quality"),
                "enrichment": business.get("enrichment"),
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
