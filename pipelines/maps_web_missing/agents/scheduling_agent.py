"""
Scheduling Agent for Maps No-Website Pipeline.

Infers scheduling capability requirements for enriched leads based on
industry and category signals. Pure function implementation with no
I/O side effects.

CRITICAL INVARIANTS:
- Inference is deterministic (same input → same output)
- All weights and rules are module-level constants
- No API calls, no env vars, no timestamps, no randomness
- No vendor references (Calendly, Cal.com, Acuity, etc.)
- All contract fields preserved unchanged
- dedup_key, lead_route, target_sheet never modified
- Scheduling block attached ONLY when scheduling is inferred

Integration Position:
    EnrichmentAggregatorAgent
           ↓
    SchedulingAgent            ← THIS AGENT
           ↓
    LeadFormatterAgent

Input: enriched_leads
Output: scheduled_leads
"""

from typing import Any, Dict, List, Optional, Set

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger
from core.contracts.scheduling import (
    SchedulingCapability,
    SchedulingMode,
    LocationMode,
    UrgencyLevel,
)

logger = get_logger(__name__)


# =============================================================================
# SCHEDULING INFERENCE CONSTANTS (DETERMINISTIC)
# =============================================================================

# Healthcare keywords → appointment scheduling
HEALTHCARE_KEYWORDS: Set[str] = {
    "healthcare", "dental", "dentist", "medical", "clinic",
    "doctor", "physician", "optometrist", "chiropractor",
    "therapy", "therapist", "orthodontist", "dermatologist",
    "pediatric", "veterinary", "vet", "urgent care", "hospital",
    "health center", "wellness", "physical therapy", "mental health",
}

# Home services keywords → estimate_request scheduling
HOME_SERVICES_KEYWORDS: Set[str] = {
    "plumbing", "plumber", "lawn", "landscaping", "roofing",
    "roofer", "cleaning", "cleaner", "maid", "hvac", "heating",
    "air conditioning", "electrical", "electrician", "handyman",
    "contractor", "remodeling", "renovation", "pest control",
    "tree service", "gutter", "pressure washing", "painting",
    "flooring", "carpet", "moving", "junk removal", "pool service",
}

# Auto services keywords → dropoff scheduling
AUTO_SERVICES_KEYWORDS: Set[str] = {
    "car wash", "auto detailing", "detailing", "auto repair",
    "mechanic", "tire", "oil change", "brake", "transmission",
    "body shop", "auto body", "collision", "muffler", "exhaust",
    "windshield", "auto glass", "smog", "emissions", "tune up",
}

# Confidence values (conservative, fixed)
CONFIDENCE_HEALTHCARE = 0.9
CONFIDENCE_HOME_SERVICES = 0.8
CONFIDENCE_AUTO_SERVICES = 0.85


# =============================================================================
# INFERENCE CATEGORY TYPE
# =============================================================================

class InferenceResult:
    """Result of scheduling inference for a single lead."""

    __slots__ = ("category", "confidence", "matched_keyword")

    def __init__(
        self,
        category: Optional[str],
        confidence: float,
        matched_keyword: Optional[str],
    ) -> None:
        self.category = category
        self.confidence = confidence
        self.matched_keyword = matched_keyword

    @property
    def should_attach(self) -> bool:
        """Returns True if scheduling block should be attached."""
        return self.category is not None


# =============================================================================
# PURE INFERENCE FUNCTIONS
# =============================================================================

def _normalize_text(text: Optional[str]) -> str:
    """
    Normalize text for keyword matching.

    Args:
        text: Input text to normalize.

    Returns:
        Lowercase string, empty string if None.
    """
    if text is None:
        return ""
    return str(text).lower().strip()


def _check_keywords(text: str, keywords: Set[str]) -> Optional[str]:
    """
    Check if text contains any keyword from the set.

    Args:
        text: Normalized text to search.
        keywords: Set of keywords to match.

    Returns:
        First matched keyword or None.
    """
    for keyword in keywords:
        if keyword in text:
            return keyword
    return None


def _infer_scheduling_category(lead: Dict[str, Any]) -> InferenceResult:
    """
    Infer scheduling category from lead data.

    Checks industry_category, primary_category, detected_industry, and name
    against known keyword sets.

    Order of precedence:
        1. Healthcare (highest priority)
        2. Home Services
        3. Auto Services

    Args:
        lead: Lead dictionary with enrichment data.

    Returns:
        InferenceResult with category, confidence, and matched keyword.
    """
    # Gather all relevant text fields
    industry_category = _normalize_text(lead.get("industry_category"))
    primary_category = _normalize_text(lead.get("primary_category"))
    name = _normalize_text(lead.get("name"))

    # Check enrichment block if present
    enrichment = lead.get("enrichment", {})
    detected_industry = _normalize_text(enrichment.get("detected_industry"))

    # Combine all searchable text
    searchable_text = f"{industry_category} {primary_category} {detected_industry} {name}"

    # Check healthcare first (highest priority)
    matched = _check_keywords(searchable_text, HEALTHCARE_KEYWORDS)
    if matched:
        return InferenceResult(
            category="healthcare",
            confidence=CONFIDENCE_HEALTHCARE,
            matched_keyword=matched,
        )

    # Check home services second
    matched = _check_keywords(searchable_text, HOME_SERVICES_KEYWORDS)
    if matched:
        return InferenceResult(
            category="home_services",
            confidence=CONFIDENCE_HOME_SERVICES,
            matched_keyword=matched,
        )

    # Check auto services third
    matched = _check_keywords(searchable_text, AUTO_SERVICES_KEYWORDS)
    if matched:
        return InferenceResult(
            category="auto_services",
            confidence=CONFIDENCE_AUTO_SERVICES,
            matched_keyword=matched,
        )

    # No match
    return InferenceResult(
        category=None,
        confidence=0.0,
        matched_keyword=None,
    )


def _build_scheduling_block(
    inference: InferenceResult,
) -> SchedulingCapability:
    """
    Build a SchedulingCapability block from inference result.

    Mapping rules:
        healthcare:
            - required: True
            - mode: appointment
            - urgency: standard
            - location_mode: on_site

        home_services:
            - required: True
            - mode: estimate_request
            - urgency: flexible
            - location_mode: off_site

        auto_services:
            - required: True
            - mode: dropoff
            - urgency: standard
            - location_mode: on_site

    Args:
        inference: InferenceResult with category and confidence.

    Returns:
        SchedulingCapability conforming to contract schema.
    """
    category = inference.category

    # Define mappings per category
    if category == "healthcare":
        mode: SchedulingMode = "appointment"
        urgency: UrgencyLevel = "standard"
        location_mode: LocationMode = "on_site"
    elif category == "home_services":
        mode = "estimate_request"
        urgency = "flexible"
        location_mode = "off_site"
    elif category == "auto_services":
        mode = "dropoff"
        urgency = "standard"
        location_mode = "on_site"
    else:
        # Should not reach here if called correctly
        raise ValueError(f"Unknown scheduling category: {category}")

    return SchedulingCapability(
        required=True,
        urgency=urgency,
        mode=mode,
        location_mode=location_mode,
        inferred_from=f"industry:{category}:{inference.matched_keyword}",
        inference_confidence=inference.confidence,
    )


def infer_scheduling_for_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Infer scheduling capability for a single lead.

    If scheduling is inferred, attaches a 'scheduling' block to the lead.
    If not inferred, returns the lead unchanged.

    Args:
        lead: Lead dictionary with enrichment data.

    Returns:
        Lead dictionary, optionally with 'scheduling' block added.
    """
    inference = _infer_scheduling_category(lead)

    if not inference.should_attach:
        # No scheduling inferred, return lead unchanged
        return lead

    # Build scheduling block
    scheduling_block = _build_scheduling_block(inference)

    # Create new dict to avoid mutating input
    result = dict(lead)
    result["scheduling"] = dict(scheduling_block)

    return result


def infer_scheduling_for_leads(leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Infer scheduling capability for a list of leads.

    Args:
        leads: List of enriched lead dictionaries.

    Returns:
        List of leads, each optionally augmented with 'scheduling' block.
    """
    return [infer_scheduling_for_lead(lead) for lead in leads]


# =============================================================================
# SCHEDULING AGENT
# =============================================================================

class SchedulingAgent(BaseAgent):
    """
    Agent that infers scheduling capability requirements for enriched leads.

    Adds an optional 'scheduling' block to leads where scheduling is inferred,
    based on industry and category signals.

    Contract:
        Input: enriched_leads (from EnrichmentAggregatorAgent)
        Output: scheduled_leads

    Scheduling Block (when attached):
        - required: bool
        - urgency: "immediate" | "standard" | "flexible"
        - mode: "appointment" | "dropoff" | "estimate_request"
        - location_mode: "on_site" | "off_site" | "remote"
        - inferred_from: str (e.g., "industry:healthcare:dentist")
        - inference_confidence: float (0.7-0.9)

    Invariants:
        - Keyword lookup ONLY (no ML, no embeddings)
        - No external API calls
        - Deterministic (same input → same output)
        - All contract fields preserved unchanged
        - Scheduling block attached ONLY when inferred
    """

    def __init__(self) -> None:
        """Initialize the Scheduling Agent."""
        super().__init__(name="SchedulingAgent")
        logger.info("SchedulingAgent initialized")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Infer scheduling capabilities for enriched leads.

        Args:
            input_data: Dict with 'enriched_leads' from EnrichmentAggregatorAgent.

        Returns:
            Dict with 'scheduled_leads' containing leads with optional
            scheduling blocks.

        Raises:
            ValueError: If enriched_leads is missing (contract violation).
        """
        enriched_leads = input_data.get("enriched_leads")

        if enriched_leads is None:
            raise ValueError(
                "Pipeline contract violation: 'enriched_leads' key missing. "
                "SchedulingAgent requires input from EnrichmentAggregatorAgent."
            )

        if not isinstance(enriched_leads, list):
            raise ValueError(
                f"Pipeline contract violation: 'enriched_leads' must be a list, "
                f"got {type(enriched_leads).__name__}"
            )

        logger.info(f"Inferring scheduling for {len(enriched_leads)} leads")

        # Infer scheduling for all leads
        scheduled_leads = infer_scheduling_for_leads(enriched_leads)

        # Compute summary stats
        leads_with_scheduling = sum(
            1 for lead in scheduled_leads if "scheduling" in lead
        )
        leads_without_scheduling = len(scheduled_leads) - leads_with_scheduling

        if scheduled_leads:
            category_counts: Dict[str, int] = {}
            for lead in scheduled_leads:
                if "scheduling" in lead:
                    inferred_from = lead["scheduling"].get("inferred_from", "")
                    # Extract category from "industry:category:keyword"
                    parts = inferred_from.split(":")
                    if len(parts) >= 2:
                        category = parts[1]
                        category_counts[category] = category_counts.get(category, 0) + 1

            logger.info(
                f"Scheduling inferred: {leads_with_scheduling}, "
                f"not inferred: {leads_without_scheduling}"
            )
            if category_counts:
                logger.info(f"Category distribution: {category_counts}")
        else:
            logger.info("No leads to process")

        return {
            "scheduled_leads": scheduled_leads,
        }
