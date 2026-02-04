"""
Enrichment Aggregator Agent for Maps No-Website Pipeline.

Aggregates enrichment signals into a single enrichment block for each lead.
Uses deterministic heuristics based on existing lead fields.

CRITICAL INVARIANTS:
- Keyword lookup only, NO probabilistic NLP or embeddings
- No external API calls
- Deterministic (same input → same output)
- All contract fields preserved unchanged
- dedup_key, lead_route, target_sheet never modified

Integration Position:
    LeadScoringAgent
           ↓
    EnrichmentAggregatorAgent  ← THIS AGENT
           ↓
    LeadFormatterAgent

Input: scored_leads
Output: enriched_leads
"""

from typing import Any, Dict, List, Optional, TypedDict
import re

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# ENRICHMENT CONSTANTS (DETERMINISTIC HEURISTICS)
# =============================================================================

# Business size estimation thresholds (based on review count)
SIZE_THRESHOLD_LARGE = 500
SIZE_THRESHOLD_MEDIUM = 100
SIZE_THRESHOLD_SMALL = 10

# Category keywords for industry classification
# NOTE: Keyword lookup ONLY - no embeddings, no ML, no probabilistic NLP
INDUSTRY_KEYWORDS = {
    "restaurant": [
        "restaurant", "cafe", "coffee", "diner", "bistro", "grill",
        "pizzeria", "sushi", "bar", "pub", "eatery", "bakery",
        "food", "kitchen", "catering", "dining",
    ],
    "healthcare": [
        "doctor", "dentist", "clinic", "medical", "health", "hospital",
        "pharmacy", "dental", "physician", "chiropractor", "therapy",
        "optometrist", "veterinary", "vet", "urgent care",
    ],
    "automotive": [
        "auto", "car", "mechanic", "tire", "repair", "body shop",
        "dealer", "vehicle", "motor", "transmission", "brake",
        "oil change", "detailing", "wash",
    ],
    "retail": [
        "store", "shop", "boutique", "outlet", "market", "mall",
        "grocery", "supermarket", "convenience", "retail",
    ],
    "services": [
        "salon", "spa", "barber", "beauty", "nail", "hair",
        "cleaning", "laundry", "plumber", "electrician", "hvac",
        "contractor", "landscaping", "lawn", "moving", "storage",
    ],
    "professional": [
        "attorney", "lawyer", "accountant", "cpa", "insurance",
        "real estate", "realtor", "consultant", "agency", "firm",
        "financial", "tax", "legal",
    ],
    "fitness": [
        "gym", "fitness", "yoga", "crossfit", "pilates", "martial",
        "boxing", "training", "workout", "studio",
    ],
    "education": [
        "school", "academy", "tutoring", "learning", "education",
        "preschool", "daycare", "college", "university", "training",
    ],
}

# Digital maturity indicators (in website/description)
DIGITAL_MATURITY_POSITIVE = [
    "online", "booking", "appointment", "schedule", "order",
    "delivery", "app", "digital", "virtual", "ecommerce",
]

DIGITAL_MATURITY_NEGATIVE = [
    "cash only", "walk-in only", "no website", "call for",
]


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class EnrichmentBlock(TypedDict):
    """Enrichment data block added to leads."""
    business_size_estimate: str  # small | medium | large | unknown
    industry_confidence: float   # 0.0 - 1.0
    digital_maturity_score: float  # 0.0 - 1.0
    detected_industry: str       # Detected industry category or "unknown"


class EnrichedLead(TypedDict, total=False):
    """Type definition for an enriched lead record."""
    # All scored lead fields preserved
    name: str
    website: str
    phone: str
    address: str
    place_id: str
    dedup_key: str
    has_real_website: bool
    website_status: str
    website_checked_at: str
    lead_route: str
    target_sheet: str
    quality: Dict[str, float]
    # New enrichment block
    enrichment: EnrichmentBlock


# =============================================================================
# PURE ENRICHMENT FUNCTIONS
# =============================================================================

def _parse_review_count(reviews_value: Any) -> Optional[int]:
    """
    Parse review count from various formats.

    Handles:
    - Integer: 150
    - String: "150", "150 reviews", "1.2K"

    Args:
        reviews_value: Review count in various formats.

    Returns:
        Integer count or None if unparseable.
    """
    if reviews_value is None:
        return None

    if isinstance(reviews_value, int):
        return reviews_value

    if isinstance(reviews_value, float):
        return int(reviews_value)

    if isinstance(reviews_value, str):
        # Handle "1.2K" format
        text = reviews_value.strip().upper()
        if "K" in text:
            try:
                num = float(text.replace("K", "").replace(",", ""))
                return int(num * 1000)
            except ValueError:
                pass

        # Extract first number from string
        match = re.search(r"(\d+(?:,\d+)?)", reviews_value)
        if match:
            try:
                return int(match.group(1).replace(",", ""))
            except ValueError:
                pass

    return None


def estimate_business_size(lead: Dict[str, Any]) -> str:
    """
    Estimate business size based on review count heuristic.

    Deterministic mapping:
    - >= 500 reviews → large
    - >= 100 reviews → medium
    - >= 10 reviews → small
    - < 10 or unknown → unknown

    Args:
        lead: Lead record with optional 'reviews' field.

    Returns:
        One of: "small", "medium", "large", "unknown"
    """
    review_count = _parse_review_count(lead.get("reviews"))

    if review_count is None:
        return "unknown"

    if review_count >= SIZE_THRESHOLD_LARGE:
        return "large"
    elif review_count >= SIZE_THRESHOLD_MEDIUM:
        return "medium"
    elif review_count >= SIZE_THRESHOLD_SMALL:
        return "small"
    else:
        return "unknown"


def detect_industry(lead: Dict[str, Any]) -> tuple[str, float]:
    """
    Detect industry category using keyword matching.

    IMPORTANT: This uses simple keyword lookup ONLY.
    No embeddings, no ML, no probabilistic NLP.

    Args:
        lead: Lead record with name, category, description fields.

    Returns:
        Tuple of (industry_name, confidence_score).
        Returns ("unknown", 0.0) if no match found.
    """
    # Combine searchable text fields
    searchable_parts = []
    for field in ["name", "category", "description"]:
        value = lead.get(field)
        if value and isinstance(value, str):
            searchable_parts.append(value.lower())

    searchable_text = " ".join(searchable_parts)

    if not searchable_text.strip():
        return ("unknown", 0.0)

    # Find best matching industry
    best_industry = "unknown"
    best_match_count = 0

    for industry, keywords in INDUSTRY_KEYWORDS.items():
        match_count = sum(1 for kw in keywords if kw in searchable_text)
        if match_count > best_match_count:
            best_match_count = match_count
            best_industry = industry

    # Calculate confidence based on match count
    # More keyword matches = higher confidence
    if best_match_count == 0:
        confidence = 0.0
    elif best_match_count == 1:
        confidence = 0.5
    elif best_match_count == 2:
        confidence = 0.7
    elif best_match_count >= 3:
        confidence = 0.9
    else:
        confidence = 0.0

    return (best_industry, round(confidence, 2))


def compute_digital_maturity(lead: Dict[str, Any]) -> float:
    """
    Compute digital maturity score based on indicators.

    Checks for presence of digital-positive and digital-negative keywords.

    Args:
        lead: Lead record.

    Returns:
        Float between 0.0 and 1.0.
    """
    # Combine searchable text
    searchable_parts = []
    for field in ["website", "description", "name"]:
        value = lead.get(field)
        if value and isinstance(value, str):
            searchable_parts.append(value.lower())

    searchable_text = " ".join(searchable_parts)

    # Base score
    score = 0.5

    # Has a website at all?
    has_website = bool(lead.get("website"))
    has_real = lead.get("has_real_website", False)

    if has_real:
        score += 0.3
    elif has_website:
        score += 0.1

    # Check for positive digital indicators
    positive_matches = sum(
        1 for kw in DIGITAL_MATURITY_POSITIVE if kw in searchable_text
    )
    score += min(positive_matches * 0.05, 0.2)

    # Check for negative digital indicators
    negative_matches = sum(
        1 for kw in DIGITAL_MATURITY_NEGATIVE if kw in searchable_text
    )
    score -= min(negative_matches * 0.1, 0.3)

    # Clamp to valid range
    return round(max(0.0, min(1.0, score)), 2)


def enrich_single_lead(lead: Dict[str, Any]) -> EnrichedLead:
    """
    Enrich a single lead with business intelligence.

    Pure function - no side effects, no I/O, no external calls.
    Preserves all original fields and appends enrichment block.

    Args:
        lead: Scored lead record.

    Returns:
        EnrichedLead with appended enrichment block.
    """
    detected_industry, industry_confidence = detect_industry(lead)

    enrichment: EnrichmentBlock = {
        "business_size_estimate": estimate_business_size(lead),
        "industry_confidence": industry_confidence,
        "digital_maturity_score": compute_digital_maturity(lead),
        "detected_industry": detected_industry,
    }

    enriched_lead: EnrichedLead = {
        **lead,
        "enrichment": enrichment,
    }

    return enriched_lead


def enrich_leads(leads: List[Dict[str, Any]]) -> List[EnrichedLead]:
    """
    Enrich a batch of leads.

    Preserves input ordering.

    Args:
        leads: List of scored leads.

    Returns:
        List of enriched leads in same order.
    """
    return [enrich_single_lead(lead) for lead in leads]


# =============================================================================
# ENRICHMENT AGGREGATOR AGENT
# =============================================================================

class EnrichmentAggregatorAgent(BaseAgent):
    """
    Agent that aggregates enrichment signals for scored leads.

    Adds an 'enrichment' block to each lead containing:
    - business_size_estimate: small | medium | large | unknown
    - industry_confidence: 0.0-1.0 confidence in industry detection
    - digital_maturity_score: 0.0-1.0 digital readiness estimate
    - detected_industry: Detected industry category

    Contract:
        Input: scored_leads (from LeadScoringAgent)
        Output: enriched_leads

    Invariants:
        - Keyword lookup ONLY (no ML, no embeddings)
        - No external API calls
        - Deterministic (same input → same enrichment)
        - All contract fields preserved unchanged
    """

    def __init__(self) -> None:
        """Initialize the Enrichment Aggregator Agent."""
        super().__init__(name="EnrichmentAggregatorAgent")
        logger.info("EnrichmentAggregatorAgent initialized")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich scored leads with business intelligence.

        Args:
            input_data: Dict with 'scored_leads' from LeadScoringAgent.

        Returns:
            Dict with 'enriched_leads' containing enriched leads.

        Raises:
            ValueError: If scored_leads is missing (contract violation).
        """
        scored_leads = input_data.get("scored_leads")

        if scored_leads is None:
            raise ValueError(
                "Pipeline contract violation: 'scored_leads' key missing. "
                "EnrichmentAggregatorAgent requires input from LeadScoringAgent."
            )

        if not isinstance(scored_leads, list):
            raise ValueError(
                f"Pipeline contract violation: 'scored_leads' must be a list, "
                f"got {type(scored_leads).__name__}"
            )

        logger.info(f"Enriching {len(scored_leads)} leads")

        # Enrich all leads
        enriched_leads = enrich_leads(scored_leads)

        # Compute summary stats
        if enriched_leads:
            size_counts = {}
            industry_counts = {}
            for lead in enriched_leads:
                size = lead["enrichment"]["business_size_estimate"]
                industry = lead["enrichment"]["detected_industry"]
                size_counts[size] = size_counts.get(size, 0) + 1
                industry_counts[industry] = industry_counts.get(industry, 0) + 1

            logger.info(f"Size distribution: {size_counts}")
            logger.info(f"Top industries: {dict(list(industry_counts.items())[:5])}")
        else:
            logger.info("No leads to enrich")

        return {
            "enriched_leads": enriched_leads,
        }
