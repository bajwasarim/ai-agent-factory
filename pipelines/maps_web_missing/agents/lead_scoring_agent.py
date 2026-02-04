"""
Lead Scoring Agent for Maps No-Website Pipeline.

Computes quality scores for routed leads based on field completeness,
data confidence, and contactability. Pure function implementation
with no I/O side effects.

CRITICAL INVARIANTS:
- Scoring is deterministic (same input → same output)
- All weights are module-level constants
- No implicit truthiness (explicit presence checks)
- Routing fields (lead_route, target_sheet) pass through unchanged
- dedup_key is NEVER modified

Integration Position:
    LeadRouterAgent
           ↓
    LeadScoringAgent           ← THIS AGENT
           ↓
    EnrichmentAggregatorAgent
           ↓
    LeadFormatterAgent

Input: routed_leads
Output: scored_leads
"""

from typing import Any, Dict, List, Optional, TypedDict

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# SCORING WEIGHT CONSTANTS (DETERMINISTIC)
# =============================================================================

# Completeness score weights (must sum to 1.0)
WEIGHT_HAS_PHONE = 0.25
WEIGHT_HAS_ADDRESS = 0.25
WEIGHT_HAS_NAME = 0.20
WEIGHT_HAS_CATEGORY = 0.15
WEIGHT_HAS_RATING = 0.10
WEIGHT_HAS_PLACE_ID = 0.05

# Confidence score weights (must sum to 1.0)
WEIGHT_WEBSITE_STATUS_CONFIDENCE = 0.40
WEIGHT_PLACE_ID_PRESENT = 0.30
WEIGHT_SOURCE_RELIABILITY = 0.30

# Contactability score weights (must sum to 1.0)
WEIGHT_PHONE_PRESENT = 0.50
WEIGHT_ADDRESS_PRESENT = 0.30
WEIGHT_WEBSITE_PRESENT = 0.20

# Location confidence weights (must sum to 1.0)
WEIGHT_ADDRESS_EXISTS = 0.40
WEIGHT_PLACE_ID_EXISTS = 0.35
WEIGHT_LOCATION_EXISTS = 0.25

# Website status confidence mapping
WEBSITE_STATUS_CONFIDENCE = {
    "valid": 1.0,
    "invalid": 0.9,
    "missing": 0.85,
    "error": 0.3,
}
DEFAULT_STATUS_CONFIDENCE = 0.5

# Source reliability mapping
SOURCE_RELIABILITY = {
    "google_maps": 0.95,
    "yelp": 0.85,
    "csv": 0.70,
}
DEFAULT_SOURCE_RELIABILITY = 0.5


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class QualityScores(TypedDict):
    """Quality score block added to leads."""
    completeness_score: float
    confidence_score: float
    contactability_score: float
    location_confidence: float


class ScoredLead(TypedDict, total=False):
    """Type definition for a scored lead record."""
    # All original routed lead fields preserved
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
    # New quality scores block
    quality: QualityScores


# =============================================================================
# PURE SCORING FUNCTIONS
# =============================================================================

def _has_value(value: Any) -> bool:
    """
    Check if a value is present and non-empty.

    Explicit presence check - no implicit truthiness.

    Args:
        value: Any field value.

    Returns:
        True if value is present and non-empty string/non-None.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return len(value.strip()) > 0
    return True


def compute_completeness_score(lead: Dict[str, Any]) -> float:
    """
    Compute completeness score based on field presence.

    Deterministic weighted sum of field presence flags.

    Args:
        lead: Lead record to score.

    Returns:
        Float between 0.0 and 1.0.
    """
    score = 0.0

    if _has_value(lead.get("phone")):
        score += WEIGHT_HAS_PHONE
    if _has_value(lead.get("address")):
        score += WEIGHT_HAS_ADDRESS
    if _has_value(lead.get("name")):
        score += WEIGHT_HAS_NAME
    if _has_value(lead.get("category")):
        score += WEIGHT_HAS_CATEGORY
    if _has_value(lead.get("rating")):
        score += WEIGHT_HAS_RATING
    if _has_value(lead.get("place_id")):
        score += WEIGHT_HAS_PLACE_ID

    return round(score, 4)


def compute_confidence_score(lead: Dict[str, Any]) -> float:
    """
    Compute confidence score based on data reliability signals.

    Uses website validation status, place_id presence, and source.

    Args:
        lead: Lead record to score.

    Returns:
        Float between 0.0 and 1.0.
    """
    score = 0.0

    # Website status confidence
    website_status = lead.get("website_status", "")
    status_confidence = WEBSITE_STATUS_CONFIDENCE.get(
        website_status, DEFAULT_STATUS_CONFIDENCE
    )
    score += status_confidence * WEIGHT_WEBSITE_STATUS_CONFIDENCE

    # Place ID presence (strong identifier)
    if _has_value(lead.get("place_id")):
        score += WEIGHT_PLACE_ID_PRESENT

    # Source reliability
    source = lead.get("source", "")
    source_reliability = SOURCE_RELIABILITY.get(source, DEFAULT_SOURCE_RELIABILITY)
    score += source_reliability * WEIGHT_SOURCE_RELIABILITY

    return round(score, 4)


def compute_contactability_score(lead: Dict[str, Any]) -> float:
    """
    Compute contactability score based on contact information availability.

    Measures how easily the business can be contacted.

    Args:
        lead: Lead record to score.

    Returns:
        Float between 0.0 and 1.0.
    """
    score = 0.0

    if _has_value(lead.get("phone")):
        score += WEIGHT_PHONE_PRESENT
    if _has_value(lead.get("address")):
        score += WEIGHT_ADDRESS_PRESENT
    if _has_value(lead.get("website")):
        score += WEIGHT_WEBSITE_PRESENT

    return round(score, 4)


def compute_location_confidence(lead: Dict[str, Any]) -> float:
    """
    Compute location confidence based on geographic data quality.

    Args:
        lead: Lead record to score.

    Returns:
        Float between 0.0 and 1.0.
    """
    score = 0.0

    if _has_value(lead.get("address")):
        score += WEIGHT_ADDRESS_EXISTS
    if _has_value(lead.get("place_id")):
        score += WEIGHT_PLACE_ID_EXISTS
    if _has_value(lead.get("location")):
        score += WEIGHT_LOCATION_EXISTS

    return round(score, 4)


def score_single_lead(lead: Dict[str, Any]) -> ScoredLead:
    """
    Score a single lead with quality metrics.

    Pure function - no side effects, no I/O.
    Preserves all original fields and appends quality block.

    Args:
        lead: Routed lead record.

    Returns:
        ScoredLead with appended quality block.
    """
    quality: QualityScores = {
        "completeness_score": compute_completeness_score(lead),
        "confidence_score": compute_confidence_score(lead),
        "contactability_score": compute_contactability_score(lead),
        "location_confidence": compute_location_confidence(lead),
    }

    scored_lead: ScoredLead = {
        **lead,
        "quality": quality,
    }

    return scored_lead


def score_leads(leads: List[Dict[str, Any]]) -> List[ScoredLead]:
    """
    Score a batch of leads.

    Preserves input ordering.

    Args:
        leads: List of routed leads.

    Returns:
        List of scored leads in same order.
    """
    return [score_single_lead(lead) for lead in leads]


# =============================================================================
# LEAD SCORING AGENT
# =============================================================================

class LeadScoringAgent(BaseAgent):
    """
    Agent that computes quality scores for routed leads.

    Adds a 'quality' block to each lead containing:
    - completeness_score: Field presence (0.0-1.0)
    - confidence_score: Data reliability (0.0-1.0)
    - contactability_score: Contact info availability (0.0-1.0)
    - location_confidence: Geographic data quality (0.0-1.0)

    Contract:
        Input: routed_leads (from LeadRouterAgent)
        Output: scored_leads

    Invariants:
        - Deterministic (same input → same scores)
        - All routing fields preserved unchanged
        - dedup_key never modified
        - No I/O, no side effects
    """

    def __init__(self) -> None:
        """Initialize the Lead Scoring Agent."""
        super().__init__(name="LeadScoringAgent")
        logger.info("LeadScoringAgent initialized")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Score routed leads with quality metrics.

        Args:
            input_data: Dict with 'routed_leads' from LeadRouterAgent.

        Returns:
            Dict with 'scored_leads' containing quality-scored leads.

        Raises:
            ValueError: If routed_leads is missing (contract violation).
        """
        routed_leads = input_data.get("routed_leads")

        if routed_leads is None:
            raise ValueError(
                "Pipeline contract violation: 'routed_leads' key missing. "
                "LeadScoringAgent requires input from LeadRouterAgent."
            )

        if not isinstance(routed_leads, list):
            raise ValueError(
                f"Pipeline contract violation: 'routed_leads' must be a list, "
                f"got {type(routed_leads).__name__}"
            )

        logger.info(f"Scoring {len(routed_leads)} leads")

        # Score all leads
        scored_leads = score_leads(routed_leads)

        # Compute summary stats
        if scored_leads:
            avg_completeness = sum(
                lead["quality"]["completeness_score"] for lead in scored_leads
            ) / len(scored_leads)
            avg_confidence = sum(
                lead["quality"]["confidence_score"] for lead in scored_leads
            ) / len(scored_leads)
            logger.info(
                f"Scoring complete: avg_completeness={avg_completeness:.2f}, "
                f"avg_confidence={avg_confidence:.2f}"
            )
        else:
            logger.info("No leads to score")

        return {
            "scored_leads": scored_leads,
        }
