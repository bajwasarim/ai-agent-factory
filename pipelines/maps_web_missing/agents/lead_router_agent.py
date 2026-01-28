"""
Lead Router Agent for Maps No-Website Pipeline.

Routes validated leads into target categories based on website presence status.
Pure function implementation with no I/O side effects.

Routing Rules:
    ┌────────────────────────────────────────────────────────────────────┐
    │ has_real_website │ website_status    │ lead_route │ target_sheet   │
    ├──────────────────┼───────────────────┼────────────┼────────────────┤
    │ False            │ missing, invalid  │ TARGET     │ NO_WEBSITE_... │
    │ True             │ valid             │ EXCLUDED   │ HAS_WEBSITE_...│
    │ *                │ error             │ RETRY      │ WEBSITE_CHECK..│
    │ * (unexpected)   │ * (unexpected)    │ RETRY      │ WEBSITE_CHECK..│
    └────────────────────────────────────────────────────────────────────┘

Integration Position:
    WebsitePresenceValidator
            ↓
    LeadRouterAgent            ← THIS AGENT
            ↓
    LeadFormatterAgent
            ↓
    ExporterAgent(s)
"""

from typing import Any, Dict, List, Optional, TypedDict
from enum import Enum

from pipelines.core.base_agent import BaseAgent
from core.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# ROUTING CONSTANTS
# =============================================================================

class LeadRoute(str, Enum):
    """Lead routing categories."""
    TARGET = "TARGET"
    EXCLUDED = "EXCLUDED"
    RETRY = "RETRY"


class TargetSheet(str, Enum):
    """Target sheet destinations for each route."""
    NO_WEBSITE_TARGETS = "NO_WEBSITE_TARGETS"
    HAS_WEBSITE_EXCLUDED = "HAS_WEBSITE_EXCLUDED"
    WEBSITE_CHECK_ERRORS = "WEBSITE_CHECK_ERRORS"


# Website statuses that qualify for TARGET routing
TARGET_STATUSES = frozenset(["missing", "invalid"])

# Website status indicating validation success
VALID_STATUS = "valid"

# Website status indicating error (needs retry)
ERROR_STATUS = "error"


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class RoutedLead(TypedDict, total=False):
    """Type definition for a routed lead record."""
    # Original fields (preserved)
    name: str
    website: str
    phone: str
    address: str
    place_id: str
    dedup_key: str
    has_real_website: bool
    website_status: str
    website_checked_at: str
    # Appended routing fields
    lead_route: str
    target_sheet: str


class RoutingResult(TypedDict):
    """Type definition for routing output."""
    targets: List[RoutedLead]
    excluded: List[RoutedLead]
    retry: List[RoutedLead]


# =============================================================================
# PURE ROUTING FUNCTIONS
# =============================================================================

def route_single_lead(lead: Dict[str, Any]) -> RoutedLead:
    """
    Route a single lead based on website validation status.

    Pure function - no side effects, no I/O.
    Preserves all original fields and appends routing fields.

    Args:
        lead: Business record with website validation fields.

    Returns:
        RoutedLead with appended lead_route and target_sheet fields.

    Routing Logic:
        1. has_real_website=False AND status in (missing, invalid) → TARGET
        2. has_real_website=True AND status=valid → EXCLUDED
        3. status=error OR any unexpected state → RETRY
    """
    # Defensive extraction with explicit None handling
    has_real_website: Optional[bool] = lead.get("has_real_website")
    website_status: Optional[str] = lead.get("website_status")

    # Determine route based on rules
    lead_route, target_sheet = _determine_route(has_real_website, website_status)

    # Create new dict preserving all original fields + appending routing fields
    routed_lead: RoutedLead = {
        **lead,
        "lead_route": lead_route.value,
        "target_sheet": target_sheet.value,
    }

    return routed_lead


def _determine_route(
    has_real_website: Optional[bool],
    website_status: Optional[str],
) -> tuple[LeadRoute, TargetSheet]:
    """
    Determine routing category based on validation fields.

    Pure function implementing routing decision logic.

    Args:
        has_real_website: Boolean flag from validator (may be None).
        website_status: Status string from validator (may be None).

    Returns:
        Tuple of (LeadRoute, TargetSheet).

    Decision Tree:
        ├─ website_status is None/empty → RETRY
        ├─ website_status == "error" → RETRY
        ├─ has_real_website is None → RETRY
        ├─ has_real_website == False AND status in (missing, invalid) → TARGET
        ├─ has_real_website == True AND status == "valid" → EXCLUDED
        └─ Any other combination → RETRY (unexpected state)
    """
    # Guard: null/empty website_status → RETRY
    if website_status is None or website_status == "":
        return LeadRoute.RETRY, TargetSheet.WEBSITE_CHECK_ERRORS

    # Normalize status for comparison
    status_lower = str(website_status).lower().strip()

    # Rule 3: Error status → RETRY
    if status_lower == ERROR_STATUS:
        return LeadRoute.RETRY, TargetSheet.WEBSITE_CHECK_ERRORS

    # Guard: null has_real_website → RETRY
    if has_real_website is None:
        return LeadRoute.RETRY, TargetSheet.WEBSITE_CHECK_ERRORS

    # Rule 1: No real website + target status → TARGET
    if has_real_website is False and status_lower in TARGET_STATUSES:
        return LeadRoute.TARGET, TargetSheet.NO_WEBSITE_TARGETS

    # Rule 2: Has real website + valid status → EXCLUDED
    if has_real_website is True and status_lower == VALID_STATUS:
        return LeadRoute.EXCLUDED, TargetSheet.HAS_WEBSITE_EXCLUDED

    # Default: Unexpected state → RETRY
    return LeadRoute.RETRY, TargetSheet.WEBSITE_CHECK_ERRORS


def route_leads(leads: List[Dict[str, Any]]) -> RoutingResult:
    """
    Route multiple leads into categorized arrays.

    Pure function - no side effects, no I/O.

    Args:
        leads: List of business records with website validation fields.

    Returns:
        RoutingResult with three arrays: targets, excluded, retry.

    Time Complexity: O(n) where n = len(leads)
    Space Complexity: O(n) for output arrays
    """
    targets: List[RoutedLead] = []
    excluded: List[RoutedLead] = []
    retry: List[RoutedLead] = []

    for lead in leads:
        routed = route_single_lead(lead)
        route_category = routed.get("lead_route")

        if route_category == LeadRoute.TARGET.value:
            targets.append(routed)
        elif route_category == LeadRoute.EXCLUDED.value:
            excluded.append(routed)
        else:
            # RETRY or any unexpected value
            retry.append(routed)

    return {
        "targets": targets,
        "excluded": excluded,
        "retry": retry,
    }


# =============================================================================
# AGENT CLASS
# =============================================================================

class LeadRouterAgent(BaseAgent):
    """
    Agent that routes validated leads into target categories.

    Routes based on website presence validation:
    - TARGET: Businesses without real websites (outreach candidates)
    - EXCLUDED: Businesses with valid websites (skip)
    - RETRY: Validation errors or unexpected states

    Features:
    - Pure function routing (no I/O)
    - Preserves all original fields
    - Appends only: lead_route, target_sheet
    - Defensive null handling

    Input: validated_businesses (from WebsitePresenceValidator)
    Output: routed_leads (dict with targets, excluded, retry arrays)
    """

    def __init__(self) -> None:
        """Initialize the lead router agent."""
        super().__init__(name="LeadRouterAgent")
        logger.info("LeadRouterAgent initialized")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route validated businesses into categorized arrays.

        Args:
            input_data: Dict with 'validated_businesses' list.

        Returns:
            Dict with 'routed_leads' containing:
                - targets: Leads without real websites (outreach candidates)
                - excluded: Leads with valid websites (skip)
                - retry: Leads with errors or unexpected states
            Also includes routing_summary with counts.
        """
        # Accept validated_businesses or fall back to normalized_businesses
        businesses = input_data.get(
            "validated_businesses",
            input_data.get("normalized_businesses", [])
        )

        if not businesses:
            logger.warning("No businesses to route")
            return {
                "routed_leads": {
                    "targets": [],
                    "excluded": [],
                    "retry": [],
                },
                "routing_summary": {
                    "total": 0,
                    "targets": 0,
                    "excluded": 0,
                    "retry": 0,
                },
            }

        # Route all leads (pure function call)
        routed = route_leads(businesses)

        # Flatten in stable order: TARGET → EXCLUDED → RETRY
        # This ensures deterministic exports and debug traceability
        flat_routed_leads = (
            routed["targets"] + routed["excluded"] + routed["retry"]
        )

        # Build routing stats for observability
        routing_stats = {
            "target": len(routed["targets"]),
            "excluded": len(routed["excluded"]),
            "retry": len(routed["retry"]),
            "total": len(flat_routed_leads),
        }

        # Log results
        logger.info(
            f"Routed {routing_stats['total']} leads: "
            f"{routing_stats['target']} TARGET, "
            f"{routing_stats['excluded']} EXCLUDED, "
            f"{routing_stats['retry']} RETRY"
        )

        return {
            "routed_leads": flat_routed_leads,
            "routing_stats": routing_stats,
        }
