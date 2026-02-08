"""
Scheduling Capability Contract.

Defines a provider-agnostic schema for describing scheduling capabilities
of a business. This is a CAPABILITY DESCRIPTION, not an implementation.

This contract is consumed by:
- SchedulingAgent (inference)
- LandingPageGeneratorAgent (UI hints)
- Outreach agents (messaging context)

CRITICAL INVARIANTS:
- No vendor references (Calendly, Cal.com, Acuity, etc.)
- No provider SDK imports
- No behavioral defaults
- Pure schema definition only
- All fields are descriptive, not prescriptive

Usage:
    The `SchedulingCapability` block is OPTIONAL when attached to a lead.
    Presence indicates the business MAY benefit from scheduling features.
    Absence indicates scheduling was not inferred or not applicable.
"""

from typing import List, Literal, Optional, TypedDict


# =============================================================================
# SCHEDULING MODE DEFINITIONS
# =============================================================================

SchedulingMode = Literal[
    "appointment",       # Fixed time slot booking (e.g., dentist, lawyer)
    "dropoff",           # Flexible arrival window (e.g., dry cleaner, repair shop)
    "estimate_request",  # Request for quote/callback (e.g., contractor, moving)
]

LocationMode = Literal[
    "on_site",   # Customer visits business location
    "off_site",  # Business visits customer location
    "remote",    # Virtual/phone appointment
]

UrgencyLevel = Literal[
    "immediate",   # Same-day or next-available (e.g., emergency services)
    "standard",    # Normal booking window (e.g., routine appointments)
    "flexible",    # No time pressure (e.g., consultations, estimates)
]


# =============================================================================
# BUSINESS HOURS SCHEMA
# =============================================================================

class DayHours(TypedDict, total=False):
    """Hours for a single day of the week."""
    open: str   # HH:MM format, 24-hour (e.g., "09:00")
    close: str  # HH:MM format, 24-hour (e.g., "17:00")
    closed: bool  # True if closed this day


class BusinessHours(TypedDict, total=False):
    """
    Structured business hours with timezone.
    
    All times are in the specified timezone.
    Days without entries are assumed unknown, not closed.
    """
    timezone: str  # IANA timezone (e.g., "America/New_York")
    monday: DayHours
    tuesday: DayHours
    wednesday: DayHours
    thursday: DayHours
    friday: DayHours
    saturday: DayHours
    sunday: DayHours


# =============================================================================
# SERVICE DEFINITION
# =============================================================================

class ServiceOffering(TypedDict, total=False):
    """
    A single service that can be scheduled.
    
    All fields are hints for UI generation, not booking logic.
    """
    name: str                      # Service name (e.g., "Dental Cleaning")
    duration_minutes: int          # Typical duration in minutes
    price_hint: str                # Display string (e.g., "$50-100", "Free consultation")
    location_mode: LocationMode    # Where service is performed
    requires_confirmation: bool    # Manual approval needed after booking


# =============================================================================
# SCHEDULING CONSTRAINTS
# =============================================================================

class SchedulingConstraints(TypedDict, total=False):
    """
    Constraints that affect how scheduling should be presented.
    
    These are descriptive flags, not enforcement rules.
    """
    requires_manual_confirmation: bool  # Business must approve bookings
    requires_contact_verification: bool  # Phone/email verification needed
    requires_deposit: bool               # Deposit or prepayment expected
    max_advance_days: int                # How far ahead bookings are accepted
    min_notice_hours: int                # Minimum lead time for bookings
    allows_cancellation: bool            # Whether cancellations are permitted
    allows_rescheduling: bool            # Whether rescheduling is permitted


# =============================================================================
# MAIN SCHEDULING CAPABILITY SCHEMA
# =============================================================================

class SchedulingCapability(TypedDict, total=False):
    """
    Provider-agnostic scheduling capability description.
    
    This schema describes WHAT scheduling features a business may need,
    not HOW they will be implemented. No vendor assumptions.
    
    Attachment:
        This block is OPTIONAL at the lead root level.
        Presence indicates scheduling capability was inferred.
        Absence indicates no scheduling inference was made.
    
    Example:
        {
            "required": True,
            "urgency": "standard",
            "mode": "appointment",
            "location_mode": "on_site",
            "services": [
                {"name": "Consultation", "duration_minutes": 30}
            ]
        }
    """
    # Core scheduling indicators
    required: bool                  # Whether scheduling is likely needed
    urgency: UrgencyLevel           # How time-sensitive bookings typically are
    mode: SchedulingMode            # Primary scheduling interaction type
    location_mode: LocationMode     # Primary service location type
    
    # Duration hints
    default_duration_minutes: int   # Typical appointment length if not per-service
    
    # Structured availability
    business_hours: BusinessHours   # When the business operates
    
    # Service catalog
    services: List[ServiceOffering]  # Available schedulable services
    
    # Booking constraints
    constraints: SchedulingConstraints  # Rules affecting booking flow
    
    # Inference metadata (for debugging, not business logic)
    inferred_from: str              # What triggered inference (e.g., "industry:healthcare")
    inference_confidence: float     # 0.0-1.0 confidence in scheduling need
