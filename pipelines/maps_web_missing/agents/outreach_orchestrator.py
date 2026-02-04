"""Outreach Orchestrator - Phase 6 State Machine.

Manages the outreach state machine for leads. The orchestrator coordinates
multi-channel outreach (email, WhatsApp) with:
    - State-based progression (PENDING → SENT → RESPONDED/FAILED)
    - Configurable delays between attempts
    - Channel-specific escalation paths
    - Idempotent state transitions via StateStore CAS

Event-Driven Architecture:
    The orchestrator publishes events via MessageBus for async channel agents.
    This decouples the orchestrator from channel implementations.

State Transitions:
    PENDING → QUEUED_EMAIL → EMAIL_SENT → (RESPONDED | FAILED | QUEUED_WHATSAPP)
                                                           ↓
                                              WHATSAPP_SENT → (RESPONDED | FAILED)
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional
import uuid

from core.infrastructure import MessageBus, StateStore
from core.logger import get_logger
from pipelines.core.base_agent import BaseAgent


# =============================================================================
# OUTREACH STATES
# =============================================================================

class OutreachState(str, Enum):
    """States for lead outreach progression."""
    
    PENDING = "pending"                  # Initial state, not yet processed
    QUEUED_EMAIL = "queued_email"        # Queued for email send
    EMAIL_SENT = "email_sent"            # Email sent, awaiting response
    EMAIL_FAILED = "email_failed"        # Email send failed
    QUEUED_WHATSAPP = "queued_whatsapp"  # Escalated to WhatsApp queue
    WHATSAPP_SENT = "whatsapp_sent"      # WhatsApp sent, awaiting response
    WHATSAPP_FAILED = "whatsapp_failed"  # WhatsApp send failed
    RESPONDED = "responded"               # Lead responded (terminal)
    EXHAUSTED = "exhausted"              # All channels attempted, no response


# =============================================================================
# EVENTS
# =============================================================================

class OutreachEvent(str, Enum):
    """Events published by the orchestrator."""
    
    LEAD_READY = "outreach.lead.ready"          # Lead ready for outreach
    EMAIL_QUEUED = "outreach.email.queued"      # Email queued for send
    EMAIL_SEND = "outreach.email.send"          # Request email send
    WHATSAPP_QUEUED = "outreach.whatsapp.queued"  # WhatsApp queued
    WHATSAPP_SEND = "outreach.whatsapp.send"    # Request WhatsApp send
    OUTREACH_COMPLETE = "outreach.complete"     # All outreach done


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default delays between outreach attempts
DEFAULT_EMAIL_DELAY_HOURS = 0      # Send email immediately
DEFAULT_ESCALATION_DELAY_HOURS = 24  # Wait 24h before WhatsApp escalation
DEFAULT_MAX_EMAIL_RETRIES = 2
DEFAULT_MAX_WHATSAPP_RETRIES = 1


# =============================================================================
# OUTREACH ORCHESTRATOR
# =============================================================================

class OutreachOrchestrator(BaseAgent):
    """Orchestrates multi-channel outreach for leads.
    
    The orchestrator manages state transitions and coordinates channel agents
    via the MessageBus. It does NOT send emails or WhatsApp messages directly.
    
    Design Principles:
        1. State transitions are atomic via StateStore CAS
        2. Events are fire-and-forget (MessageBus never raises)
        3. Each lead has independent state progression
        4. Idempotent: re-running with same leads advances state appropriately
    
    Attributes:
        state_store: StateStore for lead outreach states
        message_bus: MessageBus for publishing events
        email_delay: Timedelta before sending email
        escalation_delay: Timedelta before WhatsApp escalation
    """

    def __init__(
        self,
        state_store: StateStore | None = None,
        message_bus: MessageBus | None = None,
        email_delay_hours: int = DEFAULT_EMAIL_DELAY_HOURS,
        escalation_delay_hours: int = DEFAULT_ESCALATION_DELAY_HOURS,
        max_email_retries: int = DEFAULT_MAX_EMAIL_RETRIES,
        max_whatsapp_retries: int = DEFAULT_MAX_WHATSAPP_RETRIES,
    ) -> None:
        """Initialize the outreach orchestrator.
        
        Args:
            state_store: StateStore instance (creates new if None)
            message_bus: MessageBus instance (creates new if None)
            email_delay_hours: Hours to wait before first email
            escalation_delay_hours: Hours to wait before WhatsApp escalation
            max_email_retries: Maximum email send attempts
            max_whatsapp_retries: Maximum WhatsApp send attempts
        """
        super().__init__(name="OutreachOrchestrator")
        
        self.state_store = state_store or StateStore()
        self.message_bus = message_bus or MessageBus()
        
        self.email_delay = timedelta(hours=email_delay_hours)
        self.escalation_delay = timedelta(hours=escalation_delay_hours)
        self.max_email_retries = max_email_retries
        self.max_whatsapp_retries = max_whatsapp_retries
        
        self.logger = get_logger(__name__)
        self.logger.info(
            f"OutreachOrchestrator initialized "
            f"(email_delay: {email_delay_hours}h, escalation_delay: {escalation_delay_hours}h)"
        )

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """Process leads and advance outreach state machine.
        
        Reads from 'exported_leads' and processes each lead through
        the outreach state machine.
        
        Args:
            context: Pipeline context with 'exported_leads'
            
        Returns:
            Updated context with 'outreach_results' containing state transitions
        """
        exported_leads = context.get("exported_leads", [])
        
        results = {
            "processed": 0,
            "queued_email": 0,
            "queued_whatsapp": 0,
            "responded": 0,
            "exhausted": 0,
            "errors": [],
            "state_transitions": [],
        }
        
        if not exported_leads:
            self.logger.info("No leads to process for outreach")
            return {**context, "outreach_results": results}
        
        for lead in exported_leads:
            try:
                transition = self._process_lead(lead)
                if transition:
                    results["state_transitions"].append(transition)
                    results["processed"] += 1
                    
                    # Count by new state
                    new_state = transition.get("new_state")
                    if new_state == OutreachState.QUEUED_EMAIL.value:
                        results["queued_email"] += 1
                    elif new_state == OutreachState.QUEUED_WHATSAPP.value:
                        results["queued_whatsapp"] += 1
                    elif new_state == OutreachState.RESPONDED.value:
                        results["responded"] += 1
                    elif new_state == OutreachState.EXHAUSTED.value:
                        results["exhausted"] += 1
                        
            except Exception as e:
                self.logger.error(f"Error processing lead {lead.get('dedup_key', 'unknown')}: {e}")
                results["errors"].append({
                    "dedup_key": lead.get("dedup_key"),
                    "error": str(e),
                })
        
        self.logger.info(
            f"Outreach orchestration complete: "
            f"{results['processed']} processed, "
            f"{results['queued_email']} queued for email, "
            f"{results['queued_whatsapp']} queued for WhatsApp"
        )
        
        return {**context, "outreach_results": results}

    def _process_lead(self, lead: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Process a single lead through the state machine.
        
        Args:
            lead: Lead data dictionary
            
        Returns:
            State transition record, or None if no transition
        """
        dedup_key = lead.get("dedup_key")
        if not dedup_key:
            self.logger.warning("Lead missing dedup_key, skipping")
            return None
        
        # Only process TARGET leads
        lead_route = lead.get("lead_route", "").upper()
        if lead_route != "TARGET":
            return None
        
        # Get or initialize state
        state_key = f"outreach:{dedup_key}"
        raw_state = self.state_store.get(state_key)
        current_state = raw_state or {"state": OutreachState.PENDING.value}
        
        # Determine next state
        next_state = self._compute_next_state(current_state, lead)
        
        if next_state == current_state.get("state"):
            return None  # No transition needed
        
        # Attempt atomic state transition
        new_state_data = {
            "state": next_state,
            "lead": lead,
            "updated_at": datetime.now().isoformat(),
            "previous_state": current_state.get("state"),
        }
        
        # CAS expects None if key didn't exist, otherwise the current value
        success = self.state_store.compare_and_set(
            key=state_key,
            expected=raw_state,  # Use raw_state (None if new, else actual value)
            new_value=new_state_data,
        )
        
        if not success:
            self.logger.warning(f"CAS failed for {dedup_key}, concurrent modification detected")
            return None
        
        # Publish event for the transition
        self._publish_event(next_state, lead)
        
        return {
            "dedup_key": dedup_key,
            "old_state": current_state.get("state", OutreachState.PENDING.value),
            "new_state": next_state,
            "timestamp": datetime.now().isoformat(),
        }

    def _get_lead_state(self, state_key: str) -> dict[str, Any]:
        """Get current state for a lead, initializing if needed.
        
        Args:
            state_key: State store key for the lead
            
        Returns:
            Current state data
        """
        state = self.state_store.get(state_key)
        if state is None:
            return {"state": OutreachState.PENDING.value}
        return state

    def _compute_next_state(
        self, 
        current_state: dict[str, Any], 
        lead: dict[str, Any]
    ) -> str:
        """Compute the next state based on current state and lead data.
        
        State machine logic:
            PENDING → QUEUED_EMAIL (if email available)
            PENDING → QUEUED_WHATSAPP (if no email, but has phone)
            QUEUED_EMAIL → (wait for channel agent to advance)
            EMAIL_SENT → QUEUED_WHATSAPP (after escalation delay)
            EMAIL_FAILED → QUEUED_WHATSAPP (immediate escalation)
            WHATSAPP_SENT → (wait for response or timeout)
            WHATSAPP_FAILED → EXHAUSTED
            
        Args:
            current_state: Current state data
            lead: Lead data dictionary
            
        Returns:
            Next state value
        """
        state = current_state.get("state", OutreachState.PENDING.value)
        
        # Terminal states - no further progression
        if state in (OutreachState.RESPONDED.value, OutreachState.EXHAUSTED.value):
            return state
        
        # Check contact availability
        has_email = bool(lead.get("email"))
        has_phone = bool(lead.get("phone"))
        
        # PENDING → queue first available channel
        if state == OutreachState.PENDING.value:
            if has_email:
                return OutreachState.QUEUED_EMAIL.value
            elif has_phone:
                return OutreachState.QUEUED_WHATSAPP.value
            else:
                return OutreachState.EXHAUSTED.value
        
        # EMAIL_SENT or EMAIL_FAILED → escalate to WhatsApp
        if state in (OutreachState.EMAIL_SENT.value, OutreachState.EMAIL_FAILED.value):
            if has_phone:
                return OutreachState.QUEUED_WHATSAPP.value
            else:
                return OutreachState.EXHAUSTED.value
        
        # WHATSAPP_FAILED → exhausted
        if state == OutreachState.WHATSAPP_FAILED.value:
            return OutreachState.EXHAUSTED.value
        
        # States waiting for channel agent action - no autonomous transition
        return state

    def _publish_event(self, new_state: str, lead: dict[str, Any]) -> None:
        """Publish event for state transition.
        
        Args:
            new_state: The new state value
            lead: Lead data dictionary
        """
        event_map = {
            OutreachState.QUEUED_EMAIL.value: OutreachEvent.EMAIL_SEND.value,
            OutreachState.QUEUED_WHATSAPP.value: OutreachEvent.WHATSAPP_SEND.value,
            OutreachState.EXHAUSTED.value: OutreachEvent.OUTREACH_COMPLETE.value,
            OutreachState.RESPONDED.value: OutreachEvent.OUTREACH_COMPLETE.value,
        }
        
        event = event_map.get(new_state)
        if event:
            self.message_bus.publish(event, {
                "dedup_key": lead.get("dedup_key"),
                "lead": lead,
                "state": new_state,
                "timestamp": datetime.now().isoformat(),
            })

    # =========================================================================
    # CHANNEL CALLBACKS
    # =========================================================================

    def mark_email_sent(self, dedup_key: str) -> bool:
        """Mark email as sent for a lead.
        
        Called by EmailOutreachAgent after successful send.
        
        Args:
            dedup_key: Lead deduplication key
            
        Returns:
            True if state updated successfully
        """
        return self._transition_state(
            dedup_key,
            expected_state=OutreachState.QUEUED_EMAIL.value,
            new_state=OutreachState.EMAIL_SENT.value,
        )

    def mark_email_failed(self, dedup_key: str, error: str = "") -> bool:
        """Mark email as failed for a lead.
        
        Args:
            dedup_key: Lead deduplication key
            error: Error message
            
        Returns:
            True if state updated successfully
        """
        return self._transition_state(
            dedup_key,
            expected_state=OutreachState.QUEUED_EMAIL.value,
            new_state=OutreachState.EMAIL_FAILED.value,
            metadata={"error": error},
        )

    def mark_whatsapp_sent(self, dedup_key: str) -> bool:
        """Mark WhatsApp as sent for a lead.
        
        Args:
            dedup_key: Lead deduplication key
            
        Returns:
            True if state updated successfully
        """
        return self._transition_state(
            dedup_key,
            expected_state=OutreachState.QUEUED_WHATSAPP.value,
            new_state=OutreachState.WHATSAPP_SENT.value,
        )

    def mark_whatsapp_failed(self, dedup_key: str, error: str = "") -> bool:
        """Mark WhatsApp as failed for a lead.
        
        Args:
            dedup_key: Lead deduplication key
            error: Error message
            
        Returns:
            True if state updated successfully
        """
        return self._transition_state(
            dedup_key,
            expected_state=OutreachState.QUEUED_WHATSAPP.value,
            new_state=OutreachState.WHATSAPP_FAILED.value,
            metadata={"error": error},
        )

    def mark_responded(self, dedup_key: str, channel: str = "unknown") -> bool:
        """Mark lead as responded.
        
        Args:
            dedup_key: Lead deduplication key
            channel: Which channel received the response
            
        Returns:
            True if state updated successfully
        """
        state_key = f"outreach:{dedup_key}"
        current = self.state_store.get(state_key) or {}
        
        new_state = {
            **current,
            "state": OutreachState.RESPONDED.value,
            "responded_at": datetime.now().isoformat(),
            "response_channel": channel,
        }
        
        self.state_store.set(state_key, new_state)
        return True

    def _transition_state(
        self,
        dedup_key: str,
        expected_state: str,
        new_state: str,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Atomic state transition helper.
        
        Args:
            dedup_key: Lead deduplication key
            expected_state: Expected current state
            new_state: New state to transition to
            metadata: Additional metadata to store
            
        Returns:
            True if transition successful
        """
        state_key = f"outreach:{dedup_key}"
        current = self.state_store.get(state_key) or {}
        
        if current.get("state") != expected_state:
            self.logger.warning(
                f"State mismatch for {dedup_key}: "
                f"expected {expected_state}, got {current.get('state')}"
            )
            return False
        
        new_state_data = {
            **current,
            "state": new_state,
            "updated_at": datetime.now().isoformat(),
            "previous_state": expected_state,
        }
        if metadata:
            new_state_data.update(metadata)
        
        return self.state_store.compare_and_set(
            key=state_key,
            expected=current,
            new_value=new_state_data,
        )

    def get_lead_state(self, dedup_key: str) -> Optional[dict[str, Any]]:
        """Get current outreach state for a lead.
        
        Args:
            dedup_key: Lead deduplication key
            
        Returns:
            State data or None if not found
        """
        return self.state_store.get(f"outreach:{dedup_key}")
