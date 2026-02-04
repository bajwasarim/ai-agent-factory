"""Tests for OutreachOrchestrator.

Verifies Phase 6 outreach state machine:
    1. State transitions (PENDING → EMAIL → WHATSAPP → EXHAUSTED)
    2. Atomic CAS-based transitions
    3. Event publishing
    4. Channel callbacks
    5. TARGET-only filtering
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from core.infrastructure import StateStore, MessageBus
from pipelines.maps_web_missing.agents.outreach_orchestrator import (
    OutreachOrchestrator,
    OutreachState,
    OutreachEvent,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def fresh_state_store():
    """Create a fresh StateStore for each test."""
    store = StateStore()
    store._data.clear()
    return store


@pytest.fixture
def fresh_message_bus():
    """Create a fresh MessageBus for each test."""
    bus = MessageBus()
    bus._subscribers.clear()
    bus._event_history.clear()
    return bus


@pytest.fixture
def orchestrator(fresh_state_store, fresh_message_bus):
    """Create an OutreachOrchestrator with fresh dependencies."""
    return OutreachOrchestrator(
        state_store=fresh_state_store,
        message_bus=fresh_message_bus,
    )


@pytest.fixture
def target_lead_with_email():
    """A TARGET lead with email."""
    return {
        "name": "Email Business",
        "email": "contact@email-business.com",
        "phone": "+1-555-123-4567",
        "dedup_key": "pid:email001",
        "lead_route": "TARGET",
        "target_sheet": "WebsiteLeads",
    }


@pytest.fixture
def target_lead_with_phone_only():
    """A TARGET lead with phone but no email."""
    return {
        "name": "Phone Only Business",
        "phone": "+1-555-222-3333",
        "dedup_key": "pid:phone001",
        "lead_route": "TARGET",
        "target_sheet": "WebsiteLeads",
    }


@pytest.fixture
def target_lead_no_contact():
    """A TARGET lead with no contact info."""
    return {
        "name": "No Contact Business",
        "dedup_key": "pid:nocontact001",
        "lead_route": "TARGET",
        "target_sheet": "WebsiteLeads",
    }


@pytest.fixture
def excluded_lead():
    """An EXCLUDED lead."""
    return {
        "name": "Has Website",
        "email": "test@example.com",
        "dedup_key": "pid:excluded001",
        "lead_route": "EXCLUDED",
        "target_sheet": "ExcludedLeads",
    }


# =============================================================================
# INITIALIZATION TESTS
# =============================================================================

class TestOrchestratorInit:
    """Tests for orchestrator initialization."""

    def test_default_initialization(self, fresh_state_store, fresh_message_bus):
        """Orchestrator initializes with defaults."""
        orch = OutreachOrchestrator(
            state_store=fresh_state_store,
            message_bus=fresh_message_bus,
        )

        assert orch.name == "OutreachOrchestrator"
        assert orch.state_store is fresh_state_store
        assert orch.message_bus is fresh_message_bus

    def test_custom_delays(self, fresh_state_store, fresh_message_bus):
        """Orchestrator accepts custom delay configuration."""
        orch = OutreachOrchestrator(
            state_store=fresh_state_store,
            message_bus=fresh_message_bus,
            email_delay_hours=2,
            escalation_delay_hours=48,
        )

        assert orch.email_delay.total_seconds() == 2 * 3600
        assert orch.escalation_delay.total_seconds() == 48 * 3600


# =============================================================================
# STATE TRANSITION TESTS
# =============================================================================

class TestStateTransitions:
    """Tests for state machine transitions."""

    def test_pending_to_queued_email(self, orchestrator, target_lead_with_email):
        """PENDING → QUEUED_EMAIL for leads with email."""
        context = {"exported_leads": [target_lead_with_email]}

        result = orchestrator.run(context)

        transitions = result["outreach_results"]["state_transitions"]
        assert len(transitions) == 1
        assert transitions[0]["old_state"] == OutreachState.PENDING.value
        assert transitions[0]["new_state"] == OutreachState.QUEUED_EMAIL.value

    def test_pending_to_queued_whatsapp(self, orchestrator, target_lead_with_phone_only):
        """PENDING → QUEUED_WHATSAPP for leads with phone only."""
        context = {"exported_leads": [target_lead_with_phone_only]}

        result = orchestrator.run(context)

        transitions = result["outreach_results"]["state_transitions"]
        assert len(transitions) == 1
        assert transitions[0]["new_state"] == OutreachState.QUEUED_WHATSAPP.value

    def test_pending_to_exhausted_no_contact(self, orchestrator, target_lead_no_contact):
        """PENDING → EXHAUSTED for leads with no contact info."""
        context = {"exported_leads": [target_lead_no_contact]}

        result = orchestrator.run(context)

        transitions = result["outreach_results"]["state_transitions"]
        assert len(transitions) == 1
        assert transitions[0]["new_state"] == OutreachState.EXHAUSTED.value

    def test_email_sent_escalates_to_whatsapp(self, orchestrator, target_lead_with_email, fresh_state_store):
        """EMAIL_SENT → QUEUED_WHATSAPP for leads with phone."""
        # Pre-set state to EMAIL_SENT
        state_key = f"outreach:{target_lead_with_email['dedup_key']}"
        fresh_state_store.set(state_key, {"state": OutreachState.EMAIL_SENT.value})

        context = {"exported_leads": [target_lead_with_email]}
        result = orchestrator.run(context)

        transitions = result["outreach_results"]["state_transitions"]
        assert len(transitions) == 1
        assert transitions[0]["new_state"] == OutreachState.QUEUED_WHATSAPP.value

    def test_email_failed_escalates_to_whatsapp(self, orchestrator, target_lead_with_email, fresh_state_store):
        """EMAIL_FAILED → QUEUED_WHATSAPP immediately."""
        state_key = f"outreach:{target_lead_with_email['dedup_key']}"
        fresh_state_store.set(state_key, {"state": OutreachState.EMAIL_FAILED.value})

        context = {"exported_leads": [target_lead_with_email]}
        result = orchestrator.run(context)

        transitions = result["outreach_results"]["state_transitions"]
        assert len(transitions) == 1
        assert transitions[0]["new_state"] == OutreachState.QUEUED_WHATSAPP.value

    def test_whatsapp_failed_to_exhausted(self, orchestrator, target_lead_with_email, fresh_state_store):
        """WHATSAPP_FAILED → EXHAUSTED."""
        state_key = f"outreach:{target_lead_with_email['dedup_key']}"
        fresh_state_store.set(state_key, {"state": OutreachState.WHATSAPP_FAILED.value})

        context = {"exported_leads": [target_lead_with_email]}
        result = orchestrator.run(context)

        transitions = result["outreach_results"]["state_transitions"]
        assert len(transitions) == 1
        assert transitions[0]["new_state"] == OutreachState.EXHAUSTED.value

    def test_responded_is_terminal(self, orchestrator, target_lead_with_email, fresh_state_store):
        """RESPONDED is terminal - no further transitions."""
        state_key = f"outreach:{target_lead_with_email['dedup_key']}"
        fresh_state_store.set(state_key, {"state": OutreachState.RESPONDED.value})

        context = {"exported_leads": [target_lead_with_email]}
        result = orchestrator.run(context)

        # No transitions - state is terminal
        assert len(result["outreach_results"]["state_transitions"]) == 0

    def test_exhausted_is_terminal(self, orchestrator, target_lead_with_email, fresh_state_store):
        """EXHAUSTED is terminal - no further transitions."""
        state_key = f"outreach:{target_lead_with_email['dedup_key']}"
        fresh_state_store.set(state_key, {"state": OutreachState.EXHAUSTED.value})

        context = {"exported_leads": [target_lead_with_email]}
        result = orchestrator.run(context)

        assert len(result["outreach_results"]["state_transitions"]) == 0


# =============================================================================
# TARGET-ONLY FILTERING TESTS
# =============================================================================

class TestTargetOnlyFiltering:
    """Tests for filtering to TARGET leads only."""

    def test_excluded_leads_skipped(self, orchestrator, excluded_lead):
        """EXCLUDED leads are not processed."""
        context = {"exported_leads": [excluded_lead]}

        result = orchestrator.run(context)

        assert result["outreach_results"]["processed"] == 0
        assert len(result["outreach_results"]["state_transitions"]) == 0

    def test_mixed_leads_filters_target(self, orchestrator, target_lead_with_email, excluded_lead):
        """Only TARGET leads processed from mixed input."""
        context = {"exported_leads": [target_lead_with_email, excluded_lead]}

        result = orchestrator.run(context)

        assert result["outreach_results"]["processed"] == 1


# =============================================================================
# CHANNEL CALLBACK TESTS
# =============================================================================

class TestChannelCallbacks:
    """Tests for channel agent callbacks."""

    def test_mark_email_sent(self, orchestrator, target_lead_with_email, fresh_state_store):
        """mark_email_sent transitions from QUEUED_EMAIL to EMAIL_SENT."""
        dedup_key = target_lead_with_email["dedup_key"]
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})

        success = orchestrator.mark_email_sent(dedup_key)

        assert success is True
        new_state = fresh_state_store.get(state_key)
        assert new_state["state"] == OutreachState.EMAIL_SENT.value

    def test_mark_email_failed(self, orchestrator, target_lead_with_email, fresh_state_store):
        """mark_email_failed transitions with error metadata."""
        dedup_key = target_lead_with_email["dedup_key"]
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})

        success = orchestrator.mark_email_failed(dedup_key, "SMTP error")

        assert success is True
        new_state = fresh_state_store.get(state_key)
        assert new_state["state"] == OutreachState.EMAIL_FAILED.value
        assert new_state["error"] == "SMTP error"

    def test_mark_whatsapp_sent(self, orchestrator, target_lead_with_email, fresh_state_store):
        """mark_whatsapp_sent transitions from QUEUED_WHATSAPP."""
        dedup_key = target_lead_with_email["dedup_key"]
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_WHATSAPP.value})

        success = orchestrator.mark_whatsapp_sent(dedup_key)

        assert success is True
        new_state = fresh_state_store.get(state_key)
        assert new_state["state"] == OutreachState.WHATSAPP_SENT.value

    def test_mark_responded(self, orchestrator, target_lead_with_email, fresh_state_store):
        """mark_responded sets terminal state with channel info."""
        dedup_key = target_lead_with_email["dedup_key"]
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.EMAIL_SENT.value})

        success = orchestrator.mark_responded(dedup_key, channel="email")

        assert success is True
        new_state = fresh_state_store.get(state_key)
        assert new_state["state"] == OutreachState.RESPONDED.value
        assert new_state["response_channel"] == "email"

    def test_callback_wrong_state_fails(self, orchestrator, target_lead_with_email, fresh_state_store):
        """Callback fails if current state doesn't match expected."""
        dedup_key = target_lead_with_email["dedup_key"]
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.PENDING.value})

        # Trying to mark email sent when not queued
        success = orchestrator.mark_email_sent(dedup_key)

        assert success is False


# =============================================================================
# EVENT PUBLISHING TESTS
# =============================================================================

class TestEventPublishing:
    """Tests for MessageBus event publishing."""

    def test_email_queued_publishes_event(self, orchestrator, target_lead_with_email, fresh_message_bus):
        """Queueing email publishes EMAIL_SEND event."""
        context = {"exported_leads": [target_lead_with_email]}

        orchestrator.run(context)

        history = fresh_message_bus.get_event_history()
        email_events = [e for e in history if e["event_name"] == OutreachEvent.EMAIL_SEND.value]
        assert len(email_events) == 1
        assert email_events[0]["payload"]["dedup_key"] == target_lead_with_email["dedup_key"]

    def test_whatsapp_queued_publishes_event(self, orchestrator, target_lead_with_phone_only, fresh_message_bus):
        """Queueing WhatsApp publishes WHATSAPP_SEND event."""
        context = {"exported_leads": [target_lead_with_phone_only]}

        orchestrator.run(context)

        history = fresh_message_bus.get_event_history()
        wa_events = [e for e in history if e["event_name"] == OutreachEvent.WHATSAPP_SEND.value]
        assert len(wa_events) == 1

    def test_exhausted_publishes_complete_event(self, orchestrator, target_lead_no_contact, fresh_message_bus):
        """EXHAUSTED state publishes OUTREACH_COMPLETE event."""
        context = {"exported_leads": [target_lead_no_contact]}

        orchestrator.run(context)

        history = fresh_message_bus.get_event_history()
        complete_events = [e for e in history if e["event_name"] == OutreachEvent.OUTREACH_COMPLETE.value]
        assert len(complete_events) == 1


# =============================================================================
# IDEMPOTENCY TESTS
# =============================================================================

class TestIdempotency:
    """Tests for idempotent behavior."""

    def test_reprocessing_same_lead_no_duplicate_transition(self, orchestrator, target_lead_with_email):
        """Re-running orchestrator doesn't create duplicate transitions."""
        context = {"exported_leads": [target_lead_with_email]}

        # First run
        result1 = orchestrator.run(context)
        assert len(result1["outreach_results"]["state_transitions"]) == 1

        # Second run - already in QUEUED_EMAIL, no transition needed
        result2 = orchestrator.run(context)
        assert len(result2["outreach_results"]["state_transitions"]) == 0

    def test_state_preserved_across_runs(self, orchestrator, target_lead_with_email, fresh_state_store):
        """State is preserved between orchestrator runs."""
        context = {"exported_leads": [target_lead_with_email]}

        orchestrator.run(context)

        # Verify state persisted
        state_key = f"outreach:{target_lead_with_email['dedup_key']}"
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.QUEUED_EMAIL.value


# =============================================================================
# BATCH PROCESSING TESTS
# =============================================================================

class TestBatchProcessing:
    """Tests for processing multiple leads."""

    def test_multiple_leads_processed(self, orchestrator, target_lead_with_email, target_lead_with_phone_only):
        """Multiple leads are processed independently."""
        context = {"exported_leads": [target_lead_with_email, target_lead_with_phone_only]}

        result = orchestrator.run(context)

        assert result["outreach_results"]["processed"] == 2
        assert result["outreach_results"]["queued_email"] == 1
        assert result["outreach_results"]["queued_whatsapp"] == 1

    def test_error_doesnt_stop_batch(self, orchestrator, target_lead_with_email):
        """Error in one lead doesn't stop processing others."""
        bad_lead = {"lead_route": "TARGET"}  # Missing dedup_key
        context = {"exported_leads": [bad_lead, target_lead_with_email]}

        result = orchestrator.run(context)

        # Should still process the good lead
        assert result["outreach_results"]["processed"] == 1


# =============================================================================
# EDGE CASES
# =============================================================================

class TestEdgeCases:
    """Tests for edge cases."""

    def test_empty_leads(self, orchestrator):
        """Empty input handled gracefully."""
        context = {"exported_leads": []}

        result = orchestrator.run(context)

        assert result["outreach_results"]["processed"] == 0

    def test_missing_leads_key(self, orchestrator):
        """Missing exported_leads key handled gracefully."""
        context = {}

        result = orchestrator.run(context)

        assert "outreach_results" in result
        assert result["outreach_results"]["processed"] == 0

    def test_context_passthrough(self, orchestrator, target_lead_with_email):
        """Original context fields are preserved."""
        context = {
            "exported_leads": [target_lead_with_email],
            "other_field": "preserved",
        }

        result = orchestrator.run(context)

        assert result["other_field"] == "preserved"

    def test_get_lead_state(self, orchestrator, target_lead_with_email, fresh_state_store):
        """get_lead_state returns current state."""
        dedup_key = target_lead_with_email["dedup_key"]
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.EMAIL_SENT.value})

        state = orchestrator.get_lead_state(dedup_key)

        assert state["state"] == OutreachState.EMAIL_SENT.value

    def test_get_lead_state_not_found(self, orchestrator):
        """get_lead_state returns None for unknown leads."""
        state = orchestrator.get_lead_state("nonexistent")

        assert state is None
