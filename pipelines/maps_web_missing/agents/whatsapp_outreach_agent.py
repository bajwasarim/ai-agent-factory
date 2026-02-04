"""WhatsApp Outreach Agent - Phase 6 Channel Agent.

Handles WhatsApp messaging for outreach. Subscribes to WHATSAPP_SEND events
from OutreachOrchestrator and updates state via callbacks.

Design:
    - Mock mode for testing (MOCK_WHATSAPP=true)
    - Configurable API-based messaging (Twilio, Meta Cloud API, etc.)
    - Template-based message content
    - Notifies orchestrator of success/failure via callbacks
"""

import os
from datetime import datetime
from typing import Any, Optional, Protocol

from core.infrastructure import MessageBus
from core.logger import get_logger
from pipelines.maps_web_missing.agents.outreach_orchestrator import (
    OutreachOrchestrator,
    OutreachEvent,
)

# Environment configuration
MOCK_WHATSAPP = os.getenv("MOCK_WHATSAPP", "true").lower() in ("true", "1", "yes")


class WhatsAppSender(Protocol):
    """Protocol for WhatsApp sending implementations."""
    
    def send(
        self,
        to: str,
        message: str,
        from_number: Optional[str] = None,
    ) -> bool:
        """Send a WhatsApp message. Returns True on success."""
        ...


class MockWhatsAppSender:
    """Mock WhatsApp sender for testing."""
    
    def __init__(self):
        self.sent_messages: list[dict[str, Any]] = []
        self.logger = get_logger(__name__)
    
    def send(
        self,
        to: str,
        message: str,
        from_number: Optional[str] = None,
    ) -> bool:
        """Log message instead of sending."""
        msg = {
            "to": to,
            "message": message,
            "from": from_number,
            "sent_at": datetime.now().isoformat(),
        }
        self.sent_messages.append(msg)
        self.logger.info(f"MOCK WHATSAPP: To={to}, Message length={len(message)}")
        return True


# Default WhatsApp message template
DEFAULT_WHATSAPP_TEMPLATE = """Hi! 👋

I noticed {business_name} isn't online yet. 

In 2024, 80% of customers search online before visiting a business. A simple website could help you:

✅ Get found by new customers
✅ Show off your work
✅ Build trust

Would you like a free quote? Just reply YES!

- Your friendly web dev team"""


class WhatsAppOutreachAgent:
    """Handles WhatsApp outreach for leads.
    
    This agent listens for WHATSAPP_SEND events from the orchestrator
    and sends messages using the configured sender.
    
    Attributes:
        orchestrator: Reference to OutreachOrchestrator for callbacks
        sender: WhatsApp sender implementation
        template: Message template
    """

    def __init__(
        self,
        orchestrator: OutreachOrchestrator,
        message_bus: MessageBus,
        sender: WhatsAppSender | None = None,
        template: str = DEFAULT_WHATSAPP_TEMPLATE,
    ) -> None:
        """Initialize the WhatsApp outreach agent.
        
        Args:
            orchestrator: OutreachOrchestrator for state callbacks
            message_bus: MessageBus to subscribe to
            sender: WhatsApp sender implementation (uses mock if None)
            template: Message template
        """
        self.orchestrator = orchestrator
        self.message_bus = message_bus
        self.sender = sender or MockWhatsAppSender()
        self.template = template
        
        self.logger = get_logger(__name__)
        self._is_mock = isinstance(self.sender, MockWhatsAppSender)
        
        self.logger.info(f"WhatsAppOutreachAgent initialized (mock={self._is_mock})")

    def start(self) -> None:
        """Subscribe to WhatsApp send events."""
        self.message_bus.subscribe(
            OutreachEvent.WHATSAPP_SEND.value,
            self._handle_whatsapp_send,
        )
        self.logger.info("WhatsAppOutreachAgent subscribed to WHATSAPP_SEND events")

    def stop(self) -> None:
        """Unsubscribe from events."""
        self.message_bus.unsubscribe(
            OutreachEvent.WHATSAPP_SEND.value,
            self._handle_whatsapp_send,
        )
        self.logger.info("WhatsAppOutreachAgent unsubscribed from events")

    def _handle_whatsapp_send(self, payload: dict[str, Any]) -> None:
        """Handle WHATSAPP_SEND event.
        
        Args:
            payload: Event payload with lead data
        """
        dedup_key = payload.get("dedup_key")
        lead = payload.get("lead", {})
        
        if not dedup_key:
            self.logger.warning("WHATSAPP_SEND event missing dedup_key")
            return
        
        phone = lead.get("phone")
        if not phone:
            self.logger.warning(f"Lead {dedup_key} has no phone number")
            # If no phone, mark as exhausted (can't do WhatsApp without phone)
            self._mark_channel_exhausted(dedup_key)
            return
        
        # Render message content
        business_name = lead.get("name", "Your Business")
        message = self.template.format(business_name=business_name)
        
        try:
            success = self.sender.send(
                to=phone,
                message=message,
            )
            
            if success:
                self.orchestrator.mark_whatsapp_sent(dedup_key)
                self.logger.info(f"WhatsApp sent to {phone} for lead {dedup_key}")
            else:
                self._mark_channel_exhausted(dedup_key)
                self.logger.warning(f"WhatsApp send failed for {dedup_key}")
                
        except Exception as e:
            self._mark_channel_exhausted(dedup_key)
            self.logger.error(f"WhatsApp send error for {dedup_key}: {e}")

    def _mark_channel_exhausted(self, dedup_key: str) -> None:
        """Mark WhatsApp channel as exhausted (move to next or end).
        
        For now, since WhatsApp is our last channel, we mark as whatsapp_sent
        to allow the orchestrator to handle the terminal state logic.
        In a real implementation, you might have more sophisticated error handling.
        """
        # The orchestrator will handle state transitions
        # For a failed WhatsApp, we still mark as sent to advance state
        # The lead will eventually be marked EXHAUSTED if no response
        self.orchestrator.mark_whatsapp_sent(dedup_key)

    def get_sent_count(self) -> int:
        """Get count of messages sent (for mock sender only)."""
        if isinstance(self.sender, MockWhatsAppSender):
            return len(self.sender.sent_messages)
        return -1  # Unknown for real senders
