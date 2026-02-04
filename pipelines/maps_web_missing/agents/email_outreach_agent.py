"""Email Outreach Agent - Phase 6 Channel Agent.

Handles email sending for outreach. Subscribes to EMAIL_SEND events
from OutreachOrchestrator and updates state via callbacks.

Design:
    - Mock mode for testing (MOCK_EMAIL=true)
    - Configurable SMTP or API-based sending
    - Template-based email content
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
MOCK_EMAIL = os.getenv("MOCK_EMAIL", "true").lower() in ("true", "1", "yes")


class EmailSender(Protocol):
    """Protocol for email sending implementations."""
    
    def send(
        self,
        to: str,
        subject: str,
        body: str,
        from_email: Optional[str] = None,
    ) -> bool:
        """Send an email. Returns True on success."""
        ...


class MockEmailSender:
    """Mock email sender for testing."""
    
    def __init__(self):
        self.sent_emails: list[dict[str, Any]] = []
        self.logger = get_logger(__name__)
    
    def send(
        self,
        to: str,
        subject: str,
        body: str,
        from_email: Optional[str] = None,
    ) -> bool:
        """Log email instead of sending."""
        email = {
            "to": to,
            "subject": subject,
            "body": body,
            "from": from_email,
            "sent_at": datetime.now().isoformat(),
        }
        self.sent_emails.append(email)
        self.logger.info(f"MOCK EMAIL: To={to}, Subject={subject}")
        return True


# Default email template
DEFAULT_EMAIL_SUBJECT = "Your Business Needs a Website - Let's Talk"
DEFAULT_EMAIL_TEMPLATE = """Hello,

I noticed that {business_name} doesn't have a website yet. In today's digital age, 
having an online presence is crucial for attracting new customers.

I'd love to help you get started with a professional, affordable website that:
- Helps customers find you online
- Showcases your services 24/7
- Builds trust and credibility

Would you be interested in a free consultation to discuss your options?

Best regards,
Your Web Development Partner

---
If you prefer not to receive these emails, please let us know.
"""


class EmailOutreachAgent:
    """Handles email outreach for leads.
    
    This agent listens for EMAIL_SEND events from the orchestrator
    and sends emails using the configured sender.
    
    Attributes:
        orchestrator: Reference to OutreachOrchestrator for callbacks
        sender: Email sender implementation
        subject: Email subject template
        template: Email body template
    """

    def __init__(
        self,
        orchestrator: OutreachOrchestrator,
        message_bus: MessageBus,
        sender: EmailSender | None = None,
        subject: str = DEFAULT_EMAIL_SUBJECT,
        template: str = DEFAULT_EMAIL_TEMPLATE,
    ) -> None:
        """Initialize the email outreach agent.
        
        Args:
            orchestrator: OutreachOrchestrator for state callbacks
            message_bus: MessageBus to subscribe to
            sender: Email sender implementation (uses mock if None)
            subject: Email subject template
            template: Email body template
        """
        self.orchestrator = orchestrator
        self.message_bus = message_bus
        self.sender = sender or MockEmailSender()
        self.subject = subject
        self.template = template
        
        self.logger = get_logger(__name__)
        self._is_mock = isinstance(self.sender, MockEmailSender)
        
        self.logger.info(f"EmailOutreachAgent initialized (mock={self._is_mock})")

    def start(self) -> None:
        """Subscribe to email send events."""
        self.message_bus.subscribe(
            OutreachEvent.EMAIL_SEND.value,
            self._handle_email_send,
        )
        self.logger.info("EmailOutreachAgent subscribed to EMAIL_SEND events")

    def stop(self) -> None:
        """Unsubscribe from events."""
        self.message_bus.unsubscribe(
            OutreachEvent.EMAIL_SEND.value,
            self._handle_email_send,
        )
        self.logger.info("EmailOutreachAgent unsubscribed from events")

    def _handle_email_send(self, payload: dict[str, Any]) -> None:
        """Handle EMAIL_SEND event.
        
        Args:
            payload: Event payload with lead data
        """
        dedup_key = payload.get("dedup_key")
        lead = payload.get("lead", {})
        
        if not dedup_key:
            self.logger.warning("EMAIL_SEND event missing dedup_key")
            return
        
        email = lead.get("email")
        if not email:
            self.logger.warning(f"Lead {dedup_key} has no email address")
            self.orchestrator.mark_email_failed(dedup_key, "No email address")
            return
        
        # Render email content
        business_name = lead.get("name", "Your Business")
        subject = self.subject
        body = self.template.format(business_name=business_name)
        
        try:
            success = self.sender.send(
                to=email,
                subject=subject,
                body=body,
            )
            
            if success:
                self.orchestrator.mark_email_sent(dedup_key)
                self.logger.info(f"Email sent to {email} for lead {dedup_key}")
            else:
                self.orchestrator.mark_email_failed(dedup_key, "Send returned False")
                self.logger.warning(f"Email send failed for {dedup_key}")
                
        except Exception as e:
            self.orchestrator.mark_email_failed(dedup_key, str(e))
            self.logger.error(f"Email send error for {dedup_key}: {e}")

    def get_sent_count(self) -> int:
        """Get count of emails sent (for mock sender only)."""
        if isinstance(self.sender, MockEmailSender):
            return len(self.sender.sent_emails)
        return -1  # Unknown for real senders
