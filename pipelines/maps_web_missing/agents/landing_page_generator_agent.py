"""Landing Page Generator Agent - Phase 5.

Generates personalized landing pages for leads with missing websites.
This agent runs POST-EXPORT and is fully idempotent.

Key Design Decisions:
    1. POST-EXPORT: Runs after GoogleSheetsExportAgent to ensure only
       successfully exported leads get landing pages
    2. IDEMPOTENT: Uses uuid5(NAMESPACE, dedup_key) for stable page IDs
    3. TARGET-ONLY: Only generates pages for leads routed to TARGET sheet
    4. TEMPLATE-BASED: Simple configurable templates (no LLM calls)

The landing page workflow:
    1. Receive exported_leads from GoogleSheetsExportAgent
    2. Filter to TARGET-routed leads only
    3. Generate stable page_id via uuid5
    4. Skip if page already exists (idempotent)
    5. Render template with lead data
    6. Save to configured output directory
"""

import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from core.logger import get_logger
from pipelines.core.base_agent import BaseAgent

# UUID5 namespace for landing pages (stable across runs)
LANDING_PAGE_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

# Default templates
DEFAULT_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{business_name} - Get Your Website Today</title>
    <style>
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; padding: 40px 20px; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 60px 40px; border-radius: 16px; text-align: center; }}
        .header h1 {{ margin: 0 0 16px 0; font-size: 2.5rem; }}
        .header p {{ margin: 0; font-size: 1.2rem; opacity: 0.9; }}
        .content {{ background: white; padding: 40px; border-radius: 16px; margin-top: 24px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .cta {{ background: #667eea; color: white; padding: 16px 32px; border: none; border-radius: 8px; font-size: 1.1rem; cursor: pointer; display: inline-block; text-decoration: none; margin-top: 24px; }}
        .cta:hover {{ background: #5a6fd6; }}
        .contact {{ margin-top: 32px; padding: 24px; background: #f8f9fa; border-radius: 8px; }}
        .footer {{ text-align: center; margin-top: 40px; color: #666; font-size: 0.9rem; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{business_name}</h1>
            <p>{industry_text}</p>
        </div>
        <div class="content">
            <h2>Your Business Deserves a Professional Website</h2>
            <p>In today's digital world, having a website isn't optional—it's essential. 
            Let us help you establish your online presence and reach more customers.</p>
            
            <h3>Why You Need a Website:</h3>
            <ul>
                <li>Be found by customers searching online</li>
                <li>Showcase your products and services 24/7</li>
                <li>Build credibility and trust</li>
                <li>Compete with larger businesses</li>
            </ul>
            
            <a href="mailto:contact@example.com?subject=Website%20for%20{business_name_encoded}" class="cta">
                Get Your Free Quote
            </a>
            
            <div class="contact">
                <h3>Current Contact Information</h3>
                <p><strong>Address:</strong> {address}</p>
                <p><strong>Phone:</strong> {phone}</p>
            </div>
        </div>
        <div class="footer">
            <p>Page ID: {page_id}</p>
            <p>Generated: {generated_at}</p>
        </div>
    </div>
</body>
</html>
"""

DEFAULT_MARKDOWN_TEMPLATE = """# {business_name}

{industry_text}

---

## Your Business Deserves a Professional Website

In today's digital world, having a website isn't optional—it's essential.
Let us help you establish your online presence and reach more customers.

### Why You Need a Website:

- Be found by customers searching online
- Showcase your products and services 24/7
- Build credibility and trust
- Compete with larger businesses

---

## Current Contact Information

- **Address:** {address}
- **Phone:** {phone}

---

[Get Your Free Quote](mailto:contact@example.com?subject=Website%20for%20{business_name_encoded})

---

*Page ID: {page_id} | Generated: {generated_at}*
"""


class LandingPageGeneratorAgent(BaseAgent):
    """Generates landing pages for TARGET leads post-export.
    
    This agent is idempotent: re-running with the same leads will not
    regenerate existing pages. Page IDs are derived via uuid5 from
    the lead's dedup_key, ensuring stable identification.
    
    Attributes:
        output_dir: Directory to write landing pages
        format: Output format ('html' or 'markdown')
        template: Custom template string (uses defaults if None)
    """

    def __init__(
        self,
        output_dir: str | None = None,
        format: str = "html",
        template: str | None = None,
    ) -> None:
        """Initialize the landing page generator.
        
        Args:
            output_dir: Directory for output files. Defaults to data/outputs/landing_pages
            format: Output format, 'html' or 'markdown'
            template: Custom template. Uses DEFAULT_HTML_TEMPLATE or 
                     DEFAULT_MARKDOWN_TEMPLATE if None
        """
        super().__init__(name="LandingPageGeneratorAgent")
        
        self.output_dir = Path(output_dir) if output_dir else Path("data/outputs/landing_pages")
        self.format = format.lower()
        
        if template:
            self.template = template
        elif self.format == "markdown":
            self.template = DEFAULT_MARKDOWN_TEMPLATE
        else:
            self.template = DEFAULT_HTML_TEMPLATE
        
        self.logger = get_logger(__name__)
        self.logger.info(
            f"LandingPageGeneratorAgent initialized "
            f"(output_dir: {self.output_dir}, format: {self.format})"
        )

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """Generate landing pages for TARGET leads.
        
        Reads from 'exported_leads' (output of GoogleSheetsExportAgent).
        Only processes leads with lead_route == 'TARGET'.
        
        Args:
            context: Pipeline context with 'exported_leads'
            
        Returns:
            Updated context with 'landing_pages' containing generation results
        """
        exported_leads = context.get("exported_leads", [])
        
        if not exported_leads:
            self.logger.warning("No exported leads found, skipping landing page generation")
            return {
                **context,
                "landing_pages": {
                    "generated": [],
                    "skipped_existing": [],
                    "skipped_non_target": [],
                    "errors": [],
                },
            }
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        results = {
            "generated": [],
            "skipped_existing": [],
            "skipped_non_target": [],
            "errors": [],
        }
        
        for lead in exported_leads:
            try:
                self._process_lead(lead, results)
            except Exception as e:
                self.logger.error(f"Error processing lead {lead.get('dedup_key', 'unknown')}: {e}")
                results["errors"].append({
                    "dedup_key": lead.get("dedup_key"),
                    "error": str(e),
                })
        
        # Log summary
        self.logger.info(
            f"Landing page generation complete: "
            f"{len(results['generated'])} generated, "
            f"{len(results['skipped_existing'])} skipped (existing), "
            f"{len(results['skipped_non_target'])} skipped (non-target), "
            f"{len(results['errors'])} errors"
        )
        
        return {
            **context,
            "landing_pages": results,
        }

    def _process_lead(self, lead: dict[str, Any], results: dict[str, list]) -> None:
        """Process a single lead for landing page generation.
        
        Args:
            lead: Lead data dictionary
            results: Results dictionary to update
        """
        dedup_key = lead.get("dedup_key")
        lead_route = lead.get("lead_route", "").upper()
        
        # Only generate for TARGET leads
        if lead_route != "TARGET":
            results["skipped_non_target"].append({
                "dedup_key": dedup_key,
                "lead_route": lead_route,
            })
            return
        
        # Generate stable page ID
        page_id = self._generate_page_id(dedup_key)
        
        # Check if page already exists (idempotent)
        file_ext = ".html" if self.format == "html" else ".md"
        output_path = self.output_dir / f"{page_id}{file_ext}"
        
        if output_path.exists():
            results["skipped_existing"].append({
                "dedup_key": dedup_key,
                "page_id": page_id,
                "path": str(output_path),
            })
            return
        
        # Render template
        content = self._render_template(lead, page_id)
        
        # Write to file
        output_path.write_text(content, encoding="utf-8")
        
        results["generated"].append({
            "dedup_key": dedup_key,
            "page_id": page_id,
            "path": str(output_path),
            "business_name": lead.get("name", "Unknown Business"),
        })

    def _generate_page_id(self, dedup_key: str) -> str:
        """Generate a stable page ID from the dedup_key.
        
        Uses uuid5 with a fixed namespace to ensure:
        1. Same dedup_key always produces same page_id
        2. Different dedup_keys produce different page_ids
        3. Page IDs are URL-safe and filesystem-safe
        
        Args:
            dedup_key: The lead's deduplication key
            
        Returns:
            Stable page ID string
        """
        return str(uuid.uuid5(LANDING_PAGE_NAMESPACE, dedup_key))

    def _render_template(self, lead: dict[str, Any], page_id: str) -> str:
        """Render the landing page template with lead data.
        
        Args:
            lead: Lead data dictionary
            page_id: Generated page ID
            
        Returns:
            Rendered template string
        """
        import urllib.parse
        
        business_name = lead.get("name", "Your Business")
        
        # Extract industry from enrichment if available
        enrichment = lead.get("enrichment", {})
        industry = enrichment.get("industry", {}).get("primary", "Local Business")
        industry_text = f"Professional {industry} Services"
        
        # Template variables
        variables = {
            "business_name": business_name,
            "business_name_encoded": urllib.parse.quote(business_name),
            "industry_text": industry_text,
            "address": lead.get("address", "Contact us for location"),
            "phone": lead.get("phone", "Contact us"),
            "page_id": page_id,
            "generated_at": datetime.now().isoformat(),
        }
        
        return self.template.format(**variables)
