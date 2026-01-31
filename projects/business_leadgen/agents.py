"""Business lead generation agents."""

from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from core.agent_base import BaseAgent
from core.llm_client import LLMClient
from core.logger import get_logger
from core.tool_registry import tool_registry

logger = get_logger(__name__)


class BusinessSearchAgent(BaseAgent):
    """
    Agent that searches for businesses using Serper API.
    
    Input: query, location
    Output: raw_search_results
    """

    def __init__(self) -> None:
        """Initialize the business search agent."""
        super().__init__(name="BusinessSearchAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute business search.

        Args:
            input_data: Dict with 'query' (required) and 'location' (optional).

        Returns:
            Dict with 'raw_search_results' containing Serper API response.

        Raises:
            KeyError: If 'query' is missing from input_data.
            RuntimeError: If search tool is not registered.
        """
        query = input_data["query"]
        location = input_data.get("location", "")
        num_results = input_data.get("num_results", 20)

        search = tool_registry.get("serper_search")
        if search is None:
            raise RuntimeError("serper_search tool not registered. Import core.tools.serper_tool first.")

        search_query = f"{query} in {location}" if location else query
        logger.info(f"Searching: '{search_query}'")

        raw_results = search(
            query=search_query,
            location=location,
            num_results=num_results,
        )

        result_count = len(raw_results.get("organic", []))
        logger.info(f"Found {result_count} organic results")

        return {"raw_search_results": raw_results}


class LeadExtractorAgent(BaseAgent):
    """
    Agent that extracts and structures lead data from raw search results.
    
    Input: raw_search_results
    Output: extracted_leads (list of structured lead dicts)
    """

    def __init__(self) -> None:
        """Initialize the lead extractor agent."""
        super().__init__(name="LeadExtractorAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured leads from raw search results.

        Args:
            input_data: Dict with 'raw_search_results' from BusinessSearchAgent.

        Returns:
            Dict with 'extracted_leads' list of structured lead data.
        """
        raw_results = input_data.get("raw_search_results", {})
        organic = raw_results.get("organic", [])
        location = input_data.get("location", "")

        extracted_leads: List[Dict[str, Any]] = []

        for idx, item in enumerate(organic):
            lead = {
                "id": idx + 1,
                "name": item.get("title", ""),
                "website": item.get("link", ""),
                "description": item.get("snippet", ""),
                "source": "serper",
                "location": location,
                "domain": self._extract_domain(item.get("link", "")),
                "position": item.get("position", idx + 1),
            }
            extracted_leads.append(lead)

        logger.info(f"Extracted {len(extracted_leads)} leads")
        return {"extracted_leads": extracted_leads}

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        if not url:
            return ""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.replace("www.", "")
        except Exception:
            return ""


class LeadEnricherAgent(BaseAgent):
    """
    Agent that enriches leads with LLM-generated insights.
    
    Input: extracted_leads, query
    Output: enriched_leads (leads with added business insights)
    """

    def __init__(self, llm_client: Optional[LLMClient] = None) -> None:
        """
        Initialize the lead enricher agent.

        Args:
            llm_client: Optional LLM client. Defaults to LLMClient.from_env().
        """
        super().__init__(name="LeadEnricherAgent")
        self._llm_client = llm_client

    @property
    def llm_client(self) -> LLMClient:
        """Lazy initialization of LLM client."""
        if self._llm_client is None:
            self._llm_client = LLMClient.from_env()
        return self._llm_client

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich leads with LLM-generated business insights.

        Args:
            input_data: Dict with 'extracted_leads' and 'query'.

        Returns:
            Dict with 'enriched_leads' containing enhanced lead data.
        """
        leads = input_data.get("extracted_leads", [])
        query = input_data.get("query", "business")
        max_enrich = input_data.get("max_enrich", 10)

        enriched_leads: List[Dict[str, Any]] = []

        for lead in leads[:max_enrich]:
            enriched = dict(lead)
            
            try:
                insight = self._generate_insight(lead, query)
                enriched["insight"] = insight
                enriched["enriched"] = True
            except Exception as e:
                logger.warning(f"Failed to enrich lead {lead.get('id')}: {e}")
                enriched["insight"] = ""
                enriched["enriched"] = False

            enriched_leads.append(enriched)

        # Add remaining leads without enrichment
        for lead in leads[max_enrich:]:
            enriched = dict(lead)
            enriched["insight"] = ""
            enriched["enriched"] = False
            enriched_leads.append(enriched)

        logger.info(f"Enriched {min(len(leads), max_enrich)} of {len(leads)} leads")
        return {"enriched_leads": enriched_leads}

    def _generate_insight(self, lead: Dict[str, Any], query: str) -> str:
        """Generate business insight for a lead using LLM."""
        prompt = f"""Analyze this business lead and provide a brief insight (2-3 sentences):

Business: {lead.get('name', 'Unknown')}
Website: {lead.get('website', 'N/A')}
Description: {lead.get('description', 'N/A')}
Search Context: {query}

Provide a concise business insight focusing on potential value or relevance."""

        response = self.llm_client.generate(
            prompt=prompt,
            temperature=0.3,
            max_tokens=150,
            metadata={"agent": "LeadEnricherAgent", "lead_id": lead.get("id")},
        )
        return response.strip()


class LeadFormatterAgent(BaseAgent):
    """
    Agent that formats enriched leads into final output structure.
    
    Input: enriched_leads
    Output: formatted_leads, summary
    """

    def __init__(self) -> None:
        """Initialize the lead formatter agent."""
        super().__init__(name="LeadFormatterAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format leads into final output structure.

        Args:
            input_data: Dict with 'enriched_leads'.

        Returns:
            Dict with 'formatted_leads' and 'summary'.
        """
        leads = input_data.get("enriched_leads", [])
        query = input_data.get("query", "")
        location = input_data.get("location", "")

        formatted_leads = [
            {
                "rank": lead.get("id", idx + 1),
                "name": lead.get("name", ""),
                "website": lead.get("website", ""),
                "domain": lead.get("domain", ""),
                "description": lead.get("description", ""),
                "source": lead.get("source", "serper"),
                "location": lead.get("location", ""),
                "insight": lead.get("insight", ""),
                "enriched": lead.get("enriched", False),
            }
            for idx, lead in enumerate(leads)
        ]

        summary = {
            "query": query,
            "location": location,
            "total_leads": len(formatted_leads),
            "enriched_count": sum(1 for l in formatted_leads if l.get("enriched")),
        }

        logger.info(f"Formatted {len(formatted_leads)} leads for output")
        return {"formatted_leads": formatted_leads, "summary": summary}


class BusinessNormalizeAgent(BaseAgent):
    """
    Agent that normalizes raw search results into a clean structure.
    
    Input: raw_search_results
    Output: normalized_businesses (list of cleaned business dicts)
    """

    def __init__(self) -> None:
        """Initialize the business normalize agent."""
        super().__init__(name="BusinessNormalizeAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize raw search results into clean business records.

        Args:
            input_data: Dict with 'raw_search_results' from BusinessSearchAgent.

        Returns:
            Dict with 'normalized_businesses' list of cleaned business data.

        Raises:
            KeyError: If 'raw_search_results' is missing from input_data.
        """
        raw = input_data["raw_search_results"]
        organic = raw.get("organic", [])
        location = input_data.get("location", "")

        normalized: List[Dict[str, Any]] = []

        for item in organic:
            normalized.append({
                "name": item.get("title", ""),
                "website": item.get("link", ""),
                "description": item.get("snippet", ""),
                "source": "serper",
                "location": location,
            })

        logger.info(f"Normalized {len(normalized)} businesses")
        return {"normalized_businesses": normalized}


# Blacklist of aggregator/directory domains to exclude from leads
BLACKLIST_DOMAINS = [
    "zocdoc.com",
    "yelp.com",
    "healthgrades.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "mapquest.com",
    "yellowpages.com",
    "angi.com",
    "thumbtack.com",
]


class WebsiteFilterAgent(BaseAgent):
    """
    Agent that filters out invalid or unwanted links from normalized businesses.
    
    Input: normalized_businesses
    Output: leads (filtered list of businesses with valid website links)
    """

    def __init__(self) -> None:
        """Initialize the website filter agent."""
        super().__init__(name="WebsiteFilterAgent")

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter businesses to include only those with valid website links.

        Args:
            input_data: Dict with 'normalized_businesses' from BusinessNormalizeAgent.

        Returns:
            Dict with 'leads' list of businesses with valid links.

        Raises:
            KeyError: If 'normalized_businesses' is missing from input_data.
        """
        businesses = input_data["normalized_businesses"]

        # Deduplicate by domain and filter out blacklisted/invalid links
        seen: set = set()
        unique: List[Dict[str, Any]] = []
        blacklisted_count = 0

        for lead in businesses:
            website = lead.get("website", "")
            
            # Skip empty or Google Maps links
            if not website or website.startswith("https://maps.google"):
                continue
            
            # Extract domain for filtering
            domain = urlparse(website).netloc.lower()
            
            # Skip blacklisted domains
            if any(bad in domain for bad in BLACKLIST_DOMAINS):
                blacklisted_count += 1
                continue
            
            # Deduplicate by full URL
            if website not in seen:
                seen.add(website)
                unique.append(lead)

        logger.info(
            f"Filtered {len(businesses)} businesses to {len(unique)} unique leads "
            f"(blacklisted: {blacklisted_count}, duplicates: {len(businesses) - len(unique) - blacklisted_count})"
        )
        return {"leads": unique}


class GoogleSheetsExportAgent(BaseAgent):
    """
    Agent that exports leads to a Google Sheets spreadsheet.

    Uses Google Sheets API with service account authentication for batch upload
    of lead data. Configuration is loaded from environment variables with runtime
    overrides supported via input_data.

    Environment Variables:
        GOOGLE_SPREADSHEET_ID: Default spreadsheet ID (can be overridden at runtime)
        GOOGLE_SERVICE_ACCOUNT_PATH: Path to service account JSON (default: credentials/service_account.json)

    Input:
        - leads: List of lead dicts (required)
        - spreadsheet_id: Override for GOOGLE_SPREADSHEET_ID (optional)
        - sheet_name: Worksheet name (optional, defaults to 'Leads')

    Output: export_status with total_leads, success_count, sheet_url, sheet_name
    """

    # Header row for the Google Sheet
    HEADER_ROW: List[str] = ["name", "website", "description", "source", "location"]

    def __init__(self) -> None:
        """
        Initialize the Google Sheets export agent.

        Loads configuration from environment variables:
            - GOOGLE_SERVICE_ACCOUNT_PATH: Path to credentials (default: credentials/service_account.json)
            - GOOGLE_SPREADSHEET_ID: Default spreadsheet ID
        """
        import os
        from dotenv import load_dotenv

        load_dotenv()

        super().__init__(name="GoogleSheetsExportAgent")
        self.credentials_path = os.getenv(
            "GOOGLE_SERVICE_ACCOUNT_PATH", "credentials/service_account.json"
        )
        self.default_spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "")
        self._client: Optional[Any] = None

        logger.info(f"GoogleSheetsExportAgent initialized (credentials: {self.credentials_path})")

    def _get_client(self) -> Any:
        """
        Lazily initialize and return the authenticated gspread client.

        Returns:
            Authenticated gspread client instance.

        Raises:
            FileNotFoundError: If credentials file doesn't exist.
            google.auth.exceptions.GoogleAuthError: If authentication fails.
        """
        if self._client is None:
            # Import here to avoid loading dependencies when agent is not used
            import gspread
            from google.oauth2.service_account import Credentials

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_file(
                self.credentials_path, scopes=scopes
            )
            self._client = gspread.authorize(creds)
            logger.info("Google Sheets client authenticated successfully")

        return self._client

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Export leads to Google Sheets.

        Args:
            input_data: Dict containing:
                - leads: List of lead dicts with name, website, description, source, location
                - spreadsheet_id: Google Sheets spreadsheet ID (optional, falls back to GOOGLE_SPREADSHEET_ID env var)
                - sheet_name: Worksheet name (optional, defaults to 'Leads')

        Returns:
            Dict with 'export_status' containing:
                - total_leads: Number of leads processed
                - success_count: Number of leads successfully exported
                - sheet_url: URL to the Google Sheet
                - sheet_name: Name of the worksheet
                - skipped_duplicates: Number of leads skipped (already existed)

        Raises:
            ValueError: If leads is empty or spreadsheet_id is not provided/configured.
            gspread.exceptions.SpreadsheetNotFound: If spreadsheet doesn't exist.
            gspread.exceptions.WorksheetNotFound: If worksheet doesn't exist.
        """
        from datetime import datetime

        leads = input_data.get("leads", [])
        # Runtime override takes precedence over env var
        spreadsheet_id = input_data.get("spreadsheet_id") or self.default_spreadsheet_id
        
        # Auto-generate sheet name from location + date if not explicitly provided
        location = input_data.get("location", "").strip()
        if input_data.get("sheet_name"):
            sheet_name = input_data["sheet_name"]
        elif location:
            # Sanitize location for sheet name (replace spaces, special chars)
            safe_location = location.replace(" ", "_").replace(",", "")[:20]
            date_str = datetime.now().strftime("%Y-%m-%d")
            sheet_name = f"{safe_location}_{date_str}"
        else:
            sheet_name = f"Leads_{datetime.now().strftime('%Y-%m-%d')}"

        # Validate inputs
        if not leads:
            raise ValueError("No leads to export - 'leads' is empty or missing")
        if not spreadsheet_id:
            raise ValueError(
                "spreadsheet_id not provided. Set GOOGLE_SPREADSHEET_ID in .env "
                "or pass 'spreadsheet_id' in input_data"
            )

        logger.info(f"Exporting {len(leads)} leads to sheet '{sheet_name}'")

        try:
            client = self._get_client()
            spreadsheet = client.open_by_key(spreadsheet_id)

            # Try to get existing worksheet or create new one
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except Exception:
                logger.info(f"Worksheet '{sheet_name}' not found, creating new one")
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name, rows=len(leads) + 10, cols=len(self.HEADER_ROW)
                )

            # Ensure header row exists (check first row, not just if sheet is empty)
            existing_values = worksheet.get_all_values()
            has_header = (
                existing_values 
                and len(existing_values[0]) >= len(self.HEADER_ROW)
                and existing_values[0][0] == self.HEADER_ROW[0]
            )
            if not has_header:
                # Insert header at row 1 if missing
                if existing_values:
                    worksheet.insert_row(self.HEADER_ROW, index=1)
                    logger.info("Header row inserted at top of existing sheet")
                    # Refresh existing_values after insert
                    existing_values = worksheet.get_all_values()
                else:
                    worksheet.append_row(self.HEADER_ROW)
                    logger.debug("Header row written to empty sheet")
                    existing_values = [self.HEADER_ROW]

            # Build set of existing websites for deduplication (idempotency)
            # Website is in column 2 (index 1)
            existing_websites: set = set()
            for row in existing_values[1:]:  # Skip header row
                if len(row) > 1 and row[1]:
                    existing_websites.add(row[1].strip().lower())

            # Filter out duplicates - only add leads not already in sheet
            new_leads = []
            skipped_count = 0
            for lead in leads:
                website = lead.get("website", "").strip().lower()
                if website and website not in existing_websites:
                    new_leads.append(lead)
                    existing_websites.add(website)  # Prevent duplicates within batch
                else:
                    skipped_count += 1

            if skipped_count > 0:
                logger.info(f"Skipped {skipped_count} duplicate leads (already in sheet)")

            # Prepare batch data for efficient upload
            if new_leads:
                rows = [
                    [
                        lead.get("name", ""),
                        lead.get("website", ""),
                        lead.get("description", ""),
                        lead.get("source", ""),
                        lead.get("location", ""),
                    ]
                    for lead in new_leads
                ]

                # Batch append all rows at once for performance
                worksheet.append_rows(rows, value_input_option="USER_ENTERED")
                logger.info(f"Successfully exported {len(rows)} new leads to Google Sheets")
            else:
                logger.info("No new leads to export (all duplicates)")

            export_status = {
                "total_leads": len(leads),
                "success_count": len(new_leads),
                "skipped_duplicates": skipped_count,
                "sheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit",
                "sheet_name": sheet_name,
            }

            return {"export_status": export_status}

        except Exception as e:
            logger.error(f"Failed to export leads to Google Sheets: {e}")
            raise
