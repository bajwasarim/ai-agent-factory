"""
Test script for Google Sheets Export Agent.

This script demonstrates the full lead generation pipeline
with Google Sheets export functionality.

Prerequisites:
1. A Google Cloud project with Sheets API enabled
2. A service account with credentials saved to credentials/service_account.json
3. A Google Sheet shared with the service account email
4. The spreadsheet ID set in the SPREADSHEET_ID variable below

Usage:
    python tests/test_sheets_export.py
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logger import get_logger
from projects.business_leadgen.agents import (
    BusinessSearchAgent,
    BusinessNormalizeAgent,
    WebsiteFilterAgent,
    GoogleSheetsExportAgent,
)

logger = get_logger(__name__)


def test_sheets_export():
    """Test the Google Sheets export agent with sample leads."""
    
    # Configure your spreadsheet ID here
    # Get this from the URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit
    SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "YOUR_SPREADSHEET_ID_HERE")
    
    if SPREADSHEET_ID == "YOUR_SPREADSHEET_ID_HERE":
        logger.warning(
            "Please set GOOGLE_SPREADSHEET_ID environment variable or update the script"
        )
        return
    
    # Sample leads data (simulating output from WebsiteFilterAgent)
    sample_leads = [
        {
            "name": "Test Dental Practice",
            "website": "https://testdental.com",
            "description": "Family dental care services",
            "source": "serper",
            "location": "New York",
        },
        {
            "name": "Sample Orthodontics",
            "website": "https://sampleortho.com",
            "description": "Braces and aligners specialist",
            "source": "serper",
            "location": "New York",
        },
    ]
    
    # Initialize and run the export agent
    export_agent = GoogleSheetsExportAgent()
    
    input_data = {
        "leads": sample_leads,
        "spreadsheet_id": SPREADSHEET_ID,
        "sheet_name": "TestLeads",
    }
    
    try:
        result = export_agent.run(input_data)
        logger.info(f"Export status: {result['export_status']}")
        print("\n✅ Export successful!")
        print(f"   Total leads: {result['export_status']['total_leads']}")
        print(f"   Success count: {result['export_status']['success_count']}")
        print(f"   Sheet URL: {result['export_status']['sheet_url']}")
    except Exception as e:
        logger.error(f"Export failed: {e}")
        print(f"\n❌ Export failed: {e}")
        raise


def test_full_pipeline_with_export():
    """Test the complete pipeline from search to Google Sheets export."""
    
    SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")
    
    if not SPREADSHEET_ID:
        logger.warning("GOOGLE_SPREADSHEET_ID not set, skipping full pipeline test")
        return
    
    # Import and register the Serper tool
    import core.tools.serper_tool  # noqa: F401
    
    # Initialize agents
    search_agent = BusinessSearchAgent()
    normalize_agent = BusinessNormalizeAgent()
    filter_agent = WebsiteFilterAgent()
    export_agent = GoogleSheetsExportAgent()
    
    # Run pipeline
    context = {
        "query": "dentist",
        "location": "New York",
        "num_results": 5,
        "spreadsheet_id": SPREADSHEET_ID,
        "sheet_name": "PipelineTest",
    }
    
    print("\n🔍 Running full pipeline with Google Sheets export...\n")
    
    # Step 1: Search
    context.update(search_agent.run(context))
    print(f"   ✓ Search: Found {len(context.get('raw_search_results', {}).get('organic', []))} results")
    
    # Step 2: Normalize
    context.update(normalize_agent.run(context))
    print(f"   ✓ Normalize: {len(context.get('normalized_businesses', []))} businesses")
    
    # Step 3: Filter
    context.update(filter_agent.run(context))
    print(f"   ✓ Filter: {len(context.get('leads', []))} leads")
    
    # Step 4: Export to Google Sheets
    context.update(export_agent.run(context))
    print(f"   ✓ Export: {context['export_status']['success_count']} leads exported")
    print(f"\n📊 View results at: {context['export_status']['sheet_url']}")


if __name__ == "__main__":
    print("=" * 60)
    print("Google Sheets Export Agent Test")
    print("=" * 60)
    
    # Test with sample data
    test_sheets_export()
    
    # Optionally test full pipeline
    # test_full_pipeline_with_export()
