"""Configuration constants for Maps No-Website Pipeline."""

import os
from pathlib import Path

# Pipeline identification
PIPELINE_NAME = "MAPS_NO_WEBSITE_PIPELINE"

# Export configuration
EXPORT_PATH = Path("exports/maps_no_website/")

# Ensure export directory exists
EXPORT_PATH.mkdir(parents=True, exist_ok=True)

# Google Sheets configuration
# Set via environment or pass in pipeline context
DEFAULT_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")
GOOGLE_CREDENTIALS_PATH = Path(
    os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials/service_account.json")
)

# Normalized lead contract for this pipeline
LEAD_SCHEMA = {
    "name": str,
    "website": str,  # May be empty for no-website leads
    "description": str,
    "source": str,
    "location": str,
}

# Source identifier for Maps leads
SOURCE_IDENTIFIER = "google_maps"
