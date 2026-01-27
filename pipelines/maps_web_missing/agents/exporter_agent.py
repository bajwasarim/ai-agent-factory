"""Export agent for Maps No-Website Pipeline."""

import json
import csv
import uuid
from pathlib import Path
from typing import Any, Dict, List

from pipelines.core.base_agent import BaseAgent
from pipelines.maps_web_missing.config import EXPORT_PATH
from pipelines.maps_web_missing.utils.helpers import (
    ensure_export_dir,
    sanitize_filename,
    get_timestamp,
)
from core.logger import get_logger

logger = get_logger(__name__)


class ExporterAgent(BaseAgent):
    """
    Agent that exports formatted leads to files.

    Supports JSON and CSV export formats. Creates timestamped
    files in the configured export directory.

    Input: formatted_leads, summary, query, location
    Output: export_status with file paths and counts
    """

    def __init__(self, export_path: Path = EXPORT_PATH) -> None:
        """
        Initialize the exporter agent.

        Args:
            export_path: Directory path for exports.
        """
        super().__init__(name="ExporterAgent")
        self.export_path = export_path

    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Export formatted leads to JSON and CSV files.

        Args:
            input_data: Dict with 'formatted_leads' and metadata.

        Returns:
            Dict with 'export_status' containing file paths and counts.
        """
        leads = input_data.get("formatted_leads", [])
        summary = input_data.get("summary", {})
        query = input_data.get("query", "export")
        location = input_data.get("location", "")

        # Ensure export directory exists
        ensure_export_dir(self.export_path)

        # Generate filename base with UUID for collision safety
        timestamp = get_timestamp()
        run_id = uuid.uuid4().hex[:8]
        location_safe = sanitize_filename(location) if location else "unknown"
        query_safe = sanitize_filename(query)
        filename_base = f"{query_safe}_{location_safe}_{timestamp}_{run_id}"

        # Export to JSON
        json_path = self.export_path / f"{filename_base}.json"
        self._export_json(leads, summary, json_path)

        # Export to CSV
        csv_path = self.export_path / f"{filename_base}.csv"
        self._export_csv(leads, csv_path)

        export_status = {
            "total_exported": len(leads),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
            "export_directory": str(self.export_path),
        }

        logger.info(f"Export created successfully: {len(leads)} leads")
        logger.info(f"  JSON: {json_path}")
        logger.info(f"  CSV: {csv_path}")

        return {"export_status": export_status}

    def _export_json(
        self,
        leads: List[Dict[str, Any]],
        summary: Dict[str, Any],
        path: Path,
    ) -> None:
        """Export leads to JSON file."""
        output = {
            "summary": summary,
            "leads": leads,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

    def _export_csv(self, leads: List[Dict[str, Any]], path: Path) -> None:
        """Export leads to CSV file."""
        if not leads:
            # Create empty CSV with headers
            headers = [
                "rank", "name", "website", "description", "source",
                "location", "phone", "rating", "reviews", "address", "has_website"
            ]
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            return

        # Use keys from first lead as headers
        headers = list(leads[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(leads)
