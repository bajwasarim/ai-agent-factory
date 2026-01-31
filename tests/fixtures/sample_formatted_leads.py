"""
Sample formatted leads fixtures for pipeline testing.

Provides reusable mock data generators for testing:
- LeadRouterAgent ordering
- LeadFormatterAgent contract preservation
- GoogleSheetsExportAgent fan-out export

All fixtures include valid dedup_key and target_sheet fields
to satisfy pipeline contract requirements.
"""

from typing import Any, Dict, List, Optional


def generate_mock_leads(
    target_count: int = 3,
    excluded_count: int = 2,
    retry_count: int = 1,
    include_dedup_key: bool = True,
    include_target_sheet: bool = True,
) -> List[Dict[str, Any]]:
    """
    Generate mock formatted leads for testing.

    Creates leads with valid contract fields (dedup_key, lead_route, target_sheet)
    in the expected routed order: TARGET → EXCLUDED → RETRY.

    Args:
        target_count: Number of TARGET leads to generate.
        excluded_count: Number of EXCLUDED leads to generate.
        retry_count: Number of RETRY leads to generate.
        include_dedup_key: Whether to include dedup_key field (False for contract violation tests).
        include_target_sheet: Whether to include target_sheet field (False for contract violation tests).

    Returns:
        List of formatted lead dicts in routed order (TARGET → EXCLUDED → RETRY).
    """
    leads: List[Dict[str, Any]] = []

    # Generate TARGET leads (NO_WEBSITE_TARGETS)
    for i in range(target_count):
        lead = _create_lead(
            index=i,
            route="TARGET",
            sheet="NO_WEBSITE_TARGETS",
            has_real_website=False,
            website_status="missing",
            include_dedup_key=include_dedup_key,
            include_target_sheet=include_target_sheet,
        )
        leads.append(lead)

    # Generate EXCLUDED leads (HAS_WEBSITE_EXCLUDED)
    for i in range(excluded_count):
        lead = _create_lead(
            index=target_count + i,
            route="EXCLUDED",
            sheet="HAS_WEBSITE_EXCLUDED",
            has_real_website=True,
            website_status="valid",
            website="https://example.com",
            include_dedup_key=include_dedup_key,
            include_target_sheet=include_target_sheet,
        )
        leads.append(lead)

    # Generate RETRY leads (WEBSITE_CHECK_ERRORS)
    for i in range(retry_count):
        lead = _create_lead(
            index=target_count + excluded_count + i,
            route="RETRY",
            sheet="WEBSITE_CHECK_ERRORS",
            has_real_website=False,
            website_status="error",
            include_dedup_key=include_dedup_key,
            include_target_sheet=include_target_sheet,
        )
        leads.append(lead)

    return leads


def generate_leads_for_sheet(
    sheet_name: str,
    count: int,
    start_index: int = 0,
) -> List[Dict[str, Any]]:
    """
    Generate mock leads for a specific target sheet.

    Args:
        sheet_name: Target sheet name (NO_WEBSITE_TARGETS, HAS_WEBSITE_EXCLUDED, WEBSITE_CHECK_ERRORS).
        count: Number of leads to generate.
        start_index: Starting index for unique IDs.

    Returns:
        List of formatted lead dicts for the specified sheet.
    """
    sheet_config = {
        "NO_WEBSITE_TARGETS": {
            "route": "TARGET",
            "has_real_website": False,
            "website_status": "missing",
            "website": "",
        },
        "HAS_WEBSITE_EXCLUDED": {
            "route": "EXCLUDED",
            "has_real_website": True,
            "website_status": "valid",
            "website": "https://example.com",
        },
        "WEBSITE_CHECK_ERRORS": {
            "route": "RETRY",
            "has_real_website": False,
            "website_status": "error",
            "website": "",
        },
    }

    config = sheet_config.get(sheet_name, sheet_config["WEBSITE_CHECK_ERRORS"])

    leads = []
    for i in range(count):
        lead = _create_lead(
            index=start_index + i,
            route=config["route"],
            sheet=sheet_name,
            has_real_website=config["has_real_website"],
            website_status=config["website_status"],
            website=config.get("website", ""),
        )
        leads.append(lead)

    return leads


def generate_validated_businesses(
    target_count: int = 2,
    excluded_count: int = 2,
    retry_count: int = 1,
) -> List[Dict[str, Any]]:
    """
    Generate mock validated_businesses for LeadRouterAgent input.

    These are pre-routing leads with website validation fields but
    WITHOUT lead_route and target_sheet (those are added by router).

    Args:
        target_count: Count that should route to TARGET.
        excluded_count: Count that should route to EXCLUDED.
        retry_count: Count that should route to RETRY.

    Returns:
        List of validated business dicts (pre-routing).
    """
    businesses: List[Dict[str, Any]] = []

    # TARGET candidates (no real website, missing status)
    for i in range(target_count):
        businesses.append({
            "name": f"Target Business {i}",
            "phone": f"555-{100 + i:04d}",
            "address": f"{100 + i} Target St",
            "place_id": f"place_target_{i}",
            "dedup_key": f"pid:place_target_{i}",
            "website": "",
            "has_real_website": False,
            "website_status": "missing",
            "website_checked_at": "2026-01-29T12:00:00Z",
        })

    # EXCLUDED candidates (has real website, valid status)
    for i in range(excluded_count):
        businesses.append({
            "name": f"Excluded Business {i}",
            "phone": f"555-{200 + i:04d}",
            "address": f"{200 + i} Excluded St",
            "place_id": f"place_excluded_{i}",
            "dedup_key": f"pid:place_excluded_{i}",
            "website": "https://real-website.com",
            "has_real_website": True,
            "website_status": "valid",
            "website_checked_at": "2026-01-29T12:00:00Z",
        })

    # RETRY candidates (error status)
    for i in range(retry_count):
        businesses.append({
            "name": f"Retry Business {i}",
            "phone": f"555-{300 + i:04d}",
            "address": f"{300 + i} Retry St",
            "place_id": f"place_retry_{i}",
            "dedup_key": f"pid:place_retry_{i}",
            "website": "https://broken-link.com",
            "has_real_website": False,
            "website_status": "error",
            "website_checked_at": "2026-01-29T12:00:00Z",
        })

    return businesses


def _create_lead(
    index: int,
    route: str,
    sheet: str,
    has_real_website: bool,
    website_status: str,
    website: str = "",
    include_dedup_key: bool = True,
    include_target_sheet: bool = True,
) -> Dict[str, Any]:
    """
    Create a single mock lead with all required fields.

    Args:
        index: Unique index for generating IDs.
        route: Lead route (TARGET, EXCLUDED, RETRY).
        sheet: Target sheet name.
        has_real_website: Website validation result.
        website_status: Website status (missing, invalid, valid, error).
        website: Website URL (empty for no website).
        include_dedup_key: Whether to include dedup_key.
        include_target_sheet: Whether to include target_sheet.

    Returns:
        Dict representing a formatted lead.
    """
    lead: Dict[str, Any] = {
        "rank": index + 1,
        "name": f"Test Business {index}",
        "website": website,
        "description": f"Test description for business {index}",
        "source": "mock",
        "location": "Test City, ST",
        "phone": f"555-{index:04d}",
        "rating": "4.5",
        "reviews": "100",
        "address": f"{index} Test Street",
        "place_id": f"place_{index}",
        "has_website": bool(website),
        "has_real_website": has_real_website,
        "website_status": website_status,
        "website_checked_at": "2026-01-29T12:00:00Z",
        "lead_route": route,
    }

    if include_dedup_key:
        lead["dedup_key"] = f"pid:place_{index}"

    if include_target_sheet:
        lead["target_sheet"] = sheet

    return lead
