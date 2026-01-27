"""Helper utilities for Maps No-Website Pipeline."""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional


def ensure_export_dir(path: Path) -> Path:
    """
    Ensure export directory exists, creating it if necessary.

    Args:
        path: Path to the export directory.

    Returns:
        The path object (for chaining).
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def sanitize_filename(name: str, max_length: int = 50) -> str:
    """
    Sanitize a string for use as a filename.

    Args:
        name: Raw string to sanitize.
        max_length: Maximum length of output string.

    Returns:
        Sanitized filename-safe string.
    """
    # Replace spaces and special characters
    sanitized = re.sub(r'[^\w\-]', '_', name)
    # Remove consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Strip leading/trailing underscores
    sanitized = sanitized.strip('_')
    return sanitized[:max_length]


def get_timestamp(format_str: Optional[str] = None) -> str:
    """
    Get current timestamp as formatted string.

    Args:
        format_str: Optional strftime format string.
            Defaults to '%Y-%m-%d_%H%M%S'.

    Returns:
        Formatted timestamp string.
    """
    fmt = format_str or '%Y-%m-%d_%H%M%S'
    return datetime.now().strftime(fmt)


# =============================================================================
# Phone Number Formatting for Google Sheets
# =============================================================================

def format_phone_for_sheets(phone: Optional[str]) -> str:
    """
    Format phone number to prevent Google Sheets formula interpretation.

    Google Sheets auto-interprets phone numbers like '+1-212-555-0198' as
    mathematical expressions. This function prepends a single leading
    apostrophe to force text interpretation.

    Args:
        phone: Raw phone number string (may be None, empty, or formatted).

    Returns:
        Sheet-safe phone string with leading apostrophe if needed.
        Empty/null values returned unchanged.

    Examples:
        >>> format_phone_for_sheets("+1-212-555-0198")
        "'+1-212-555-0198"
        >>> format_phone_for_sheets("555-0198")
        "'555-0198"
        >>> format_phone_for_sheets("'already-prefixed")
        "'already-prefixed"
        >>> format_phone_for_sheets("")
        ""
        >>> format_phone_for_sheets(None)
        ""
    """
    if not phone:
        return ""

    phone_str = str(phone).strip()
    if not phone_str:
        return ""

    # Don't duplicate if already apostrophe-prefixed
    if phone_str.startswith("'"):
        return phone_str

    # Detect if this looks like a phone number that needs protection:
    # - Starts with + (international format)
    # - Contains digits with dashes/spaces/parentheses (standard phone formats)
    # - Is primarily numeric with formatting characters
    phone_chars = set(phone_str)
    numeric_chars = set('0123456789')
    formatting_chars = set('+-() .')

    # Check if it's a phone-like string
    is_phone_like = (
        phone_str.startswith('+') or
        phone_str.startswith('(') or
        (phone_chars - numeric_chars - formatting_chars == set() and
         any(c in phone_str for c in numeric_chars))
    )

    if is_phone_like:
        return f"'{phone_str}"

    return phone_str


# =============================================================================
# Deduplication Key Generation
# =============================================================================

def normalize_for_dedup(value: Optional[str]) -> str:
    """
    Normalize a string value for deduplication hashing.

    Applies consistent normalization:
    - Lowercase
    - Strip whitespace
    - Remove punctuation (except spaces for readability)

    Args:
        value: Raw string value.

    Returns:
        Normalized string for hashing.
    """
    if not value:
        return ""

    # Lowercase and strip
    normalized = str(value).lower().strip()

    # Remove punctuation but keep alphanumeric and spaces
    normalized = re.sub(r'[^\w\s]', '', normalized)

    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)

    return normalized.strip()


def extract_phone_digits(phone: Optional[str]) -> str:
    """
    Extract only digits from a phone number for deduplication.

    Args:
        phone: Raw phone string.

    Returns:
        Digits-only string.

    Examples:
        >>> extract_phone_digits("+1-212-555-0198")
        "12125550198"
        >>> extract_phone_digits("(555) 123-4567")
        "5551234567"
    """
    if not phone:
        return ""
    return re.sub(r'\D', '', str(phone))


def compute_dedup_key(
    place_id: Optional[str] = None,
    name: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[str] = None,
) -> str:
    """
    Compute a deterministic deduplication key for a business record.

    Priority order:
    1. place_id (if present) - most reliable unique identifier
    2. Hash of normalized (name + phone_digits + address)

    Args:
        place_id: Google Maps place_id/cid (preferred primary key).
        name: Business name.
        phone: Phone number (any format).
        address: Street address.

    Returns:
        Deterministic dedup key string.
        Format: "pid:<place_id>" or "hash:<sha256_hex>"

    Examples:
        >>> compute_dedup_key(place_id="ChIJ...")
        "pid:ChIJ..."
        >>> compute_dedup_key(name="Acme Corp", phone="+1-555-0100", address="123 Main St")
        "hash:abc123..."  # SHA256 of normalized values
    """
    import hashlib

    # Primary: use place_id if available
    if place_id:
        pid = str(place_id).strip()
        if pid:
            return f"pid:{pid}"

    # Secondary: hash of normalized fields
    norm_name = normalize_for_dedup(name)
    phone_digits = extract_phone_digits(phone)
    norm_address = normalize_for_dedup(address)

    # Combine for hashing
    combined = f"{norm_name}|{phone_digits}|{norm_address}"
    hash_hex = hashlib.sha256(combined.encode('utf-8')).hexdigest()

    return f"hash:{hash_hex}"
