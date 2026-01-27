"""YAML configuration loader utility."""

from pathlib import Path
from typing import Any

import yaml


def load_config(path: str | Path) -> dict[str, Any]:
    """
    Load a YAML configuration file.

    Args:
        path: Path to the YAML file.

    Returns:
        Dictionary containing the parsed configuration.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        yaml.YAMLError: If the file contains invalid YAML.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config if config is not None else {}


def load_project_config(project_name: str, base_path: str | Path = "projects") -> dict[str, Any]:
    """
    Load a project's configuration file.

    Args:
        project_name: Name of the project folder.
        base_path: Base path where projects are stored.

    Returns:
        Dictionary containing the project configuration.
    """
    config_path = Path(base_path) / project_name / "project.yaml"
    return load_config(config_path)
