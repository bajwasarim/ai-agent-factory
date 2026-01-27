"""Core pipeline components - shared across all pipelines."""

from pipelines.core.base_agent import BaseAgent
from pipelines.core.runner import PipelineRunner

__all__ = ["BaseAgent", "PipelineRunner"]
