from pipelines.runner import PipelineRunner
from projects.test_project.agents import EchoAgent


def build_pipeline():
    return PipelineRunner(
        agents=[EchoAgent()]
    )
