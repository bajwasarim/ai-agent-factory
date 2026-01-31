from core.agent_base import BaseAgent


class EchoAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="EchoAgent")

    def run(self, input_data: dict) -> dict:
        message = input_data.get("message", "")
        return {"echo": f"Echo: {message}"}
