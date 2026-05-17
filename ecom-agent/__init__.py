"""
Run with `adk web` for a chat UI, or `adk run` for terminal.
For Cloud Run deployment, ADK provides a built-in FastAPI server:
    adk api_server --host 0.0.0.0 --port 8080
"""
from agent import root_agent  # noqa: F401
