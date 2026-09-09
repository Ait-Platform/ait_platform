"""Compatibility import for the UIP adapter to the shared AIT AI Gateway."""
from app.uip.services.ai import run

class LunaGateway:
    ask_luna = staticmethod(run)
