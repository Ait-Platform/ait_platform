"""Explicit Flask operator factory: no unrelated startup database mutations.

Use: python -m flask --app scripts.home_publication_app:create_operator_app
Then use the existing home_sace_bp controlled-publication commands.
This file never selects a production connection or publishes on import.
"""
from app import create_app


def create_operator_app():
    return create_app(operator_mode=True)
