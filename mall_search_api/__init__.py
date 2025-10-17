"""Mall Search API package initialization."""
from __future__ import annotations

from flask import Flask

from mall_search_api.config import configure_logging
from mall_search_api.routes.search import bp as search_blueprint


def create_app() -> Flask:
    """Application factory for creating Flask app instances."""
    configure_logging()
    app = Flask(__name__)
    app.register_blueprint(search_blueprint)
    return app


__all__ = ["create_app"]
