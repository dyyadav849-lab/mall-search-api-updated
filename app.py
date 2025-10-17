"""Application entrypoint for running the Mall Search API."""
from __future__ import annotations

from mall_search_api import create_app

app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8007)
