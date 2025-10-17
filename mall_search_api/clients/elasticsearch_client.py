"""Elasticsearch client singleton for the Mall Search API."""
from __future__ import annotations

from elasticsearch import Elasticsearch

from mall_search_api.config import ELASTICSEARCH_URL

es_client = Elasticsearch(ELASTICSEARCH_URL)

__all__ = ["es_client"]
