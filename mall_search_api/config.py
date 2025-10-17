"""Application configuration constants and helpers."""
from __future__ import annotations

import json
import logging
from pathlib import Path

# =================== CONFIGS ===========================

PRODUCT_INDEX_NAME = "bajajmall_products_s3_esidx3_30092025"
CATEGORY_INDEX_NAME = "bajajmall_categories_s3_esidx3_30092025"
AUTOSUGGEST_INDEX_NAME = "bajajmall_autosuggest_s3_esidx3_30092025"
IMAGE_DOMAIN = "https://mc.bajajfinserv.in/media/catalog/product"
ELASTICSEARCH_URL = "http://localhost:9200"
ATTRIBUTE_ID_NAME_MAP_PATH = Path(
    "/datadrive1/deepak/main_pipeline/bajajmall_new/running_pipeline/new_prod_data_pipline/"
    "test_clean_api/s3_data_pipeline/data_pipeline/old_data/attribute_id_name_mapping_esidx.json"
)


def load_attribute_id_name_map() -> dict:
    """Load attribute-id/name mapping used across the search service."""
    with ATTRIBUTE_ID_NAME_MAP_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def configure_logging() -> None:
    """Configure application logging."""
    logging.basicConfig(
        filename="api.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


ATTRIBUTE_ID_NAME_MAP = load_attribute_id_name_map()
