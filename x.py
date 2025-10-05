from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import traceback
import json
import logging
import re
from rapidfuzz import process as fuzzproc, fuzz
from rapidfuzz import fuzz, process as rapidfuzz_process
from collections import defaultdict
import time
from functools import lru_cache
from utils import (
    CATEGORY_CANONICAL,
    TWOWHEELER_AUTOSUGGEST,
    BUSINESS_SYNONYMS,
    CATEGORY_HARDCODED_CHIPS,
    APPLE_TERMS,
    FILTER_ATTRIBUTE_EXCLUSIONS,
    SCOOTER_SYNONYMS,
    BUSINESS_AUTOSUGGEST,
    ALL_ATTRIBUTE_FILTERS,
    ALL_ATTRIBUTE_FILTERS_Name
)

# =================== CONFIGS ===========================

# PRODUCT_INDEX_NAME = "bajajmall_products_s3_esidx3_lat"
# CATEGORY_INDEX_NAME = "bajajmall_categories_s3_esidx3_lat"
# AUTOSUGGEST_INDEX_NAME = "bajajmall_autosuggest_s3_esidx3_lat"


PRODUCT_INDEX_NAME = "bajajmall_products_s3_esidx3_30092025"
CATEGORY_INDEX_NAME = "bajajmall_categories_s3_esidx3_30092025"
AUTOSUGGEST_INDEX_NAME = "bajajmall_autosuggest_s3_esidx3_30092025"
# BRAND_INDEX = "bajajmall_brands_s3_esidx3_30092025"  # NEW

# PRODUCT_INDEX_NAME = "bajajmall_products_s3_esidx3"   
# CATEGORY_INDEX_NAME = "bajajmall_categories_s3_esidx3"
# AUTOSUGGEST_INDEX_NAME = "bajajmall_autosuggest_s3_esidx3"
IMAGE_DOMAIN = "https://mc.bajajfinserv.in/media/catalog/product"

logging.basicConfig(filename="api.log", level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = Flask(__name__)
es = Elasticsearch("http://localhost:9200")

with open('/datadrive1/deepak/main_pipeline/bajajmall_new/running_pipeline/new_prod_data_pipline/test_clean_api/s3_data_pipeline/data_pipeline/old_data/attribute_id_name_mapping_esidx.json', 'r', encoding='utf-8') as f:
    ATTRIBUTE_ID_NAME_MAP = json.load(f)
    
# Define BRAND_NAMES list for brand protection in query correction
BRAND_NAMES = [
    "samsung", "apple", "oppo", "vivo", "realme", "redmi", "mi", "oneplus", "xiaomi", "nothing", "motorola", 
    "iqoo", "infinix", "tecno", "nokia", "lenovo", "hp", "dell", "acer", "asus", "lg", "panasonic", "sony",
    "honda", "hero", "tvs", "bajaj", "royal enfield", "hyundai", "maruti", "tata", "mahindra", "kia", "toyota",
    "ifb", "bosch", "whirlpool", "godrej"  # Added washing machine brands
]

def get_attribute(prod, keys):
    for key in keys:
        value = prod.get(key, "")
        if value:
            if isinstance(value, dict):
                value = value.get("name", "") or value.get("value", "") or str(value)
            elif isinstance(value, list) and value:
                if isinstance(value[0], dict):
                    value = value[0].get("name", "") or value[0].get("value", "") or str(value[0])
                else:
                    value = value[0]
            return str(value).lower()
    
    # Fallback to nested products
    if "products" in prod and prod["products"]:
        for sku in prod["products"]:
            out = get_attribute(sku, keys)
            if out:
                return out
    return ""

# =================== CITY-LEVEL HELPER FUNCTIONS ===================
def get_city_offer(hit, requested_city_id):
    """
    Extract city-specific offer from product data using inner_hits.
    First tries to find offer for requested_city_id, then falls back to citi_id_0.
    """
    city_offer = None
    
    # Check if we have inner_hits for city_offers
    if "inner_hits" in hit and "city_offers" in hit["inner_hits"]:
        city_hits = hit["inner_hits"]["city_offers"]["hits"]["hits"]
        # Look for the requested city first
        for city_hit in city_hits:
            city_source = city_hit["_source"]
            if city_source.get("cityid") == requested_city_id:
                city_offer = city_source
                break
        
        # If not found, try to find the default city
        if not city_offer:
            for city_hit in city_hits:
                city_source = city_hit["_source"]
                if city_source.get("cityid") == "citi_id_0":
                    city_offer = city_source
                    break
    
    # Fallback: Check directly in the source
    source = hit["_source"]
    if not city_offer and "city_offers" in source:
        for offer in source["city_offers"]:
            if offer.get("cityid") == requested_city_id:
                city_offer = offer
                break
        # If not found, try default city
        if not city_offer:
            for offer in source["city_offers"]:
                if offer.get("cityid") == "citi_id_0":
                    city_offer = offer
                    break
    
    return city_offer

def build_city_filter(city_id):
    """
    Builds a nested filter for city-specific offers.
    Ensures products are available in requested city or default city.
    """
    if not city_id:
        return None
    
    return {
        "nested": {
            "path": "city_offers",
            "query": {
                "bool": {
                    "should": [
                        {"term": {"city_offers.cityid": city_id}},
                        {"term": {"city_offers.cityid": "citi_id_0"}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "inner_hits": {
                "size": 2,  # Get both requested and default if available
            }
        }
    }
    
def build_emi_aggregation(city_id):
    """
    Builds a nested aggregation for EMI range calculation.
    Filters by requested city or default city.
    """
    return {
        "city_offers": {
            "nested": {
                "path": "city_offers"
            },
            "aggs": {
                "filtered_offers": {
                    "filter": {
                        "bool": {
                            "should": [
                                {"term": {"city_offers.cityid": city_id}},
                                {"term": {"city_offers.cityid": "citi_id_0"}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    "aggs": {
                        "min_emi": {"min": {"field": "city_offers.lowest_emi"}},
                        "max_emi": {"max": {"field": "city_offers.lowest_emi"}}
                    }
                }
            }
        }
    }

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
def is_apple_only_query(query):
    query_n = normalize(query)
    tokens = [t for t in re.split(r'[\s,]+', query_n) if t]
    NON_APPLE_NOISE = set(["kit", "and", "pro", "air", "mini", "plus", "blynk"])
    return all(
        t in APPLE_TERMS or t in NON_APPLE_NOISE for t in tokens
    ) and any(t in APPLE_TERMS for t in tokens)

# Map all categories to lowercase for lookup
CATEGORY_CANONICAL_LOWER = {k.lower(): v for k, v in CATEGORY_CANONICAL.items()}
CATEGORY_CANONICAL_LOOKUP = {v.lower(): v for k, v in CATEGORY_CANONICAL.items()}

# Add synonym support:
ALL_SYNONYM_TO_CANONICAL = {}
for canon, syns in BUSINESS_SYNONYMS.items():
    for s in syns:
        ALL_SYNONYM_TO_CANONICAL[s.lower()] = canon.lower()
for k in CATEGORY_CANONICAL.keys():
    ALL_SYNONYM_TO_CANONICAL[k.lower()] = k.lower()
    ALL_SYNONYM_TO_CANONICAL[CATEGORY_CANONICAL[k].lower()] = k.lower()

# ============= NORMALIZATION/HELPERS ==============
def normalize(text):
    if not text: return ""
    t = re.sub(r"[^a-zA-Z0-9\s&-]", "", text.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t

def build_correction_pool():
    pool = set()
    for k, v in CATEGORY_CANONICAL.items():
        pool.add(normalize(k))
        pool.add(normalize(v))
        pool.add(normalize(k.replace("-", "")))
        pool.add(normalize(k.replace(" ", "")))
    for syns in BUSINESS_SYNONYMS.values():
        for s in syns:
            pool.add(normalize(s))
            pool.add(normalize(s.replace("-", "")))
            pool.add(normalize(s.replace(" ", "")))
    
    # Add brand names to prevent over-correction
    for brand in BRAND_NAMES:
        pool.add(normalize(brand))
    
    return sorted({p for p in pool if p})

# Update the correction pool with this new function
CORRECTION_POOL = build_correction_pool()

def correct_query(user_query):
    user_query_n = normalize(user_query)
    words = user_query_n.split()
    
    # First, check if the query contains a brand name
    detected_brands = []
    remaining_words = []
    
    for word in words:
        brand_match = None
        # Check for exact brand match first
        for brand in BRAND_NAMES:
            if brand.lower() == word.lower():
                brand_match = brand
                break
        
        if brand_match:
            detected_brands.append(brand_match)
        else:
            remaining_words.append(word)
    
    # Correct only the non-brand parts of the query
    if remaining_words:
        non_brand_query = " ".join(remaining_words)
        match, score, _ = rapidfuzz_process.extractOne(
            non_brand_query, CORRECTION_POOL, scorer=fuzz.ratio, score_cutoff=78
        ) or (None, 0, None)
        
        if match and score >= 78:
            corrected_non_brand = match
        else:
            new_words = []
            for w in remaining_words:
                m, sc, _ = rapidfuzz_process.extractOne(
                    w, CORRECTION_POOL, scorer=fuzz.ratio, score_cutoff=75
                ) or (w, 0, None)
                new_words.append(m if sc >= 75 else w)
            corrected_non_brand = " ".join(new_words)
    else:
        corrected_non_brand = ""
    
    # Reconstruct the query with brands preserved
    final_query_parts = []
    if detected_brands:
        final_query_parts.extend(detected_brands)
    if corrected_non_brand:
        final_query_parts.append(corrected_non_brand)
    
    final_query = " ".join(final_query_parts)
    
    # Only return the corrected query if it's different from the original
    if final_query != user_query_n:
        return final_query
    return user_query_n

def parse_price_from_query(query):
    q = query.lower()
    cleaned_query = q
    filters = {}
    
    # Enhanced EMI patterns with more variations
    emi_patterns = [
        (r'(lowest\s*)?(emi|installment|monthly|per\s*month)[^\d]{0,10}(under|below|less\s*than|upto|<=)\s*([0-9]{3,8})',
         lambda m: ('lowest_emi', {'lte': int(m.group(4))})),
        (r'(lowest\s*)?(emi|installment|monthly|per\s*month)[^\d]{0,10}(above|over|greater\s*than|more\s*than|>=)\s*([0-9]{3,8})',
         lambda m: ('lowest_emi', {'gte': int(m.group(4))})),
        (r'(lowest\s*)?(emi|installment|monthly|per\s*month)[^\d]{0,10}(from|between|range)?\s*([0-9]{3,8})\s*(to|and|-)\s*([0-9]{3,8})',
         lambda m: ('lowest_emi', {'gte': min(int(m.group(4)), int(m.group(6))), 'lte': max(int(m.group(4)), int(m.group(6)))})),
        # New patterns for EMI-focused queries
        (r'emi\s*(under|below|less\s*than|upto|<=)\s*([0-9]{3,8})',
         lambda m: ('lowest_emi', {'lte': int(m.group(2))})),
        (r'emi\s*(above|over|greater\s*than|more\s*than|>=)\s*([0-9]{3,8})',
         lambda m: ('lowest_emi', {'gte': int(m.group(2))})),
        (r'emi\s*between\s*([0-9]{3,8})\s*and\s*([0-9]{3,8})',
         lambda m: ('lowest_emi', {'gte': min(int(m.group(1)), int(m.group(2))), 'lte': max(int(m.group(1)), int(m.group(2)))})),
        # Handle "phone under 2000 emi" type queries
        (r'(phone|mobile|smartphone|laptop|tv|refrigerator|ac|washing\s*machine)\s*(under|below|less\s*than|upto|<=)\s*([0-9]{3,8})\s*emi',
         lambda m: ('lowest_emi', {'lte': int(m.group(3))})),
        (r'(phone|mobile|smartphone|laptop|tv|refrigerator|ac|washing\s*machine)\s*(above|over|greater\s*than|more\s*than|>=)\s*([0-9]{3,8})\s*emi',
         lambda m: ('lowest_emi', {'gte': int(m.group(3))})),
    ]
    
    for pat, rng_fn in emi_patterns:
        for match in re.finditer(pat, cleaned_query):
            key, val = rng_fn(match)
            filters[key] = val
            cleaned_query = cleaned_query.replace(match.group(0), '').strip()
    
    # --------- 2. Handle price (MOP/offer_price) keywords ---------
    price_patterns = [
        (r'(price|cost|offer price|rate|mop)[^\d]{0,10}(under|below|less than|upto|<=)\s*([0-9]{3,8})',
         lambda m: ('mop', {'lte': int(m.group(3))})),
        (r'(price|cost|offer price|rate|mop)[^\d]{0,10}(above|over|greater than|more than|>=)\s*([0-9]{3,8})',
         lambda m: ('mop', {'gte': int(m.group(3))})),
        (r'(price|cost|offer price|rate|mop)[^\d]{0,10}(from|between|range)?\s*([0-9]{3,8})\s*(to|and|-)\s*([0-9]{3,8})',
         lambda m: ('mop', {'gte': min(int(m.group(4)), int(m.group(6))), 'lte': max(int(m.group(4)), int(m.group(6)))}))
    ]
    
    for pat, rng_fn in price_patterns:
        for match in re.finditer(pat, cleaned_query):
            key, val = rng_fn(match)
            filters[key] = val
            cleaned_query = cleaned_query.replace(match.group(0), '').strip()
    
    # --------- 3. "Under 25000 emi" (emi overrides if explicitly mentioned) ---------
    generic_emi = re.search(r'(under|below|less than|upto|<=)\s*([0-9]{3,8})\s*emi', cleaned_query)
    if generic_emi:
        filters['lowest_emi'] = {'lte': int(generic_emi.group(2))}
        cleaned_query = cleaned_query.replace(generic_emi.group(0), '').strip()
    
    # --------- 4. Generic price "under 35000" NOT followed by 'emi' ---------
    if 'emi' not in cleaned_query and 'installment' not in cleaned_query and 'monthly' not in cleaned_query and 'per month' not in cleaned_query:
        general_patterns = [
            (r'(under|below|less than|upto|<=)\s*([0-9]{3,8})',
             lambda m: ('mop', {'lte': int(m.group(2))})),
            (r'(above|over|greater than|more than|>=)\s*([0-9]{3,8})',
             lambda m: ('mop', {'gte': int(m.group(2))})),
            (r'(from|between|range)?\s*([0-9]{3,8})\s*(to|and|-)\s*([0-9]{3,8})',
             lambda m: ('mop', {'gte': min(int(m.group(2)), int(m.group(4))), 'lte': max(int(m.group(2)), int(m.group(4)))}))
        ]
        
        for pat, rng_fn in general_patterns:
            for match in re.finditer(pat, cleaned_query):
                key, val = rng_fn(match)
                filters[key] = val
                cleaned_query = cleaned_query.replace(match.group(0), '').strip()
    
    # --------- 5. Fallback: number at end (if not already set), treat as price (mop lte) ---------
    if "mop" not in filters and "lowest_emi" not in filters:
        match = re.search(r'([0-9]{3,8})$', cleaned_query)
        if match:
            price = int(match.group(1))
            filters['mop'] = {'lte': price}
            cleaned_query = cleaned_query.replace(match.group(0), '').strip()
    
    # --- Clean up extra spaces ---
    cleaned_query = re.sub(r'\s+', ' ', cleaned_query).strip()
    if not cleaned_query:
        cleaned_query = query
    return cleaned_query, (filters if filters else None)

def expand_search_terms(user_query):
    user_query_n = normalize(user_query)
    variants = set([user_query_n])
 
    # Always include canonical mapping (robust for synonyms and categories)
    canonical_category = CATEGORY_CANONICAL.get(user_query_n)
    if canonical_category:
        variants.add(canonical_category)
 
    # Add synonyms and all mapped forms
    for canon, syns in BUSINESS_SYNONYMS.items():
        if user_query_n == canon or user_query_n in syns:
            variants.add(canon)
            variants.update(syns)
 
    # Split by spaces and add as additional variants
    variants.update(user_query_n.split())
    return sorted({t.strip() for t in variants if t.strip()})

def update_image_url(product):
    for prod in product.get("products", []):
        image = prod.get("image", "")
        if image and not image.startswith("http"):
            if image.startswith("/"):
                prod["image"] = IMAGE_DOMAIN + image
            else:
                prod["image"] = IMAGE_DOMAIN + "/" + image
    return product

def resolve_category_for_exclusions(cat: str):
    if not cat:
        return "blank"
    cat_l = cat.lower()
    resolved = ALL_SYNONYM_TO_CANONICAL.get(cat_l, cat_l)
    return resolved if resolved in FILTER_ATTRIBUTE_EXCLUSIONS else "blank"

def clean_products_for_plp(response_dict):
    """Remove 'label' from attribute_swatch_color and the color/brand fields from each SKU in products list."""
    try:
        for product in response_dict["data"]["PostV1Productlist"]["data"]["products"]:
            for sku in product.get("products", []):
                # Remove fields outside attribute_swatch_color
                for key in ["brand_id", "color_hex_code", "color_label"]:
                    if key in sku:
                        del sku[key]
                # Remove 'label' inside attribute_swatch_color (dict or list of dicts)
                if "attribute_swatch_color" in sku:
                    color = sku["attribute_swatch_color"]
                    if isinstance(color, dict):
                        if "label" in color:
                            del color["label"]
                    elif isinstance(color, list):
                        for c in color:
                            if isinstance(c, dict) and "label" in c:
                                del c["label"]
    except Exception as e:
        import traceback
        print(traceback.format_exc())
    return response_dict

def get_intent_attributes(query):
    query = query.lower()
    colors = ["black", "white", "silver", "blue", "red", "grey", "gold", "green", "yellow", "pink", "purple"]
    storages = ["256 gb", "512 gb", "128 gb", "64 gb", "32 gb", "16 gb", "8 gb", "4 gb", "1 tb", "2 tb"]
    sizes = ["43 inch", "50 inch", "55 inch", "65 inch", "32 inch", "40 inch", "1.5 ton", "2 ton", "6 kg", "7 kg", "8 kg", "9 kg", "10 kg"]
    models = ["pro", "max", "plus", "mini", "air", "smart", "ultra"]
    # Add any other attributes relevant to your business
    intent = {
        "colors": [c for c in colors if c in query],
        "storages": [s for s in storages if s in query],
        "sizes": [s for s in sizes if s in query],
        "models": [m for m in models if m in query]
    }
    return intent

def score_product(prod, intent):
    score = 0
    color = (prod.get("color") or prod.get("colour") or prod.get("attribute_color") or "").lower()
    storage = (prod.get("storage") or prod.get("attribute_storage") or prod.get("attribute_internal_storage") or "").lower()
    size = (prod.get("size") or prod.get("screen_size") or prod.get("attribute_screen_size_in_inches") or "").lower()
    model = (prod.get("model") or prod.get("variant") or prod.get("attribute_variant") or "").lower()
    # Robust: Sometimes product info is nested under "products" key (for multi-SKU)
    if "products" in prod and isinstance(prod["products"], list) and prod["products"]:
        main_sku = prod["products"][0]
        color = (main_sku.get("color") or main_sku.get("colour") or main_sku.get("attribute_color") or color).lower()
        storage = (main_sku.get("storage") or main_sku.get("attribute_storage") or main_sku.get("attribute_internal_storage") or storage).lower()
        size = (main_sku.get("size") or main_sku.get("screen_size") or main_sku.get("attribute_screen_size_in_inches") or size).lower()
        model = (main_sku.get("model") or main_sku.get("variant") or main_sku.get("attribute_variant") or model).lower()
    # Score based on match
    if any(c in color for c in intent["colors"]): score += 3
    if any(s in storage for s in intent["storages"]): score += 2
    if any(sz in size for sz in intent["sizes"]): score += 2
    if any(m in model for m in intent["models"]): score += 1
    return -score  # Negative so best match sorts first

def sort_products_by_intent(products, query):
    intent = get_intent_attributes(query)
    return sorted(products, key=lambda p: score_product(p, intent))

# =================== MODIFIED PROCESS_RESPONSE FUNCTION ===================
def process_response(data, total, city_id=None, emi_range=None, filters=None):
    """
    Processes raw Elasticsearch hits into the correct PLP response structure,
    with robust city-wise filtering using inner_hits.
    """
    final_response = {
        "data": {
            "PostV1Productlist": {
                "status": True,
                "message": "Success",
                "data": {
                    "products": [],
                    "totalrecords": total,
                    "suggested_search_keyword": "*",
                    "filters": [],
                }
            }
        }
    }
    
    emi_values = []
    final_filters = {}
    canonical_cat = "blank"
    
    # Determine category for filter exclusions
    for entry in data:
        cat = entry["_source"].get("actual_category", "")
        if cat:
            canonical_cat = resolve_category_for_exclusions(cat)
            break
    
    excluded_filters = FILTER_ATTRIBUTE_EXCLUSIONS.get(canonical_cat, set())
    seen_skus = set()
    
    for hit in data:
        source = hit["_source"]
        source = update_image_url(source)
        
        # Get city-specific offer - pass the full hit object
        city_offer = get_city_offer(hit, city_id)
        if not city_offer:
            continue  # Skip products without offers for requested city or default city
        
        # FIXED: Use modelid from the source document, not from inner products
        model_id = source.get("modelid", source.get("model_id"))
        if model_id in seen_skus:
            continue
        seen_skus.add(model_id)
        
        # Collect EMI values for slider
        emi_values.append(city_offer.get("lowest_emi", 0))
        
        temp_dict = {
            "model_id": model_id,
            "model_launch_date": source.get("model_launch_date", "1970-01-01"),
            "mkp_active_flag": source.get("mkp_active_flag", 0),
            "avg_rating": source.get("avg_rating", 0),
            "rating_count": source.get("rating_count", 0),
            "asset_category_id": source.get("asset_category_id", 0),
            "asset_category_name": source.get("asset_category_name", "UNKNOWN"),
            "manufacturer_id": source.get("manufacturer_id", 999),
            "manufacturer_desc": source.get("manufacturer_desc", "UNKNOWN"),
            "category_type": source.get("category_type", "UNKNOWN"),
            "mop": source.get("mop", 0),
            "property": [],
            "products": []
        }
        
        # Set property with city-specific data
        property_dict = {
            "cityid": [city_offer.get("cityid")],
            "transaction_count": city_offer.get("transaction_count", 0),
            "lowest_emi": city_offer.get("lowest_emi", 0),
            "mop": city_offer.get("offer_price", 0),
            "offer_price": city_offer.get("offer_price", 0),
            "score": city_offer.get("score", 0),
            "ty_page_count": city_offer.get("ty_page_count", 0),
            "one_emi_off": city_offer.get("one_emi_off", 0),
            "pdp_view_count": city_offer.get("pdp_view_count", 0),
            "off_percentage": city_offer.get("off_percentage", 0),
            "zero_dp_flag": city_offer.get("zero_dp_flag", 0),
            "new_launch_flag": city_offer.get("new_launch_flag", 0),
            "most_viewed_flag": city_offer.get("most_viewed_flag", 0),
            "top_seller_flag": city_offer.get("top_seller_flag", 0),
            "highest_tenure": city_offer.get("highest_tenure", 0),
            "model_city_flag": city_offer.get("model_city_flag", 0),
            "phone_setup": city_offer.get("phone_setup", 0),
            "exchange_flag": city_offer.get("exchange_flag", 0),
            "installation_flag": city_offer.get("installation_flag", 0),
        }
        temp_dict["property"].append(property_dict)
        
        # FIXED: Ensure SKUs are properly processed
        products_list = source.get("products", [])
        if not products_list:
            # If no products list, add the source as a single SKU
            sku_item = {
                "name": source.get("product_name", ""),
                "sku": source.get("sku", ""),
                "image": source.get("image", ""),
                # Add other necessary fields from the source
            }
            # Copy any relevant attributes from source to SKU
            for key in ["attribute_color", "attribute_internal_storage", "attribute_ram"]:
                if key in source:
                    sku_item[key] = source[key]
            temp_dict["products"].append(sku_item)
        else:
            # Process each SKU in the products list
            for sku_item in products_list:
                # Ensure each SKU has required fields
                if "name" not in sku_item and "product_name" in source:
                    sku_item["name"] = source["product_name"]
                if "image" not in sku_item and "image" in source:
                    sku_item["image"] = source["image"]
                temp_dict["products"].append(sku_item)
        
        final_response["data"]["PostV1Productlist"]["data"]["products"].append(temp_dict)
        
        # Build attribute filters
        attribute_name = [field.replace("_value", "") for field in source.keys() if (
            field.startswith("attribute_") and field.endswith("_value"))]
        
        for filter_entry in attribute_name:
            if filter_entry in excluded_filters:
                continue
            value = source.get(f"{filter_entry}_value", "")
            fid = source.get(filter_entry, "")
            if not value or value in ["", "UNKNOWN", None]: 
                continue
            if filter_entry not in final_filters:
                final_filters[filter_entry] = set()
            
            # Special case for color swatch
            if "color_swatch" in filter_entry.lower() or "swatch_color" in filter_entry.lower():
                color_entries = set()
                for sku in source.get("products", []):
                    color_info = sku.get("attribute_swatch_color", {})
                    color_hex = color_info.get("value") or ""
                    color_name = color_info.get("name") or ""
                    color_id = color_info.get("id") or fid or ""
                    if color_hex and color_name and color_id:
                        color_entries.add((f"{color_hex}_{color_name}", color_id))
                if not color_entries and value and fid:
                    color_entries.add((value, fid))
                for name_out, id_out in color_entries:
                    final_filters[filter_entry].add((name_out, id_out))
            else:
                final_filters[filter_entry].add((value, fid))
    
    # Compile final filters in required format
    for key, items in final_filters.items():
        unique_items = []
        seen_ids = set()
        for val, fid in items:
            if fid not in seen_ids:
                unique_items.append({"name": val, "id": fid})
                seen_ids.add(fid)
        final_filters[key] = unique_items
    
    final_filters = {k: v for k, v in final_filters.items() if len(v) > 0}
    
    if filters:
        temp_final_filter = {}
        temp_final_filter["attributes"] = filters
        final_response["data"]["PostV1Productlist"]["data"]["filters"].append(temp_final_filter)
    
    # Add EMI filter if present
    if len(emi_values) > 0 and len(final_response["data"]["PostV1Productlist"]["data"]["filters"]) > 0:
        # Calculate the overall min and max EMI from the current results
        overall_min_emi = min(emi_values)
        overall_max_emi = max(emi_values)
        
        # If an emi_range is provided (user's selection), use it for the filter display
        if emi_range:
            # The emi_range is a dict that may have 'gte' and/or 'lte'
            min_emi = emi_range.get('gte', overall_min_emi)
            max_emi = emi_range.get('lte', overall_max_emi)
        else:
            min_emi = overall_min_emi
            max_emi = overall_max_emi
        
        final_response["data"]["PostV1Productlist"]["data"]["filters"][0]["emi"] = {
            "max": max_emi,
            "min": min_emi
        }
    
    return final_response

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
def normalize_spec_value(value):
    """Normalize specification values for consistent matching"""
    if not value:
        return ""
    # Convert to lowercase
    value = value.lower()
    # Remove spaces in numbers with units (e.g., "2 ton" -> "2ton")
    value = re.sub(r'(\d+)\s+(ton|kg|gb|inch|in)', r'\1\2', value)
    # Standardize units
    value = value.replace('in', 'inch')
    value = value.replace('ton', 'ton')
    value = value.replace('kg', 'kg')
    value = value.replace('gb', 'gb')
    return value.strip()

def extract_query_specifications(query):
    """Extract key specifications from user query"""
    query_lower = query.lower()
    specs = {}
    
    # Extract AC tonnage
    tonnage_match = re.search(r'(\d+\.?\d*)\s*ton', query_lower)
    if tonnage_match:
        specs['tonnage'] = normalize_spec_value(tonnage_match.group(0))
    
    # Extract washing machine capacity
    capacity_match = re.search(r'(\d+)\s*kg', query_lower)
    if capacity_match:
        specs['capacity'] = normalize_spec_value(capacity_match.group(0))
    
    # Extract TV size
    size_match = re.search(r'(\d+)\s*inch', query_lower)
    if size_match:
        specs['size'] = normalize_spec_value(size_match.group(0))
    
    # Extract storage
    storage_match = re.search(r'(\d+)\s*gb', query_lower)
    if storage_match:
        specs['storage'] = normalize_spec_value(storage_match.group(0))
    
    # Extract brand
    for brand in BRAND_NAMES:
        if brand.lower() in query_lower:
            specs['brand'] = brand.lower()
            break
    
    return specs

def score_product_by_specifications(product, query_specs):
    """Score product based on exact specification matches"""
    score = 0
    
    # Check tonnage match (AC)
    if 'tonnage' in query_specs:
        prod_tonnage = normalize_spec_value(get_attribute(product, ["attribute_capacity_in_tons"]))
        if query_specs['tonnage'] == prod_tonnage:
            score += 50  # Very high score for exact tonnage match
    
    # Check capacity match (Washing Machine)
    if 'capacity' in query_specs:
        prod_capacity = normalize_spec_value(get_attribute(product, ["attribute_capacity_wm"]))
        if query_specs['capacity'] == prod_capacity:
            score += 50  # Very high score for exact capacity match
    
    # Check size match (TV)
    if 'size' in query_specs:
        prod_size = normalize_spec_value(get_attribute(product, ["size", "screen_size", "attribute_screen_size_in_inches"]))
        if query_specs['size'] == prod_size:
            score += 50  # Very high score for exact size match
    
    # Check storage match (Mobile/Laptop)
    if 'storage' in query_specs:
        prod_storage = normalize_spec_value(get_attribute(product, ["storage", "attribute_storage", "attribute_internal_storage"]))
        if query_specs['storage'] == prod_storage:
            score += 40  # High score for exact storage match
    
    # Check brand match
    if 'brand' in query_specs:
        prod_brand = get_attribute(product, ["manufacturer_desc", "brand"]).lower()
        if query_specs['brand'] in prod_brand:
            score += 30  # Good score for brand match
    
    return score

def rank_by_specifications(products, user_query):
    """Rank products by exact specification matches"""
    query_specs = extract_query_specifications(user_query)
    
    if not query_specs:
        return products
    
    # Score each product
    scored_products = []
    for product in products:
        score = score_product_by_specifications(product, query_specs)
        scored_products.append((product, score))
    
    # Sort by score (descending) and then by relevance
    scored_products.sort(key=lambda x: (-x[1], x[0].get('score', 0)), reverse=False)
    
    return [p[0] for p in scored_products]

def normalize_sku_name(sku_name):
    """
    Normalize SKU names by removing parenthetical content and special characters
    while preserving the core product information.
    """
    if not sku_name:
        return ""
    
    # Convert to lowercase
    normalized = sku_name.lower()
    
    # Remove content within parentheses (including the parentheses)
    normalized = re.sub(r'\([^)]*\)', '', normalized)
    
    # Remove special characters except spaces, alphanumeric, and common units
    normalized = re.sub(r'[^\w\s\.]', '', normalized)
    
    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    # Normalize common units and abbreviations
    normalized = normalized.replace('gb', 'gb')
    normalized = normalized.replace('tb', 'tb')
    normalized = normalized.replace('inch', 'inch')
    normalized = normalized.replace('in', 'inch')
    normalized = normalized.replace('ton', 'ton')
    normalized = normalized.replace('kg', 'kg')
    normalized = normalized.replace('mah', 'mah')
    
    return normalized

def rank_exact_sku_matches_first(products, user_query):
    """
    Ranks products with exact SKU name or search_field matches at the top,
    using robust normalization to handle parenthetical content.
    """
    # Normalize the user query
    normalized_query = normalize_sku_name(user_query)
    query_terms = normalized_query.split()
    
    # Extract specifications for partial matching
    query_specs = extract_query_specifications(user_query)
    
    # Define match tiers
    exact_matches = []
    spec_matches = []
    partial_matches = []
    other_matches = []
    
    for product in products:
        match_score = 0
        match_reason = []
        
        # Normalize product fields
        product_name = normalize_sku_name(product.get("product_name", ""))
        search_field = normalize_sku_name(product.get("search_field", ""))
        
        # Check for exact match on product name or search field
        if normalized_query == product_name or normalized_query == search_field:
            match_score = 100
            match_reason.append("exact_name_match")
        
        # Check for exact model ID match
        if match_score == 0:
            model_id = str(product.get("model_id", product.get("modelid", "")))
            if normalized_query == model_id.lower():
                match_score = 95
                match_reason.append("exact_model_id_match")
        
        # Check SKU names
        if match_score == 0:
            for sku in product.get("products", []):
                sku_name = normalize_sku_name(sku.get("name", ""))
                if normalized_query == sku_name:
                    match_score = 90
                    match_reason.append("exact_sku_match")
                    break
        
        # Check for specification matches (even if not exact name match)
        if match_score == 0 and query_specs:
            spec_score = score_product_by_specifications(product, query_specs)
            if spec_score >= 40:  # Only consider significant spec matches
                match_score = spec_score
                match_reason.append("spec_match")
        
        # Check for partial matches (all query terms present)
        if match_score == 0:
            # Check if all query terms are in the product name
            if all(term in product_name for term in query_terms):
                match_score = 30
                match_reason.append("partial_name_match")
            # Check if all query terms are in the search field
            elif all(term in search_field for term in query_terms):
                match_score = 25
                match_reason.append("partial_search_field_match")
            # Check SKU names
            else:
                for sku in product.get("products", []):
                    sku_name = normalize_sku_name(sku.get("name", ""))
                    if all(term in sku_name for term in query_terms):
                        match_score = 20
                        match_reason.append("partial_sku_match")
                        break
        
        # Categorize based on match score
        if match_score >= 90:
            exact_matches.append((product, match_score, match_reason))
        elif match_score >= 40:
            spec_matches.append((product, match_score, match_reason))
        elif match_score >= 20:
            partial_matches.append((product, match_score, match_reason))
        else:
            other_matches.append(product)
    
    # Sort each category by score (descending)
    exact_matches.sort(key=lambda x: x[1], reverse=True)
    spec_matches.sort(key=lambda x: x[1], reverse=True)
    partial_matches.sort(key=lambda x: x[1], reverse=True)
    
    # Extract products from sorted tuples
    exact_products = [p[0] for p in exact_matches]
    spec_products = [p[0] for p in spec_matches]
    partial_products = [p[0] for p in partial_matches]
    
    # Return combined results in order of priority
    return exact_products + spec_products + partial_products + other_matches

################################################
def is_mobile_query(query_n):
    all_mobiles_syns = set(BUSINESS_SYNONYMS.get("mobile phones", [])) | set([
        "mobile phones", "mobiles", "mobiles", "smartphone", "phone", "phones", "mobilephone", "cellphone", "cell phone"
    ])
    qn = query_n
    qn2 = query_n.replace(" ", "")
    return qn in all_mobiles_syns or qn2 in all_mobiles_syns

def is_apple_query(query_n):
    all_apple_syns = set(BUSINESS_SYNONYMS.get("apple", [])) | set([
        "apple", "iphone", "iphones", "apple phone", "apple phones", "apple mobile", "apple mobiles", "apple smartphone"
    ])
    qn = query_n
    qn2 = query_n.replace(" ", "")
    return qn in all_apple_syns or qn2 in all_apple_syns

def patch_brand_token_must(parsed, user_query):
    tokens = [t for t in parsed["tokens"]]
    brands = set([b.lower() for b in parsed["brands"]])
    must = []
    must_not = []
    # Handle spacing variants for numbers/GB/MB etc.
    def gen_variants(t):
        if "gb" in t.lower() and not " " in t.lower():
            base = t.lower().replace("gb", "")
            return [t, f"{base} GB", f"{base} gb", f"{base}GB", f"{base}Gb"]
        return [t]
    if brands:
        for b in brands:
            must.append({
                "bool": {
                    "should": [
                        {"match_phrase": {"product_name": b}},
                        {"match_phrase": {"search_field": b}},
                    ],
                    "minimum_should_match": 1
                }
            })
        # Each *other* token: allow ANY variant match in any key field
        for t in tokens:
            if t.lower() not in brands and len(t) > 2:
                variants = gen_variants(t)
                must.append({
                    "bool": {
                        "should": [
                            {"multi_match": {
                                "query": variant,
                                "fields": [
                                    "product_name^3", "search_field^2", "sku", "products.name^2", "products.sku"
                                ],
                                "operator": "or",
                                "fuzziness": "AUTO"
                            }} for variant in variants
                        ],
                        "minimum_should_match": 1
                    }
                })
    return must, must_not

def rank_by_match_priority(products, user_query):
    """
    Ranks products based on match priority:
    1. Exact match with product_name or search_field (highest priority)
    2. Fuzzy match with 80%+ similarity (medium priority)
    3. Category and brand matches (lower priority)
    """
    # Normalize the user query
    normalized_query = normalize_sku_name(user_query)
    query_terms = normalized_query.split()
    
    # Score each product
    scored_products = []
    for product in products:
        score = 0
        match_reasons = []
        
        # Normalize product fields
        product_name = normalize_sku_name(product.get("product_name", ""))
        search_field = normalize_sku_name(product.get("search_field", ""))
        category = product.get("actual_category", "").lower()
        brand = product.get("manufacturer_desc", "").lower()
        
        # 1. Exact match check (highest priority)
        if normalized_query == product_name or normalized_query == search_field:
            score = 100
            match_reasons.append("exact_match")
        else:
            # 2. Fuzzy match check (80%+ threshold)
            name_ratio = fuzz.ratio(normalized_query, product_name)
            field_ratio = fuzz.ratio(normalized_query, search_field)
            max_ratio = max(name_ratio, field_ratio)
            
            if max_ratio >= 80:
                score = 80
                match_reasons.append(f"fuzzy_match_{max_ratio}%")
            else:
                # 3. Category and brand matches (lower priority)
                # Check if all query terms are in the product name or search field
                all_terms_in_name = all(term in product_name for term in query_terms)
                all_terms_in_field = all(term in search_field for term in query_terms)
                
                if all_terms_in_name or all_terms_in_field:
                    score = 60
                    match_reasons.append("all_terms_present")
                else:
                    # Check category match
                    if any(term in category for term in query_terms):
                        score = 50
                        match_reasons.append("category_match")
                    
                    # Check brand match
                    if any(term in brand for term in query_terms):
                        score = 40
                        match_reasons.append("brand_match")
        
        scored_products.append((product, score, match_reasons))
    
    # Sort by score (descending) and then by relevance
    scored_products.sort(key=lambda x: (-x[1], x[0].get('score', 0)), reverse=False)
    
    # Return just the products in sorted order
    return [p[0] for p in scored_products]

def is_refurbished(prod):
    for field in [
        prod.get("actual_category", ""),
        prod.get("category_type", ""),
        prod.get("model_id", ""),
        prod.get("model_launch_date", ""),
        prod.get("manufacturer_desc", ""),
        prod.get("asset_category_name", ""),
        prod.get("product_name", ""),
    ]:
        if field and "refurbished" in field.lower():
            return True
    for sku in prod.get("products", []):
        if "refurbished" in (sku.get("name", "") + sku.get("sku", "")).lower():
            return True
    return False

# =================== MODIFIED BUILD_ADVANCED_SEARCH_QUERY FUNCTION ===================
def normalize_specification(value, spec_type):
    """
    Normalize specification values by removing parentheses and standardizing format
    """
    # Remove parentheses if present
    value = re.sub(r'[()]', '', value.strip())
    
    # Normalize float values
    if '.' in value:
        value = str(float(value))
    
    # Add the specification type
    normalized = f"{value} {spec_type}"
    
    return normalized.strip()


# Canonical brand list (add full names you actually store in ES)
BRAND_NAMES += [
    "maruti suzuki", "volkswagen", "mercedes", "skoda"
]

# Map short/colloquial tokens to the canonical value you store
BRAND_ALIASES = {
    "maruti": "maruti suzuki",
    "maruti-suzuki": "maruti suzuki",
    
}

def expand_brand_aliases_in_query(q: str) -> str:
    if not q:
        return q
    import re
    out = " " + q.lower() + " "
    for k, v in BRAND_ALIASES.items():
        out = re.sub(rf"\b{k}\b", v, out)
    return out.strip()

def deep_freezer_must_not_clauses():
    """
    Returns a list of ES must_not clauses that exclude deep freezers by
    category, product name/search_field phrases, and SKU/name variants.
    """
    terms = [
        "deep freezer", "deepfreezer", "deep-freezer", "deep freeze",
        "chest freezer", "upright freezer", "glass top freezer", "visicooler", "visi cooler"
    ]
    must_not = [
        {"term": {"actual_category": "deep freezer"}},
        {"match": {"actual_category": {"query": "deep freezer"}}},
        {"match_phrase": {"asset_category_name": "Deep Freezer"}}
    ]
    # Block name/search_field phrases
    for t in terms:
        must_not.append({"match_phrase": {"product_name": t}})
        must_not.append({"match_phrase": {"search_field": t}})

    # Block within nested SKUs too
    must_not.append({
        "nested": {
            "path": "products",
            "query": {
                "bool": {
                    "should": [
                        {"match_phrase": {"products.name": "deep freezer"}},
                        {"match_phrase": {"products.name": "deepfreezer"}},
                        {"match_phrase": {"products.name": "deep freeze"}},
                        {"match_phrase": {"products.name": "chest freezer"}},
                        {"match_phrase": {"products.name": "upright freezer"}},
                        {"match_phrase": {"products.name": "glass top freezer"}},
                        {"match_phrase": {"products.name": "blue star"}},
                        # SKU variants / typos (wildcards on keyword fields are fine)
                        {"wildcard": {"products.sku": {"value": "*deep*freez*"}}},
                        {"wildcard": {"products.sku": {"value": "*deep*freezer*"}}},
                        {"wildcard": {"products.sku": {"value": "*chest*freezer*"}}},
                        {"wildcard": {"products.sku": {"value": "*upright*freezer*"}}}
                    ],
                    "minimum_should_match": 1
                }
            }
        }
    })
    return must_not


def build_parentheses_aware_fuzzy_query(user_query, fields, boost=1):
    """
    Build fuzzy query that handles content in parentheses separately
    """
    query_lower = user_query.lower()
    
    # Extract content in parentheses
    parentheses_matches = re.findall(r'\(([^)]+)\)', query_lower)
    
    # Remove parentheses content from main query
    main_query = re.sub(r'\([^)]*\)', '', query_lower).strip()
    
    queries = []
    
    # Build query for main content (without parentheses)
    if main_query:
        main_tokens = re.findall(r'\b\w+\b', main_query)
        for token in main_tokens:
            if len(token) < 2:
                continue
            queries.append({
                "match": {
                    "product_name": {
                        "query": token,
                        "fuzziness": "AUTO",
                        "boost": boost
                    }
                }
            })
    
    # Build query for parentheses content
    for content in parentheses_matches:
        # Try to extract specifications from parentheses content
        for spec_type, patterns in fuzzy_spec_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, content)
                if match:
                    value = match.group(1)
                    normalized = normalize_specification(value, spec_type)
                    
                    # Boost normalized specification matches
                    queries.append({
                        "match": {
                            "product_name": {
                                "query": normalized,
                                "fuzziness": "AUTO",
                                "boost": boost * 2
                            }
                        }
                    })
                    
                    # Also try matching with parentheses
                    queries.append({
                        "match": {
                            "product_name": {
                                "query": f"({normalized})",
                                "fuzziness": "AUTO",
                                "boost": boost * 1.5
                            }
                        }
                    })
                    break
        
        # If no specification found, treat as general content
        tokens = re.findall(r'\b\w+\b', content)
        for token in tokens:
            if len(token) < 2:
                continue
            queries.append({
                "match": {
                    "product_name": {
                        "query": token,
                        "fuzziness": "AUTO",
                        "boost": boost * 1.2
                    }
                }
            })
    
    if not queries:
        return None
    
    return {
        "bool": {
            "should": queries,
            "minimum_should_match": 1
        }
    }

def build_fuzzy_query_with_tokens(user_query, fields, boost=1, fuzziness="AUTO"):
    """
    Build a fuzzy query that handles tokenized queries with spelling mistakes
    """
    query_lower = user_query.lower()
    
    # Extract potential specifications and brands with fuzzy patterns
    spec_patterns = {
        'ton': r'(\d+\.?\d*)\s*to[nm]?',  # Handles "ton", "tom", "ton"
        'inch': r'(\d+\.?\d*)\s*inc[h]?',   # Handles "inch", "inc", "in"
        'gb': r'(\d+)\s*g[b]?',             # Handles "gb", "g"
        'kg': r'(\d+)\s*k[g]?',             # Handles "kg", "k"
    }
    
    # Extract fuzzy specifications
    extracted_specs = {}
    for spec_type, pattern in spec_patterns.items():
        match = re.search(pattern, query_lower)
        if match:
            value = match.group(1)
            if '.' in value:
                value = str(float(value))
            extracted_specs[spec_type] = value
    
    # Extract brand with fuzzy matching
    query_brand = None
    for brand in BRAND_NAMES:
        # Use fuzzy matching for brand detection
        ratio = fuzz.ratio(query_lower, brand.lower())
        if ratio >= 80 or brand.lower() in query_lower:
            query_brand = brand
            break
    
    # Build token-based fuzzy query
    tokens = re.findall(r'\b\w+\b', query_lower)
    token_queries = []
    
    for token in tokens:
        # Skip very short tokens (likely noise)
        if len(token) < 2:
            continue
            
        # Build fuzzy match for each token
        token_query = {
            "bool": {
                "should": [
                    # Fuzzy match in product name
                    {
                        "match": {
                            "product_name": {
                                "query": token,
                                "fuzziness": fuzziness,
                                "boost": boost
                            }
                        }
                    },
                    # Fuzzy match in search field
                    {
                        "match": {
                            "search_field": {
                                "query": token,
                                "fuzziness": fuzziness,
                                "boost": boost * 0.8
                            }
                        }
                    },
                    # Fuzzy match in product keywords
                    {
                        "match": {
                            "product_keywords": {
                                "query": token,
                                "fuzziness": fuzziness,
                                "boost": boost * 0.6
                            }
                        }
                    }
                ]
            }
        }
        token_queries.append(token_query)
    
    # Build specification-based fuzzy queries
    spec_queries = []
    if extracted_specs:
        for spec_type, spec_value in extracted_specs.items():
            # Map specification to attribute fields
            attr_field_map = {
                'ton': 'attribute_capacity_in_tons',
                'inch': 'attribute_screen_size_in_inches',
                'gb': 'attribute_internal_storage',
                'kg': 'attribute_capacity_wm',
                'ton': 'attribute_capacity_in_tons',                
                'litre': 'attribute_capacity_litres',
                'L': 'attribute_capacity_litres',
            }
            
            if spec_type in attr_field_map:
                spec_queries.append({
                    "match": {
                        attr_field_map[spec_type]: {
                            "query": spec_value,
                            "fuzziness": "1",  # Less fuzziness for numeric values
                            "boost": boost * 2
                        }
                    }
                })
                
                # Also try fuzzy matching in product name
                spec_queries.append({
                    "match": {
                        "product_name": {
                            "query": f"{spec_value} {spec_type}",
                            "fuzziness": fuzziness,
                            "boost": boost * 1.5
                        }
                    }
                })
    
    # Build brand-based fuzzy queries
    brand_queries = []
    if query_brand:
        brand_queries.append({
            "match": {
                "manufacturer_desc": {
                    "query": query_brand,
                    "fuzziness": "AUTO",
                    "boost": boost * 2
                }
            }
        })
        
        # Also try in product name
        brand_queries.append({
            "match": {
                "product_name": {
                    "query": query_brand,
                    "fuzziness": fuzziness,
                    "boost": boost * 1.8
                }
            }
        })
    
    # Combine all queries
    all_queries = token_queries + spec_queries + brand_queries
    
    if not all_queries:
        return None
    
    # Return a should clause with minimum should match
    return {
        "bool": {
            "should": all_queries,
            "minimum_should_match": max(1, len(all_queries) // 2)  # At least half should match
        }
    }

def find_fuzzy_brand_match(query, brands, threshold=80):
    query_lower = query.lower()
    
    # First try direct substring match
    for brand in brands:
        if brand.lower() in query_lower:
            return brand
    
    # Then try fuzzy matching for each word in the query
    words = re.findall(r'\b\w+\b', query_lower)
    for word in words:
        if len(word) < 3:  # Skip very short words
            continue
            
        for brand in brands:
            # Check brand name against query word
            ratio = fuzz.ratio(word, brand.lower())
            if ratio >= threshold:
                return brand
                
            # Check if word is part of brand name
            if word in brand.lower():
                return brand
    
    # Finally, try partial ratio for multi-word brands
    for brand in brands:
        brand_parts = brand.lower().split()
        for part in brand_parts:
            if len(part) < 3:
                continue
            ratio = fuzz.partial_ratio(query_lower, part)
            if ratio >= threshold:
                return brand
    
    return None

# Define global fuzzy_spec_patterns for use in helper functions
fuzzy_spec_patterns = {
    'ton': [
        r'(\d+\.?\d*)\s*ton',      # Correct spelling
        r'(\d+\.?\d*)\s*tom',      # Common misspelling
        r'(\d+\.?\d*)\s*toc',      # Another common misspelling
        r'(\d+\.?\d*)\s*to[nm]?',  # More variations
        r'\((\d+\.?\d*)\s*ton\)',  # With parentheses: (1.5 ton)
        r'\((\d+\.?\d*)\s*tom\)',  # With parentheses and misspelling: (1.5 tom)
    ],
    'inch': [
        r'(\d+\.?\d*)\s*inch',     # Correct spelling
        r'(\d+\.?\d*)\s*inc',      # Common misspelling
        r'(\d+\.?\d*)\s*in',       # Short form
        r'(\d+\.?\d*)\s*inc[h]?',   # More variations
        r'\((\d+\.?\d*)\s*inch\)', # With parentheses: (45 inch)
        r'\((\d+\.?\d*)\s*inc\)',  # With parentheses and misspelling: (45 inc)
        r'\((\d+\.?\d*)\s*in\)',   # With parentheses and short form: (45 in)
    ],
    'gb': [
        r'(\d+)\s*gb',             # Correct spelling
        r'(\d+)\s*g[b]?',          # With optional b
        r'\((\d+)\s*gb\)',         # With parentheses: (8gb)
        r'\((\d+)\s*g\)',          # With parentheses and short form: (8g)
    ],
    'kg': [
        r'(\d+)\s*kg',             # Correct spelling
        r'(\d+)\s*k[g]?',          # With optional g
        r'\((\d+)\s*kg\)',         # With parentheses: (7kg)
    ],
    'ram': [
        r'(\d+)\s*gb\s*ram',       # Correct spelling
        r'(\d+)\s*gb\s*ra[m]?',    # With fuzzy matching
        r'\((\d+)\s*gb\s*ram\)',   # With parentheses: (8gb ram)
    ],
    
    'litre': [
        r'(\d{2,4})\s*l(?:itre|iter|itres|iters|ltr|ltrs|L|l)?\b',
        r'\((\d{2,4})\s*l(?:itre|iter|itres|iters|ltr|ltrs|L|l)?\)'
    # With parentheses: (8gb ram)
    ],
}

def build_advanced_search_query(
    user_query,
    filters=None,
    city_id=None,
    mapped_category=None,
    price_filter=None,
    products_is_nested=True,
    emi_range=None,
):
    terms = expand_search_terms(user_query)
    should_clauses = []
    must_clauses = []
    must_not_clauses = []
    category_boosts = []
    filter_clauses = []
    
    import re
    
    # Add city filter if city_id is provided
    if city_id:
        city_filter = build_city_filter(city_id)
        if city_filter:
            filter_clauses.append(city_filter)
    
    # Extract query tokens for better understanding
    query_tokens = user_query.lower().split()
    
    # Extract brand from query if present
    query_brand = None
    for brand in BRAND_NAMES:
        if brand.lower() in user_query.lower():
            query_brand = brand
            break
    
    # === Enhanced Specification Extraction with Parentheses Support ===
    extracted_specs = {}
    for spec_type, patterns in fuzzy_spec_patterns.items():
        for pattern in patterns:
            match = re.search(pattern, user_query.lower())
            if match:
                value = match.group(1)
                # Normalize float values
                if '.' in value:
                    value = str(float(value))
                extracted_specs[spec_type] = value
                break  # Use first match
    
    # Build normalized specifications for matching
    normalized_specs = {}
    for spec_type, value in extracted_specs.items():
        normalized_specs[spec_type] = normalize_specification(value, spec_type)
    
    # === Enhanced Brand Detection with Fuzzy Matching ===
    query_brand = find_fuzzy_brand_match(user_query, BRAND_NAMES, threshold=75)
    
    # === Multi-Layer Fuzzy Matching ===
    # Build comprehensive fuzzy query for handling spelling mistakes
    fuzzy_query = build_fuzzy_query_with_tokens(user_query, 
                                               fields=["product_name", "search_field"], 
                                               boost=5, 
                                               fuzziness="AUTO")
    
    if fuzzy_query:
        should_clauses.append(fuzzy_query)
    
    # Additional fuzzy matching for SKU fields
    should_clauses.append({
        "nested": {
            "path": "products",
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "products.sku": {
                                    "query": user_query,
                                    "fuzziness": "AUTO",
                                    "boost": 15
                                }
                            }
                        },
                        {
                            "match": {
                                "products.name": {
                                    "query": user_query,
                                    "fuzziness": "AUTO",
                                    "boost": 12
                                }
                            }
                        }
                    ]
                }
            }
        }
    })
    
    # Fuzzy matching for attribute fields
    attribute_fields = [
        "attribute_capacity_in_tons",
        "attribute_screen_size_in_inches", 
        "attribute_internal_storage",
        "attribute_ram",
        "attribute_capacity_wm",
        "attribute_battery_capacity_new"
    ]
    
    for attr_field in attribute_fields:
        should_clauses.append({
            "match": {
                attr_field: {
                    "query": user_query,
                    "fuzziness": "1",  # Less fuzziness for numeric attributes
                    "boost": 8
                }
            }
        })
    
    # Multi-token fuzzy matching for phrases
    # Break query into phrases of 2-3 words and match them fuzzily
    query_words = re.findall(r'\b\w+\b', user_query.lower())
    phrase_queries = []
    
    for i in range(len(query_words)):
        for j in range(i+1, min(i+4, len(query_words)+1)):  # Phrases of 2-3 words
            phrase = " ".join(query_words[i:j])
            if len(phrase) < 4:  # Skip very short phrases
                continue
                
            phrase_queries.append({
                "match_phrase": {
                    "product_name": {
                        "query": phrase,
                        "boost": 10,
                        "slop": 1  # Allow one word difference
                    }
                }
            })
    
    if phrase_queries:
        should_clauses.append({
            "bool": {
                "should": phrase_queries,
                "minimum_should_match": 1
            }
        })
    
    # === Enhanced Exact Match for Parentheses Content ===
    if normalized_specs:
        for spec_type, normalized_spec in normalized_specs.items():
            # Exact match without parentheses
            should_clauses.append({
                "multi_match": {
                    "query": normalized_spec,
                    "fields": ["product_name^20", "search_field^15"],
                    "type": "phrase"
                }
            })
            
            # Exact match with parentheses
            parenthesized_spec = f"({normalized_spec})"
            should_clauses.append({
                "multi_match": {
                    "query": parenthesized_spec,
                    "fields": ["product_name^18", "search_field^12"],
                    "type": "phrase"
                }
            })
            
            # Also boost in attribute fields
            attr_field_map = {
                'ton': 'attribute_capacity_in_tons',
                'inch': 'attribute_screen_size_in_inches',
                'gb': 'attribute_internal_storage',
                'kg': 'attribute_capacity_wm',
            }
            
            if spec_type in attr_field_map:
                should_clauses.append({
                    "term": {
                        attr_field_map[spec_type]: {
                            "value": extracted_specs[spec_type],
                            "boost": 15
                        }
                    }
                })
    
    # === Enhanced SKU and Search Field Matching ===
    # Enhanced SKU matching with parentheses support
    should_clauses.append({
        "nested": {
            "path": "products",
            "query": {
                "bool": {
                    "should": [
                        # Exact SKU match
                        {
                            "term": {
                                "products.sku": {
                                    "value": user_query,
                                    "boost": 25
                                }
                            }
                        },
                        # Fuzzy SKU match
                        {
                            "match": {
                                "products.sku": {
                                    "query": user_query,
                                    "fuzziness": "AUTO",
                                    "boost": 20
                                }
                            }
                        },
                        # Match SKU content with parentheses
                        {
                            "wildcard": {
                                "products.sku": {
                                    "value": f"*{user_query}*",
                                    "boost": 15
                                }
                            }
                        },
                        # Exact product name match
                        {
                            "term": {
                                "products.name": {
                                    "value": user_query,
                                    "boost": 20
                                }
                            }
                        },
                        # Fuzzy product name match
                        {
                            "match": {
                                "products.name": {
                                    "query": user_query,
                                    "fuzziness": "AUTO",
                                    "boost": 15
                                }
                            }
                        },
                        # Match product name with parentheses
                        {
                            "wildcard": {
                                "products.name": {
                                    "value": f"*{user_query}*",
                                    "boost": 12
                                }
                            }
                        }
                    ]
                }
            }
        }
    })
    
    # Enhanced search field matching with parentheses support
    should_clauses.append({
        "bool": {
            "should": [
                # Exact search field match
                {
                    "term": {
                        "search_field": {
                            "value": user_query,
                            "boost": 20
                        }
                    }
                },
                # Fuzzy search field match
                {
                    "match": {
                        "search_field": {
                            "query": user_query,
                            "fuzziness": "AUTO",
                            "boost": 15
                        }
                    }
                },
                # Wildcard match for parentheses content
                {
                    "wildcard": {
                        "search_field": {
                            "value": f"*{user_query}*",
                            "boost": 12
                        }
                    }
                },
                # Match normalized specifications
                *[
                    {
                        "match": {
                            "search_field": {
                                "query": normalized_spec,
                                "boost": 18
                            }
                        }
                    }
                    for normalized_spec in normalized_specs.values()
                ]
            ]
        }
    })
    
    # === Parentheses-Aware Fuzzy Query ===
    parentheses_fuzzy_query = build_parentheses_aware_fuzzy_query(user_query, 
                                                                  fields=["product_name", "search_field"], 
                                                                  boost=8)
    if parentheses_fuzzy_query:
        should_clauses.append(parentheses_fuzzy_query)
    
    # === Continue with existing query building logic ===
    # [Rest of your existing code continues here]
    
    # NEW: Detect smartphone-specific attributes
    smartphone_attributes = ["ram", "storage", "internal storage", "memory", "gb"]
    has_smartphone_attributes = any(attr in user_query.lower() for attr in smartphone_attributes)
    
    # Detect if this is a smartphone query based on brand + attributes
    is_smartphone_query = False
    if query_brand and has_smartphone_attributes:
        is_smartphone_query = True
        mapped_category = "smartphone"
    
    # NEW: Handle specific model queries to prevent category leakage
    is_specific_model_query = False
    SMARTPHONE_MODELS = {
        "pixel": ["google", "pixel"],
        "iphone": ["apple", "iphone"],
        "oneplus": ["oneplus"],
        "samsung": ["samsung", "galaxy"],
    }
    
    for brand, patterns in SMARTPHONE_MODELS.items():
        for pattern in patterns:
            if pattern in user_query.lower() and re.search(r'\d+', user_query):
                is_specific_model_query = True
                mapped_category = "smartphone"
                break
        if is_specific_model_query:
            break
    
    # NEW: Handle high bounce keywords with special logic
    is_high_bounce_keyword = False
    high_bounce_keywords = {
        "google mobile": "smartphone",
        "vivo v60": "smartphone",
        "iphone": "smartphone"
    }
    
    for keyword, category in high_bounce_keywords.items():
        if keyword in user_query.lower():
            is_high_bounce_keyword = True
            mapped_category = category
            break
    
    # Handle washing machine queries with more precision
    washing_machine_indicators = [
        "washing", "washing machine", "machine", "laundry", "kapde", "clothes",
    ]
    
    # Check if this is a washing machine query
    is_washing_machine_query = any(
        indicator in user_query.lower() 
        for indicator in washing_machine_indicators
    )
    
    # Check if this is a brand-specific washing machine query
    is_brand_washing_machine = is_washing_machine_query and query_brand
    
    if is_brand_washing_machine:
        # For brand-specific washing machine queries, we need to ensure both brand and category are preserved
        # Force category to washing machines
        must_clauses.append({"terms": {"actual_category": ["washing machine"]}})
        
        # Add brand-specific filter
        must_clauses.append({
            "bool": {
                "should": [
                    {"match_phrase": {"product_name": query_brand}},
                    {"match_phrase": {"search_field": query_brand}},
                    {"term": {"manufacturer_desc": query_brand}}
                ],
                "minimum_should_match": 1
            }
        })
        
        # Block unrelated categories
        must_not_clauses.extend([
            {"term": {"actual_category": "laptop"}},
            {"term": {"actual_category": "Washbasin"}},
            {"term": {"actual_category": "dishwasher"}},
            {"match": {"product_name": {"query": "Washbasin"}}},
            {"match": {"product_name": {"query": "laptop"}}},
            {"match": {"product_name": {"query": "dishwasher"}}},
        ])
        
        # Boost washing machine specific terms with brand
        should_clauses.extend([
            {"match": {"product_name": {"query": f"{query_brand} Washing Machine", "boost": 50}}},
            {"match": {"search_field": {"query": f"{query_brand} Washing Machine", "boost": 50}}},
            {"match": {"product_keywords": {"query": f"{query_brand} washing machine", "boost": 40}}},
            {"match": {"actual_category": {"query": "washing machine", "boost": 30}}},
            {"match": {"asset_category_name": {"query": "Washing Machine", "boost": 30}}},
            {"term": {"manufacturer_desc": {"value": query_brand, "boost": 60}}},
        ])
        
        # Also add general term matching
        for term in terms:
            should_clauses += [
                {"match": {"product_name": {"query": term, "boost": 10}}},
                {"match": {"search_field": {"query": term, "boost": 10}}},
                {"match": {"actual_category": {"query": term, "boost": 8}}},
                {"match": {"top_level_category_name": {"query": term, "boost": 5}}},
                {"match": {"product_keywords": {"query": term, "boost": 5}}},
                {"match_phrase_prefix": {"search_field": {"query": term, "boost": 7}}},
                {"match": {"search_field": {"query": term, "fuzziness": "AUTO", "boost": 5}}},
                {"match": {"products.sku": {"query": term, "boost": 5}}},
                {"match": {"products.name": {"query": term, "boost": 5}}},
            ]
    elif is_washing_machine_query:
        # For generic washing machine queries (without brand)
        # Force category to washing machines
        must_clauses.append({"terms": {"actual_category": ["washing machine"]}})
        
        # Block unrelated categories
        must_not_clauses.extend([
            {"term": {"actual_category": "laptop"}},
            {"term": {"actual_category": "Washbasin"}},
            {"term": {"actual_category": "dishwasher"}},
            {"match": {"product_name": {"query": "Washbasin"}}},
            {"match": {"product_name": {"query": "laptop"}}},
            {"match": {"product_name": {"query": "dishwasher"}}},
        ])
        
        # Boost washing machine specific terms
        should_clauses.extend([
            {"match": {"product_name": {"query": "Washing Machine", "boost": 30}}},
            {"match": {"search_field": {"query": "Washing Machine", "boost": 30}}},
            {"match": {"product_keywords": {"query": "washing machine", "boost": 10}}},
            {"match": {"actual_category": {"query": "washing machine", "boost": 15}}},
            {"match": {"asset_category_name": {"query": "Washing Machine", "boost": 15}}},
        ])
        
        # Also add general term matching
        for term in terms:
            should_clauses += [
                {"match": {"product_name": {"query": term, "boost": 5}}},
                {"match": {"search_field": {"query": term, "boost": 5}}},
                {"match": {"actual_category": {"query": term, "boost": 4}}},
                {"match": {"top_level_category_name": {"query": term, "boost": 2}}},
                {"match": {"product_keywords": {"query": term, "boost": 2}}},
                {"match_phrase_prefix": {"search_field": {"query": term, "boost": 3}}},
                {"match": {"search_field": {"query": term, "fuzziness": "AUTO", "boost": 2}}},
                {"match": {"products.sku": {"query": term, "boost": 2}}},
                {"match": {"products.name": {"query": term, "boost": 2}}},
            ]
    elif is_smartphone_query or is_specific_model_query or is_high_bounce_keyword:
        # Handle smartphone queries with attributes, specific models, or high bounce keywords
        # Force category to smartphones
        must_clauses.append({"terms": {"actual_category": ["smartphone"]}})
        
        # Block unrelated categories
        must_not_clauses.extend([
            {"term": {"actual_category": "ac"}},
            {"term": {"actual_category": "air conditioner"}},
            {"term": {"actual_category": "refrigerator"}},
            {"term": {"actual_category": "washing machine"}},
            {"term": {"actual_category": "tv"}},
            {"term": {"actual_category": "television"}},
            {"term": {"actual_category": "laptop"}},
        ])
        
        # Add brand-specific filter if brand is present
        if query_brand:
            must_clauses.append({
                "bool": {
                    "should": [
                        {"match_phrase": {"product_name": query_brand}},
                        {"match_phrase": {"search_field": query_brand}},
                        {"term": {"manufacturer_desc": query_brand}}
                    ],
                    "minimum_should_match": 1
                }
            })
            
            # Boost brand-specific smartphone terms
            should_clauses.extend([
                {"match": {"product_name": {"query": f"{query_brand} smartphone", "boost": 50}}},
                {"match": {"search_field": {"query": f"{query_brand} smartphone", "boost": 50}}},
                {"term": {"manufacturer_desc": {"value": query_brand, "boost": 60}}},
            ])
            
           # NEW: Special handling for Google Pixel queries to show latest models first
        
        
        
        # NEW: Special handling for Google Pixel queries to show latest models first
        if "google pixel" in user_query.lower():
            # Extract model number if present
            model_match = re.search(r'google pixel (\d+)', user_query.lower())
            if model_match:
                model_number = model_match.group(1)
                # Boost exact model match
                should_clauses.append({
                    "bool": {
                        "should": [
                            {"match_phrase": {"product_name": f"google pixel {model_number}"}},
                            {"match": {"model_id": f"google pixel {model_number}"}}
                        ],
                        "boost": 100
                    }
                })
            else:
                # For general "google mobile" query, boost latest models
                should_clauses.append({
                    "bool": {
                        "should": [
                            {"match_phrase": {"product_name": "google pixel 9", "boost": 100}},
                            {"match_phrase": {"product_name": "google pixel 8 pro", "boost": 90}},
                            {"match_phrase": {"product_name": "google pixel 8", "boost": 80}},
                            {"match_phrase": {"product_name": "google pixel 7 pro", "boost": 70}},
                            {"match_phrase": {"product_name": "google pixel 7", "boost": 60}}
                        ]
                    }
                })
        
        # NEW: Special handling for iPhone queries to show flagship phones from other brands
        if "iphone" in user_query.lower():
            # Block Apple products
            must_not_clauses.append({"term": {"manufacturer_desc": "Apple"}})
            
            # Boost non-Apple flagship phones (high price range)
            should_clauses.append({
                "bool": {
                    "must": [
                        {"range": {"mop": {"gte": 60000}}},
                        {"bool": {
                            "must_not": [
                                {"term": {"manufacturer_desc": "Apple"}}
                            ]
                        }}
                    ],
                    "boost": 50
                }
            })
        
        # Boost smartphone-specific attributes
        for attr in smartphone_attributes:
            if attr in user_query.lower():
                should_clauses.append({
                    "match": {
                        "product_name": {
                            "query": attr,
                            "boost": 40
                        }
                    }
                })
                should_clauses.append({
                    "match": {
                        "search_field": {
                            "query": attr,
                            "boost": 40
                        }
                    }
                })
        
        # Also add general term matching
        for term in terms:
            should_clauses += [
                {"match": {"product_name": {"query": term, "boost": 10}}},
                {"match": {"search_field": {"query": term, "boost": 10}}},
                {"match": {"actual_category": {"query": term, "boost": 8}}},
                {"match": {"top_level_category_name": {"query": term, "boost": 5}}},
                {"match": {"product_keywords": {"query": term, "boost": 5}}},
                {"match_phrase_prefix": {"search_field": {"query": term, "boost": 7}}},
                {"match": {"search_field": {"query": term, "fuzziness": "AUTO", "boost": 5}}},
                {"match": {"products.sku": {"query": term, "boost": 5}}},
                {"match": {"products.name": {"query": term, "boost": 5}}},
            ]
    else:
        # Normal query processing for non-smartphone, non-washing machine queries
        for term in terms:
            should_clauses += [
                {"match": {"product_name": {"query": term, "boost": 5}}},
                {"match": {"search_field": {"query": term, "boost": 5}}},
                {"match": {"actual_category": {"query": term, "boost": 4}}},
                {"match": {"top_level_category_name": {"query": term, "boost": 2}}},
                {"match": {"product_keywords": {"query": term, "boost": 2}}},
                {"match_phrase_prefix": {"search_field": {"query": term, "boost": 3}}},
                {"match": {"search_field": {"query": term, "fuzziness": "AUTO", "boost": 2}}},
                {"match": {"products.sku": {"query": term, "boost": 2}}},
                {"match": {"products.name": {"query": term, "boost": 2}}},
            ]
    
    # ------------- CATEGORY FALLBACK (FIXED) -------------
    if mapped_category and not is_washing_machine_query and not is_smartphone_query and not is_specific_model_query and not is_high_bounce_keyword:
        # FIXED: Use should clause instead of must for category fallback
        should_clauses += [
            {"term": {"actual_category": mapped_category}},
            {"match": {"actual_category": {"query": mapped_category, "boost": 4}}},
        ]
    
    parsed = decompose_query_for_boosting(user_query)
    brandlock_must, brandlock_must_not = patch_brand_token_must(parsed, user_query)
    must_clauses.extend(brandlock_must)
    must_not_clauses.extend(brandlock_must_not)
    
    brands = parsed["brands"]
    categories = parsed["categories"]
    attributes = parsed["attributes"]
    
    # Handle AC queries with brand specificity
    is_ac_query = "ac" in query_tokens or "air conditioner" in user_query.lower()
    
    if brands and is_ac_query:
        # For AC queries with brand, boost brand-specific AC products
        for brand in brands:
            # Strong boost for direct brand field match
            should_clauses.append({
                "bool": {
                    "must": [
                        {"term": {"actual_category": {"value": "air conditioner"}}},
                        {"term": {"manufacturer_desc": {"value": brand, "boost": 200}}}
                    ],
                    "boost": 200
                }
            })
            # Boost brand in product name
            should_clauses.append({
                "bool": {
                    "must": [
                        {"term": {"actual_category": {"value": "air conditioner"}}},
                        {"match_phrase": {"product_name": {"query": brand, "boost": 150}}}
                    ],
                    "boost": 150
                }
            })
    elif brands:
        
        # Normal brand boosting for non-AC queries
        # AFTER (term OR phrase on manufacturer_desc, plus existing name/search boosts)
        for brand in brands:
            # Canonicalize brand if an alias exists
            brand = BRAND_ALIASES.get(brand.lower(), brand)

            should_clauses.append({
                "bool": {
                    "should": [
                        {"term": {"manufacturer_desc": brand}},
                        {"match_phrase": {"manufacturer_desc": brand}}
                    ],
                    "minimum_should_match": 1,
                    "boost": 200
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "product_name": {
                        "query": brand,
                        "boost": 150
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "search_field": {
                        "query": brand,
                        "boost": 120
                    }
                }
            })

    
    if categories and not is_washing_machine_query and not is_smartphone_query and not is_specific_model_query and not is_high_bounce_keyword:
        for cat in categories:    
            should_clauses.append({
                "term": {
                    "actual_category": {
                        "value": cat,
                        "boost": 100
                    }
                }
            })
            # Boost if category term is in product_name or search_field
            should_clauses.append({
                "match_phrase": {
                    "product_name": {
                        "query": cat,
                        "boost": 70
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "search_field": {
                        "query": cat,
                        "boost": 50
                    }
                }
            })
    
    if attributes.get("color"):
        for color in attributes["color"]:
            # Boost on direct color attribute
            should_clauses.append({
                "term": {
                    "attribute_color": {
                        "value": color,
                        "boost": 80
                    }
                }
            })
            # Boost in product_name/search_field
            should_clauses.append({
                "match_phrase": {
                    "product_name": {
                        "query": color,
                        "boost": 60
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "search_field": {
                        "query": color,
                        "boost": 50
                    }
                }
            })
    
    if attributes.get("storage"):
        for storage in attributes["storage"]:
            should_clauses.append({
                "term": {
                    "attribute_internal_storage": {
                        "value": storage,
                        "boost": 80
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "product_name": {
                        "query": storage,
                        "boost": 60
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "search_field": {
                        "query": storage,
                        "boost": 50
                    }
                }
            })
    
    if attributes.get("ram"):
        for ram in attributes["ram"]:
            should_clauses.append({
                "term": {
                    "attribute_ram": {
                        "value": ram,
                        "boost": 80
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "product_name": {
                        "query": ram,
                        "boost": 60
                    }
                }
            })
            should_clauses.append({
                "match_phrase": {
                    "search_field": {
                        "query": ram,
                        "boost": 50
                    }
                }
            })
    
    # ---------- CATEGORY LOCKING ----------
    CATEGORY_LOCK_SYNS = {
        "car": set([
            "car", "cars", "new car", "new cars", "for wheeler", "four wheeler", "four-wheeler", "4 wheeler",
            "skoda", "maruti suzuki", "kia", "volkwagen", "hyundai", "bmw", "marcedes", "4wheeler", "sedan", "hatchback", "suv"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("new cars", [])]),
        "smartphone": set([
            "smartphone", "mobile", "mobiles", "phone", "phones", "cellphone", "cell phone"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("mobile phones", [])]),
        "television": set([
            "television", "smart tv", "smart led tv", "tv", "led tv", "smart tv"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("tv and home entertainment", [])]),
        "laptop": set([
            "laptop", "laptops", "notebook", "ultrabook", "chromebook", "macbook"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("laptops", [])]),
        "tractor": set([
            "tractor", "tractors", "farm tractor", "agriculture tractor"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("tractor", [])]),
        "refrigerator": set([
            "double door refrigerator", "single door refrigerator", "single door fridge",  "double door fridge",
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("refrigerators", [])]),
        "ac": set([
            "ac", "air conditioner", "airconditioner", "split ac", "window ac"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("ac", [])]),
        "two-wheeler": set([
            "two wheeler", "two-wheeler", "twowheeler", "two-wheelers", "2 wheeler", "2wheeler"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("two-wheeler", [])]),
        "washing machines": set([
            "washing machine", "washing machines", "Washing Machine", "wahing", "kapde machine",
            "laundry machine", "clothes washer", "front load washer", "top load washer", "washingmachine"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("washing machines", [])]),
        "tablets": set([
            "tab", "tablate", "tablet"
        ] + [normalize(x) for x in BUSINESS_SYNONYMS.get("tablets", [])]),
        
       
    }
    
    is_fridge_query = {
        "refrigerator" in user_query.lower() or "fridge" in user_query.lower() or 
        (mapped_category in {"refrigerator", "refrigerators", "single door refrigerator", "double door refrigerator"}) or
        ("refrigerator" in [c.lower() for c in parsed.get("categories",[])]) or 
        ("fridge" in [c.lower() for c in parsed.get("categories",[])])
    
    }   
    
    if is_fridge_query:
        must_not_clauses.extend(deep_freezer_must_not_clauses())
        
    
    
    category_locked = False
    lock_category = None
    qn = normalize(user_query)
    q_tokens = set(qn.split())
    
    # --- HARD CATEGORY LOCK FOR INVERTER/BACKUP QUERIES ---
    MOBILE_BLOCK_TERMS = [
        "blynk", "refurbished", "iphone xs", "iphone xs 64", "xs", "iphone xs max"
    ]
    
    if is_mobile_query(user_query):
        for block_term in MOBILE_BLOCK_TERMS:
            must_not_clauses.append({"match_phrase": {"product_name": block_term}})
            must_not_clauses.append({"match_phrase": {"manufacturer_desc": block_term}})
            must_not_clauses.append({"match_phrase": {"actual_category": block_term}})
    
    # 1. Define all user queries that should be "inverter" only:
    INVERTER_SYNONYMS = {
        "inverter", "inverters", "backup", "battery backup", "power backup",
        "inverter battery", "home inverter", "power inverter"
    }
    
    user_query_norm = normalize(user_query)
    if user_query_norm in INVERTER_SYNONYMS:
        # Remove other category locks, enforce inverter
        must_clauses = [cl for cl in must_clauses if cl.get("term", {}).get("actual_category") == "inverter"]
        must_clauses.append({"term": {"actual_category": "inverter"}})
        # Strongly exclude speakers, plates, power flash, emergency light, etc.
        must_not_clauses.extend([
            {"term": {"actual_category": "ac"}},
            {"term": {"actual_category": "air conditioner"}},
            {"match": {"product_name": "ac"}},
            {"match": {"product_name": "split ac"}},
            {"match": {"product_name": "speaker"}},
            {"match": {"product_name": "speakers"}},
            {"match": {"product_name": "built in battery"}},
            {"match": {"product_name": "battery plate"}},
            {"match": {"product_name": "battery plates"}},
            {"match": {"product_name": "power flash"}},
            {"match": {"product_name": "emergency light"}},
            {"match": {"product_name": "emergency bulb"}},
            {"match": {"product_name": "power bulb"}},
            {"match": {"product_name": "flashlight"}},
            {"match": {"product_name": "torch"}}
        ])
        
    # NEW: When Apple/Blynk is in query, block them and boost premium alternatives
    if any(x in user_query.lower() for x in ["apple","iphone","ios","blynk"]):
        must_clauses.append({"terms": {"actual_category": ["smartphone"]}})
        must_not_clauses.extend([
            {"term": {"manufacturer_desc": "Apple"}},
            {"term": {"manufacturer_desc": "Blynk"}},
            {"match_phrase": {"product_name": "Apple"}},
            {"match_phrase": {"product_name": "iPhone"}},
            {"match_phrase": {"product_name": "iOS"}},
            {"match_phrase": {"product_name": "Blynk"}},
        ])
        # Prefer high-price non-Apple/Blynk, especially Samsung flagships
        should_clauses.extend([
            {"bool": {
                "must": [{"range": {"mop": {"gte": 70000}}}],
                "must_not": [{"terms": {"manufacturer_desc": ["Apple","Blynk"]}}],
                "boost": 80
            }},
            {"term": {"manufacturer_desc": {"value": "Samsung", "boost": 120}}},
            # gentle nudges for other premium brands if present
            {"term": {"manufacturer_desc": {"value": "Google", "boost": 60}}},
            {"term": {"manufacturer_desc": {"value": "OnePlus", "boost": 60}}},
            {"term": {"manufacturer_desc": {"value": "Nothing", "boost": 40}}}
        ])
    
    # Skip category locking for special queries that we already handled
    if not is_washing_machine_query and not is_smartphone_query and not is_specific_model_query and not is_high_bounce_keyword:
        for cat, syn_set in CATEGORY_LOCK_SYNS.items():
            if qn in syn_set or q_tokens & syn_set:
                must_clauses.append({"terms": {"actual_category": list(syn_set)}})
                category_locked = True
                lock_category = cat
                break
            
        CAR_SYNS = CATEGORY_LOCK_SYNS.get("car", set())
        if (qn in CAR_SYNS or q_tokens & CAR_SYNS) and not category_locked:
            must_clauses.append({"term": {"actual_category": ["new cars","car", "cars"]}})
            category_locked = True
            lock_category = "car"
            # Exclude accessories, tyres, coolers, etc.
            must_not_clauses.extend([
                {"term": {"actual_category": "tyre"}},
                {"term": {"actual_category": "ac"}},
                {"term": {"actual_category": "car accessory"}},
                {"term": {"actual_category": "car cooler"}},
                {"match": {"product_name": "tyre"}},
                {"match": {"product_name": "suzuki access"}},
                {"match": {"product_name": "suzuki V-Strom"}},
                {"match": {"product_name": "accessory"}},
                {"match": {"product_name": "cooler"}},
            ])
            
        # PATCH: STRICT CATEGORY LOCKING for "washing machine" (covers variants)
        WASHING_MACHINE_CATEGORIES = [
            "washing machine", "Washing Machine", "washing machines", "WASHING_MACHINE", "WASHING_MACHINES"
        ]
        
        if not category_locked:
            if (
                qn in CATEGORY_LOCK_SYNS["washing machines"]
                or any(word in CATEGORY_LOCK_SYNS["washing machines"] for word in q_tokens)
                or "washing machine" in user_query.lower()
                or "washing machines" in user_query.lower()
            ):
                must_clauses.append({"terms": {"actual_category": WASHING_MACHINE_CATEGORIES}})
                category_locked = True
                lock_category = "washing machines"
                
                # --- Exclude laptops, washbasins and noisy categories ---
                must_not_clauses.extend([
                    {"term": {"actual_category": "Laptop"}},
                    {"term": {"actual_category": "Washbasin"}},
                    {"match": {"product_name": "Washbasin"}},
                ])
                
        if not category_locked:
            if qn in CATEGORY_LOCK_SYNS["tractor"] or any(word in CATEGORY_LOCK_SYNS["tractor"] for word in q_tokens):
                must_clauses.append({"term": {"actual_category": "tractor"}})
                category_locked = True
                lock_category = "tractor"
                
 
                
        if not category_locked:
            if qn in CATEGORY_LOCK_SYNS["smartphone"] or any(word in CATEGORY_LOCK_SYNS["smartphone"] for word in q_tokens):
                must_clauses.append({"term": {"actual_category": "smartphone"}})
                category_locked = True
                lock_category = "smartphone"
                
        if not category_locked:
            if qn in CATEGORY_LOCK_SYNS["tablets"] or any(word in CATEGORY_LOCK_SYNS["tablets"] for word in q_tokens):
                must_clauses.append({"term": {"actual_category": "tablets"}})
                category_locked = True
                lock_category = "tablets"
                
        # if not category_locked:
        #     if qn in CATEGORY_LOCK_SYNS["audio & video"] or any(word in CATEGORY_LOCK_SYNS["audio & video"] for word in q_tokens):
        #         must_clauses.append({"term": {"actual_category": "audio & video"}})
        #         category_locked = True
        #         lock_category = "audio & video"
                
        q_scoot = {"scooter", "scooty", "activa", "jupiter", "vespa", "burgman", "maestro", "grazia", "access", "ntorq"}
        if any(word in q_tokens for word in q_scoot):
            must_clauses[:] = [cl for cl in must_clauses if cl.get("term", {}).get("actual_category") == "two wheeler"]
            if not any(cl.get("term", {}).get("actual_category") == "two wheeler" for cl in must_clauses):
                must_clauses.append({"term": {"actual_category": "two wheeler"}})
            must_not_clauses.extend([
                {"term": {"actual_category": "air cooler"}},
                {"term": {"actual_category": "cooler"}},
                {"term": {"actual_category": "water heater"}},
                {"match_phrase": {"product_name": "cooler"}},
                {"match_phrase": {"product_name": "air cooler"}},
                {"match_phrase": {"product_name": "water heater"}},
                {"match_phrase": {"manufacturer_desc": "HERO"}},
                {"match_phrase": {"product_name": "Bajaj Pulsar"}},
                {"match_phrase": {"product_name": "TVS Radeon"}},
                {"match_phrase": {"product_name": "Hero Splendor Plus"}}
            ])
            
    user_query_n = normalize(user_query)
    def is_brand_smartphone_query(user_query_n, brands):
        tokens = user_query_n.split()
        for brand in brands:
            if (
                brand in user_query_n and (
                    len(tokens) == 1 or
                    (len(tokens) == 2 and tokens[0] == brand and tokens[1] in {"mobile", "phone", "smartphone"}) or
                    (len(tokens) == 2 and tokens[1] == brand and tokens[0] in {"mobile", "phone", "smartphone"})
                )
            ):
                return brand
            if brand == "samsung" and ("galaxy" in tokens or "galaxy" in user_query_n):
                return brand
        return None
    
    brand_matched = is_brand_smartphone_query(user_query_n, MOBILE_BRANDS)
    if brand_matched:
        should_clauses.append({
            "bool": {
                "must": [
                    {"term": {"actual_category": {"value": "smartphone"}}},
                    {"match": {"search_field": {"query": "samsung galaxy", "boost": 20}}},
                ],
                "boost": 200
            }
        })
        should_clauses.append({
            "term": {"actual_category": {"value": "smartphone", "boost": 120}}
        })
        should_clauses.append({
            "term": {"manufacturer_desc": {"value": brand_matched, "boost": 120}}
        })
    
    # ---------- USUAL BOOSTING LOGIC ----------
    for cat in parsed["categories"]:
        should_clauses.append({
            "term": {"actual_category": {"value": cat, "boost": 15}}
        })
        should_clauses.append({
            "term": {"category_type": {"value": cat, "boost": 6}}
        })
        should_clauses.append({
            "term": {"asset_category_name": {"value": cat, "boost": 6}}
        })
    
    for brand in parsed["brands"]:
        should_clauses.append({
            "term": {"manufacturer_desc": {"value": brand, "boost": 10}}
        })
    
    for attr, vals in parsed["attributes"].items():
        for val in vals:
            should_clauses.append({
                "match": {"product_name": {"query": val, "boost": 6}}
            })
            should_clauses.append({
                "match": {"search_field": {"query": val, "boost": 5}}
            })
    
    if len(parsed["tokens"]) > 2:
        should_clauses.append({
            "match": {
                "product_name": {
                    "query": " ".join(parsed["tokens"]),
                    "operator": "and",
                    "boost": 10
                }
            }
        })
    
    # ------------- SPECIAL CASE HANDLING (existing logic) -------------
    all_mobiles_syns = set(BUSINESS_SYNONYMS.get("mobiles", [])) | set([
        "mobile phones", "mobiles", "smartphone", "phone", "phones", "mobilephone", "cellphone", "cell phone"
    ])
    
    user_query_n = normalize(user_query)
    user_query_ns = user_query_n.replace(" ", "")
    
    # ----- PATCHED: Robust scooter/twowheeler matching -----
    SCOOTER_SYNONYMS_SET = {
        "scooter", "scooters", "scooty", "activa", "jupiter", "vespa", "burgman", "maestro", "grazia", "access", "ntorq"
    }
    
    if any(word in user_query_n.split() for word in SCOOTER_SYNONYMS_SET):
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        # Block air coolers/cooler/fuzzy matches:
        must_not_clauses.extend([
            {"term": {"actual_category": "air cooler"}},
            {"term": {"actual_category": "cooler"}},
            {"term": {"actual_category": "water heater"}},
            {"match_phrase": {"product_name": "cooler"}},
            {"match_phrase": {"product_name": "air cooler"}},
            {"match_phrase": {"product_name": "water heater"}},
            {"match_phrase": {"manufacturer_desc": "HERO"}},
            {"match_phrase": {"product_name": "Hero Splendor Plus"}},
            {"match_phrase": {"product_name": "Honda SP"}},
            {"match_phrase": {"product_name": "TVS"}},
        ])
        should_clauses.extend([
            {"match": {"product_name": {"query": "activa", "boost": 12}}},
            {"match": {"product_name": {"query": "electric scooter", "boost": 12}}},
            {"match": {"product_name": {"query": "tvs", "boost": 15}}},
            {"match": {"manufacturer_desc": {"query": "TVS", "boost": 12}}},
            {"match": {"manufacturer_desc": {"query": "honda activa", "boost": 12}}},
            {"match": {"product_name": {"query": "scooty", "boost": 11}}},
            {"match": {"product_name": {"query": "vespa", "boost": 10}}},
            {"match": {"product_name": {"query": "burgman", "boost": 10}}},
            {"match": {"product_name": {"query": "maestro", "boost": 10}}},
            {"match": {"product_name": {"query": "ntorq", "boost": 10}}},
            {"match": {"product_name": {"query": "grazia", "boost": 10}}},
            {"match": {"product_name": {"query": "access", "boost": 10}}},
        ])
        
    elif user_query_n in {"iqube", "tvs activa", "TVS", "TVS activa", "Tvs activa", "tvs"}:
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "splendor"}},
            {"match_phrase": {"product_name": "hero splendor"}},
            {"match_phrase": {"product_name": "Honda activa"}},
            {"term": {"actual_category": "air cooler"}},
            {"term": {"actual_category": "water heater"}},
        ])
        should_clauses.extend([
            {"match": {"product_name": {"query": "tvs", "boost": 20}}},
            {"match": {"product_name": {"query": "iqube", "boost": 12}}},            
            {"match": {"product_name": {"query": "electric scooter", "boost": 10}}},
            {"match": {"manufacturer_desc": {"query": "TVS", "boost": 10}}},
            {"match": {"manufacturer_desc": {"query": "TVS", "boost": 10}}},
        ])
        
    elif user_query_n in {"tvs bike", "tvs motorcycle"}:
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "splendor"}},
            {"match_phrase": {"product_name": "hero splendor"}},
            {"match_phrase": {"product_name": "Honda activa"}},
            {"term": {"actual_category": "air cooler"}},
            {"term": {"actual_category": "water heater"}},
        ])
        should_clauses.extend([
            {"match": {"product_name": {"query": "tvs redeon", "boost": 20}}},
            {"match": {"product_name": {"query": "tvs", "boost": 12}}},            
            {"match": {"manufacturer_desc": {"query": "TVS", "boost": 10}}},
            {"match": {"manufacturer_desc": {"query": "TVS", "boost": 10}}},
        ])
        
    elif user_query_n in {"royal", "royalenfield", "royal enfield", "enfield"}:
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match_phrase": {"search_field": "hero"}},
            {"match_phrase": {"search_field": "honda"}},
            {"match_phrase": {"product_name": "activa"}},
        ])
        should_clauses.extend([
            {"match_phrase": {"search_field": "royal enfield"}},
            {"match_phrase": {"search_field": "enfield"}},
            {"match_phrase": {"product_name": "royal"}},
        ])
        
    elif user_query_n in {"scooty", "scoty", "activa"}:
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "splendor"}},
            {"match_phrase": {"product_name": "hero splendor"}},
            {"match_phrase": {"product_name": "bike"}},
            {"term": {"actual_category": "air cooler"}},
            {"term": {"actual_category": "water heater"}},
        ])
        should_clauses.extend([
            {"match": {"product_name": {"query": "activa", "boost": 12}}},
            {"match": {"product_name": {"query": "electric scooter", "boost": 10}}},
            {"match": {"manufacturer_desc": {"query": "tvs jupiter", "boost": 10}}},
        ])
        
    elif user_query_n in {"honda", "honda bike", "honda bick", "honda"}:
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "scooter"}},
            {"match_phrase": {"product_name": "gym bike"}},
            {"match_phrase": {"product_name": "activa"}},
            {"match_phrase": {"product_name": "scooty"}},
            {"match_phrase": {"product_name": "AC"}},
            {"match_phrase": {"product_name": "air puridier"}},
            {"match_phrase": {"product_name": "pleasure plus"}},
            {"match_phrase": {"product_name": "bajaj"}},
            {"match_phrase": {"product_name": "hero"}},
            {"match_phrase": {"product_name": "tvs"}},
            {"match": {"product_name": {"query": "royal enfield", "boost": 8}}},
        ])
        should_clauses.extend([
            {"match": {"product_name": {"query": "Honda dio", "boost": 12}}},
            {"match": {"product_name": {"query": "honda activa", "boost": 12}}},
            {"match": {"product_name": {"query": "Honda unicorn", "boost": 12}}},
            {"match": {"manufacturer_desc": {"query": "Honda", "boost": 12}}},
        ])
        
    elif user_query_n in {"bike", "hero bike", "hero", "hero bick"}:
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "scooter"}},
            {"match_phrase": {"product_name": "activa"}},
            {"match_phrase": {"product_name": "scooty"}},
            {"match_phrase": {"product_name": "honda"}},
            {"match_phrase": {"product_name": "AC"}},
            {"match_phrase": {"product_name": "air puridier"}},
            {"match_phrase": {"product_name": "pleasure plus"}},
        ])
        should_clauses.extend([
            {"match": {"product_name": {"query": "hero", "boost": 12}}},
            {"match": {"product_name": {"query": "hero splendor", "boost": 12}}},
            {"match": {"product_name": {"query": "hero bike", "boost": 10}}},
            {"match": {"manufacturer_desc": {"query": "hero", "boost": 12}}},
            {"match": {"product_name": {"query": "hero splendor", "boost": 15}}},
        ])
        
    if user_query_n in {"air purifier", "airpurifier", "air-purifier"}:
        must_clauses.append({"term": {"actual_category": "air purifier"}})
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "water purifier"}},
            {"match_phrase": {"actual_category": "water purifier"}},
        ])
        
    # --- OnePlus model strict category lock (prevents OnePlus TV leakage) ---
    if re.search(r'\boneplus\s*\d+[a-z]*\b', user_query.lower()) or re.search(r'\boneplus\d+[a-z]*\b', user_query.lower()):
        mapped_category = "smartphone"
        must_clauses.append({"terms": {"actual_category": ["smartphone"]}})
        must_not_clauses.extend([
            {"term": {"actual_category": "tv"}},
            {"term": {"actual_category": "television"}},
            {"term": {"actual_category": "tv and home entertainment"}}
        ])
        
   
        
        
    if user_query_n in {"mop", "Mop", "mop cleaner", "vacuum cleaner","cleaner"}:
        must_clauses.append({"term": {"actual_category": "vacuum cleaner"}})
        should_clauses.extend([
            {"match": {"product_name": {"query": "cleaner", "boost": 12}}},
            {"match": {"product_name": {"query": "cleaner", "boost": 12}}}
        ])
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "burner"}},
            {"match_phrase": {"product_name": "royal"}},
            {"match_phrase": {"product_name": "Top"}},
            {"match_phrase": {"product_name": "Freezer"}},
            {"match_phrase": {"actual_category": "two wheeler"}},
            {"match_phrase": {"actual_category": "washing machine"}},
            {"match_phrase": {"actual_category": "cooler"}},
        ])
    
    if user_query_n in {"goole", "pixel", "pixel phone", "google phone", "google pixel", "google pixle", "gogle pixal"}:
        must_clauses.append({"term": {"actual_category": "smartphone"}})
        must_clauses.append({"term": {"manufacturer_desc": "Google"}})
        should_clauses.extend([
            {"match": {"product_name": {"query": "google pixel", "boost": 12}}}
        ])
        must_not_clauses.extend([
            {"match_phrase": {"product_name": "samsung"}},
            {"match_phrase": {"product_name": "blynk"}}])
        
    if user_query_n in {"samsung"}:
        must_clauses.extend([
            {"match": {"product_name": "samsung AC"}},
            {"match": {"product_name": "samsung refrigerator"}},
            {"match": {"product_name": "samsung phone"}},
            {"match": {"product_name": "samsung washing machine"}}
        ])

        must_not_clauses.append({"term": {"actual_category": "car"}})
          
       

        
    import re
    nothing_brand_pattern = re.compile(r"\bnothing\b", re.IGNORECASE)
    phone_pattern = re.compile(r"\bphone\b", re.IGNORECASE)
    if (
        ("nothing" in user_query_n and "phone" in user_query_n)
        or re.search(r"\bnothing\s?\d*\b", user_query_n)
        or user_query_n in {"nothing", "nothing smartphone"}
    ):
        must_clauses.append({"term": {"actual_category": "smartphone"}})
        must_clauses.append({"term": {"manufacturer_desc": "Nothing"}})
        should_clauses.extend([
            {"match": {"product_name": {"query": "nothing phone", "boost": 30}}},
            {"match": {"product_name": {"query": "nothing", "boost": 12}}},
            {"match": {"search_field": {"query": "nothing phone", "boost": 10}}},
        ])
        must_not_clauses.extend([
            {"term": {"manufacturer_desc": b}} for b in BRANDS if b != "nothing"
        ])
        
    if (
        ("tvs" in user_query_n and "activa" in user_query_n)
        or re.search(r"\btvs\s?\d*\b", user_query_n)
        or user_query_n in {"tvs bike", "tvs", "tvs scooty"}
    ):
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_clauses.append({"term": {"manufacturer_desc": "TVS"}})
        should_clauses.extend([
            {"match": {"product_name": {"query": "tvs", "boost": 30}}},
            {"match": {"product_name": {"query": "tvs activa", "boost": 12}}},
            {"match": {"search_field": {"query": "activa", "boost": 10}}},
        ])
        
    # if (
    #     ("maruti" in user_query_n or "suzuki" in user_query_n)
    #     or re.search(r"\brezza\s?\d*\b", user_query_n)
    #     or user_query_n in {"ciaz"}
    # ):
    #     must_clauses.append({"term": {"top_level_category_name": "new cars"}})
    #     must_clauses.append({"term": {"manufacturer_desc": "maruti suzuki"}})
    #     should_clauses.extend([
    #     {"match": {"product_name": {"query": "maruti ", "boost": 30}}},
    #     # {"match": {"product_name": {"query": "brezza", "boost": 12}}},
    #     {"match": {"search_field": {"query": "maruti", "boost": 10}}},
    #     ])
        
       
        
    if (
        ("smartphone" in user_query_n and "phone" in user_query_n)
        or re.search(r"\bsmartphone\s?\d*\b", user_query_n) or 
        re.search(r"\bphone\s?\d*\b", user_query_n)
        or user_query_n in {"mobile", "smartphone", "phone", "mobiles", "phones"}
    ):
        must_clauses.append({"term": {"actual_category": "smartphone"}})
        must_not_clauses.extend([
            {"match": {"product_name": {"query": "iPhone", "boost": 30}}},
            {"match": {"product_name": {"query": "Blynk", "boost": 30}}},
            {"match": {"product_name": {"query": "iphone", "boost": 12}}},
            {"match": {"manufacturer_desc": {"query": "Apple", "boost": 10}}},
            {"match": {"manufacturer_desc": {"query": "Blynk", "boost": 10}}},
        ])
        
    if (
        ("bike" in user_query_n and "motorcycle" in user_query_n)
        or re.search(r"\bbike\s?\d*\b", user_query_n) or 
        re.search(r"\bmotorcycle\s?\d*\b", user_query_n) or re.search(r"\bmotor\s?\d*\b", user_query_n)
        or user_query_n in {"motorcycle", "bike", "two wheeler", "motor cycle"}
    ):
        must_clauses.append({"term": {"actual_category": "two wheeler"}})
        must_not_clauses.extend([
            {"match": {"product_name": {"query": "FitX bike", "boost": 30}}},
            {"match": {"product_name": {"query": "mountain bike", "boost": 30}}},
            {"match": {"product_name": {"query": "gym bike", "boost": 12}}},
            {"match": {"product_name": {"query": "activa", "boost": 12}}},
            {"match": {"product_name": {"query": "sports activa", "boost": 12}}},
        ])
    # ------------- MAIN CATEGORY/SYNONYM BLOCK -------------
    is_mobile = user_query_n in all_mobiles_syns or user_query_ns in all_mobiles_syns
    if is_mobile:
        brand_boosts = [
            ("Samsung", 120),
            ("Oppo", 100),
            ("Vivo", 90),
            ("Realme", 80),
            ("Redmi", 60),
            ("Nothing", 50),
        ]
        for brand, boost in brand_boosts:
            should_clauses.append({
                "term": {"manufacturer_desc": {"value": brand, "boost": boost}}
            })
        must_clauses.append({"term": {"actual_category": "smartphone"}})
    elif user_query_n in set(TWOWHEELER_AUTOSUGGEST + BUSINESS_SYNONYMS["two-wheeler"] + ["two wheeler", "two-wheeler", "twowheeler"]) or user_query_ns in set(TWOWHEELER_AUTOSUGGEST + BUSINESS_SYNONYMS["two-wheeler"]):
        category_boosts.append({"term": {"actual_category": {"value": "two wheeler", "boost": 16}}})
    elif user_query_n in set(BUSINESS_SYNONYMS["new cars"] + ["four wheeler", "four-wheeler", "for wheeler", "fourwheeler", "car", "cars"]) or user_query_ns in set(BUSINESS_SYNONYMS["new cars"]):
        category_boosts.append({"term": {"actual_category": {"value": "new cars", "boost": 10}}})
        should_clauses.extend([
            {"match": {"product_name": {"query": "car", "boost": 30}}},
            {"match": {"product_name": {"query": "skoda sedan", "boost": 12}}},
            {"match": {"product_name": {"query": "skoda sedan", "boost": 12}}},
            {"match": {"product_name": {"query": "volkwagen suv", "boost": 12}}},
            {"match": {"product_name": {"query": "maruti hatchback", "boost": 10}}},
            {"match": {"search_field": {"query": "car", "boost": 15}}},
        ])
        
    # ------------- FILTER CLAUSES (FLAT - CORRECT) -------------
    if filters:
        for entry in filters:
            if entry == "emi":
                continue
            values = [v.strip() for v in str(filters[entry]).split(",") if v.strip()]
            if not values:
                continue
            filter_clauses.append({"terms": {entry: values}})
    # ------------- PRICE FILTER -------------
    if price_filter:
        for field, rng in price_filter.items():
            filter_clauses.append({"range": {field: rng}})
            
    # FIXED: EMI filter should be applied to the nested city_offers field
    if emi_range:
        filter_clauses.append({
            "nested": {
                "path": "city_offers",
                "query": {
                    "range": {"city_offers.lowest_emi": emi_range}
                }
            }
        })
        
    must_not_clauses.append({
    "bool": {
        "must": [
            {
                "bool": {
                    "should": [
                        {"term": {"manufacturer_desc": "blynk"}},
                        {"term": {"manufacturer_desc": "Blynk"}},
                        {"match": {"product_name": "blynk"}},
                        {"match": {"product_name": "Blynk"}}
                    ],
                    "minimum_should_match": 1
                }
            },
            {"match": {"product_name": "refurbished"}},
            {"match": {"product_name": "iphone"}}
        ]
    }
})
    # ------------- FINAL QUERY -------------
    es_query = {
        "bool": {
            "filter": filter_clauses,
            "must": must_clauses,
            "must_not": must_not_clauses,
            "should": should_clauses + category_boosts,
            # FIXED: Set minimum_should_match to 0 if there are no should clauses
            "minimum_should_match": 1 if (should_clauses + category_boosts) else 0
        }
    }
    
    # ------- Optional: Log for debug -------
    import json
    print("\n[DEBUG] FINAL ES QUERY BODY:\n", json.dumps(es_query, indent=2), "\n")
    return es_query
# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
def resolve_query_and_filters(data, es=None, mapped_category_in=None):
    """
    Combines user query and chip (if present) for robust search and filter logic.
    Handles canonical category mapping, chip-to-attribute mapping, SKU/variant mapping, and edge/corner flexis.
    """
    query = data.get("query", "").strip()
    chip = data.get("chip", "").strip()
    base_category = data.get("base_category", "").strip()
    filters = data.get("filters", {}).copy()
    mapped_category = mapped_category_in  # Use if already set, else None
    # Normalize everything for robustness
    base_cat_norm = normalize(base_category)
    query_norm = normalize(query)
    chip_norm = normalize(chip)
    # --- 1. Canonical mapping using synonyms and canonical dict ---
    def resolve_canonical(q):
        qn = normalize(q)
        # Try BUSINESS_SYNONYMS first
        for canon, syns in BUSINESS_SYNONYMS.items():
            if qn == normalize(canon) or qn in [normalize(s) for s in syns]:
                return canon
        # Try canonical mapping direct
        for k in CATEGORY_CANONICAL.keys():
            if qn == normalize(k) or qn == normalize(CATEGORY_CANONICAL[k]):
                return k
        return None
    
    mapped_category = mapped_category_in
    if base_category:
        mapped_category = resolve_canonical(base_category)
    if not mapped_category and query:
        mapped_category = resolve_canonical(query)
    
    # --- 2. Chip-to-attribute mapping for all supported categories ---
    category_attribute_map = {
        # Mobiles & Smartphone
        "mobile phones": "attribute_internal_storage",
        "smartphone": "attribute_internal_storage",
        # Refrigerators
        "refrigerators": "attribute_capacity_litres",
        "refrigerator": "attribute_capacity_litres",
        # AC & Air Conditioner
        "ac": "attribute_capacity_in_tons",
        "air conditioner": "attribute_capacity_in_tons",
        # Two-Wheeler (UPDATED)
        "two-wheeler": "attribute_engine_capacity_new",
        "twowheeler": "attribute_engine_capacity_new",
        "bike": "attribute_engine_capacity_new",
        "scooter": "attribute_engine_capacity_new",
        # New Cars (UPDATED)
        "new cars": "attribute_engine_capacity_4w",
        "car": "attribute_engine_capacity_4w",
        "cars": "attribute_engine_capacity_4w",
        # Laptops
        "laptops": "attribute_storage_size",
        "laptop": "attribute_storage_size",
        # SSD capacity (add if you want to support chips for SSD specifically)
        "laptop_ssd": "attribute_ssd",
        # Washing Machines (UPDATED)
        "washing machines": "attribute_capacity_wm",
        "washing machine": "attribute_capacity_wm",
        # TV & Home Entertainment
        "tv and home entertainment": "attribute_screen_size_in_inches",
        "television": "attribute_screen_size_in_inches",
        # Audio & Video
        "audio & video": "attribute_speaker_weight",
        "audio and video": "attribute_speaker_weight",
        # Tractor
        "tractor": "attribute_engine_capacity",
        # Air Coolers (NEW)
        "air coolers": "attribute_capacity_air_cooler",
        "air cooler": "attribute_capacity_air_cooler",
        # Tablets (NEW)
        "tablets": "attribute_primary_camera_new",
        "tablet": "attribute_primary_camera_new",
        # Air Purifiers (NEW)
        "air purifiers": "attribute_suitable_for",
        "air purifier": "attribute_suitable_for",
        # Printers (NEW)
        "printers": "attribute_duplex_printing_new",
        "printer": "attribute_duplex_printing_new",
        # Desktop & Monitor (NEW)
        "desktop monitor": "attribute_screen_size_new",
        "monitor": "attribute_screen_size_new",
        # Furniture (left unchanged, no chip field in your data)
        "furniture": "attribute_type2",
    }
    
    # Dynamic support for chip-to-attribute in all known chip-enabled categories
    if chip and mapped_category:
        attr_key = category_attribute_map.get(mapped_category)
        if not attr_key:
            # Fallback: check if mapped_category is in chips and try to infer
            if mapped_category in CATEGORY_HARDCODED_CHIPS:
                chips, _ = CATEGORY_HARDCODED_CHIPS[mapped_category]
                if chips:
                    attr_key = f"attribute_{normalize(mapped_category).replace(' ', '_')}"
        if attr_key:
            filters[attr_key] = chip
        else:
            # Optional: log missing mapping for dev review
            pass  # No-op for future proofing
    
    # --- 3. Chip as SKU/variant/color-level filter (optional advanced case) ---
    if chip and es is not None and mapped_category:
        sku_match_attr = ["color", "variant", "sku", "name"]
        # Heuristic: only attempt ES lookup for relevant chip size (avoid single-char chips, etc.)
        if len(chip_norm) > 1:
            try:
                query_body = {
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"actual_category": mapped_category}},
                                {"multi_match": {"query": chip, "fields": [f"products.{field}" for field in sku_match_attr]}}
                            ]
                        }
                    },
                    "size": 1
                }
                sku_result = es.search(index=PRODUCT_INDEX_NAME, body=query_body)
                if sku_result.get("hits", {}).get("hits"):
                    filters["sku_chip"] = chip
            except Exception as ex:
                # Fail gracefully in edge/corner flexis (bad ES state, missing index, etc.)
                pass
    
    # --- 4. Combine base query and chip for "full intent" search (always combine if both are present) ---
    base_for_query = base_category if base_category else query
    if chip and base_for_query:
        # Combine for full semantic intent
        query_out = f"{base_for_query} {chip}".strip()
    else:
        query_out = base_for_query.strip()
    
    # --- 5. Absolute fallback: if nothing, return minimal fields (handles edge/corner flexis) ---
    if not query_out:
        query_out = chip or ""
    
    if not filters:
        filters = {}
    
    return query_out, filters, mapped_category

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
MOBILE_BRANDS = set([
    "samsung", "samsung galaxy", "oppo", "vivo", "google pixel", "google pixel3", "realme", "redmi", "oneplus", "xiaomi", "apple", "nothing", "motorola", "iqoo", "infinix", "tecno"
])
MOBILE_ATTRS = set(["storage", "ram", "color", "battery", "screen", "camera"])
def patch_categories_for_mobile(q_decomposed):
    # Only add category if not already present
    if not q_decomposed["categories"]:
        has_brand = any(b in MOBILE_BRANDS for b in q_decomposed["brands"])
        has_mobile_attr = any(attr in MOBILE_ATTRS for attr in q_decomposed["attributes"])
        if has_brand and has_mobile_attr:
            q_decomposed["categories"].append("smartphone")
    return q_decomposed

BRANDS = [
    "samsung", "apple", "oppo", "vivo", "redmi", "realme", "oneplus", "nokia", "xiaomi", "motorola", "lg",
    "panasonic", "sony", "lenovo", "hp", "dell", "acer", "asus", "mi", "iqoo", "poco", "infinix", "lava", "micromax"
]

COLOR_SYNONYMS = {
    "red": ["red", "redd", "red color", "red colur", "reddish", "laal", "lal"],
    "black": ["black", "blak", "black color", "black colur", "kala", "kala color", "kaala"],
    "blue": ["blue", "blu", "blue color", "blue colur", "neela", "nila"],
    "white": ["white", "whte", "white color", "white colur", "safed", "safed color"],
    "green": ["green", "gren", "green color", "hara", "hara color"],
    "gold": ["gold", "golden", "gold color", "sona", "sona color"],
    "grey": ["grey", "gray", "grey color", "gray color", "dhusar", "dhoosar"],
    "silver": ["silver", "slvr", "silver color", "chandi", "chandi color"],
    "pink": ["pink", "pnik", "pink color", "gulabi", "gulabi color"],
    "purple": ["purple", "parple", "purple color", "baingani"],
    "yellow": ["yellow", "yello", "yellow color", "peela", "peela color"],
    # add more as needed
}

COLOR_LOOKUP = {}
for canon, syns in COLOR_SYNONYMS.items():
    for s in syns:
        COLOR_LOOKUP[s] = canon

CATEGORIES = [
    "mobile phones", "mobiles", "smartphone", "phone", "laptop", "laptops", "notebook", "ultrabook",
    "television", "tv", "tvs", "refrigerator", "fridge", "ac", "air conditioner", "airconditioner", "split ac",
    "washing machine", "washing machines", "smartwatch", "smart watch", "camera", "tablet",
]

# You should have a mapping of attribute regex patterns for each product vertical, here's a mini version:
ATTRIBUTE_PATTERNS = {
    "storage": r"(\d{2,4})\s?gb",
    "ram": r"(\d{1,2})\s?gb\s?ram",
    "screen": r"(\d{1,2}(\.\d)?(\s)?(inch|in|inches))",
    "battery": r"(\d{3,5})\s?mah",
    "camera": r"(\d{1,3})\s?mp",
    "engine_cc": r"(\d{3,5})\s?cc",
    "display": r"(hd|full hd|fhd|4k|amoled|lcd|oled|retina)",
    "variant": r"(pro|max|plus|mini|air|ultra|prime|sport|classic|trend)",
    "color": r"([a-z]+ color|[a-z]+ colou?r|[a-z]+)",
}

def decompose_query_for_boosting(query):
    norm_query = normalize(query)
    tokens = norm_query.split()
    brands_found = []
    categories_found = []
    attributes_found = {}
    # Used for multi-word/compound attributes
    query_words = norm_query
    
    # --- 1. Color Extraction (fuzzy & synonyms) ---
    detected_colors = set()
    for word in tokens:
        color_val = COLOR_LOOKUP.get(word)
        if not color_val:
            match, score, _ = fuzzproc.extractOne(word, COLOR_LOOKUP.keys(), scorer=fuzz.ratio, score_cutoff=78) or (None, 0, None)
            if match:
                color_val = COLOR_LOOKUP[match]
        if color_val:
            detected_colors.add(color_val)
    
    # Also scan multi-word for color phrases ("red color", "black colur", etc)
    for canon, syns in COLOR_SYNONYMS.items():
        for syn in syns:
            if syn in query_words:
                detected_colors.add(canon)
    
    if detected_colors:
        attributes_found["color"] = list(detected_colors)
    
    # --- 2. Storage, RAM, Battery, etc. ---
    for attr, pat in ATTRIBUTE_PATTERNS.items():
        if attr == "color": continue  # already handled
        matches = re.findall(pat, query_words)
        vals = []
        for m in matches:
            if isinstance(m, tuple):
                vals.append(m[0])
            else:
                vals.append(m)
        if vals:
            attributes_found[attr] = vals
    
    # --- 3. Display keywords (HD, 4K, AMOLED, etc) ---
    for disp_kw in ["hd", "full hd", "fhd", "4k", "amoled", "lcd", "oled", "retina"]:
        if disp_kw in query_words:
            attributes_found.setdefault("display", []).append(disp_kw)
    
    # --- 4. Brand Extraction (fuzzy) ---
    for word in tokens:
        brand, score, _ = fuzzproc.extractOne(word, BRANDS, scorer=fuzz.ratio, score_cutoff=80) or (None, 0, None)
        if brand and brand not in brands_found:
            brands_found.append(brand)
    
    # Also match multi-word brand tokens
    for brand in BRANDS:
        if brand in query_words and brand not in brands_found:
            brands_found.append(brand)
    
    # --- 5. Category Extraction (fuzzy) ---
    for word in tokens:
        cat, score, _ = fuzzproc.extractOne(word, CATEGORIES, scorer=fuzz.ratio, score_cutoff=80) or (None, 0, None)
        if cat and cat not in categories_found:
            categories_found.append(cat)
    
    for cat in CATEGORIES:
        if cat in query_words and cat not in categories_found:
            categories_found.append(cat)
    
    tokens_cleaned = [
        tok for tok in tokens
        if tok not in brands_found and tok not in categories_found and tok not in detected_colors
    ]
    
    nothing_found = not (brands_found or categories_found or attributes_found)
    if nothing_found:
        # All tokens become fallback keywords for the ES query
        fallback_keywords = tokens_cleaned if tokens_cleaned else tokens
        return {
            "brands": [],
            "categories": [],
            "attributes": {},
            "tokens": fallback_keywords  # these will go to ES query as boosted 'should' match
        }
    
    # If normal results found:
    return {
        "brands": list(set(brands_found)),
        "categories": list(set(categories_found)),
        "attributes": attributes_found,
        "tokens": tokens_cleaned
    }

def parse_query_to_filters(query):
    # This is a simplified version. For prod, use spaCY or custom NER+pattern rules.
    q = query.lower()
    filters = {}
    sort_by = None
    
    # Brands
    for brand in BRANDS:
        if brand in q:
            filters['manufacturer_desc'] = brand
            q = q.replace(brand, "")
    
    # Categories
    for cat in CATEGORIES:
        if cat in q:
            filters['actual_category'] = CATEGORY_CANONICAL.get(cat, cat)
            q = q.replace(cat, "")
    
    # RAM
    match = re.search(r'(\d{1,2})\s*gb\s*ram', q)
    if match:
        filters['attribute_ram'] = match.group(1) + ' GB'
        q = q.replace(match.group(0), "")
    
    # Storage
    match = re.search(r'(\d{2,4})\s*gb(?!\s*ram)', q)
    if match:
        filters['attribute_internal_storage'] = match.group(1) + ' GB'
        q = q.replace(match.group(0), "")
    
    # Battery
    match = re.search(r'(\d{3,5})\s*mah', q)
    if match:
        filters['attribute_battery_capacity_new'] = match.group(1) + ' mAh'
        q = q.replace(match.group(0), "")
    
    # Capacity (fridge/AC)
    match = re.search(r'(\d{2,4})\s*litre', q)
    if match:
        filters['attribute_capacity_litres'] = match.group(1)
        q = q.replace(match.group(0), "")
    
    match = re.search(r'(\d\.\d|\d+)\s*ton', q)
    if match:
        filters['attribute_capacity_in_tons'] = match.group(1)
        q = q.replace(match.group(0), "")
    
    # 5G
    if "5g" in q:
        filters['product_keywords'] = "5g"
        q = q.replace("5g", "")
    
    # "Best", "Top", "Trending" => sort by rating/popularity
    if any(w in q for w in ["best", "top", "trending"]):
        sort_by = "popularity"  # or "avg_rating", as per your schema
    
    # Numeric price/emi filter (already handled in your code)
    _, price_emi_filters = parse_price_from_query(query)
    if price_emi_filters:
        filters.update(price_emi_filters)
    
    return filters, sort_by

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
def boost_exact_matches(products, user_query):
    """
    Sort products so that exact matches on product_name or other key fields come first.
    Boosts:
      - Exact (case-insensitive) product_name == user_query
      - Exact modelid/SKU match (optional)
    """
    n_query = normalize(user_query)
    
    def exact_score(prod):
        # Product Name match (string match, normalized)
        prod_name = (prod.get("product_name") or "").strip().lower()
        if normalize(prod_name) == n_query:
            return -100  # Highest priority
        
        # Main SKU name match (if available)
        for sku in prod.get("products", []):
            sku_name = (sku.get("name") or "").strip().lower()
            if normalize(sku_name) == n_query:
                return -90
        
        # Optionally, exact modelid match (if user typed a modelid)
        modelid = (prod.get("model_id") or prod.get("modelid") or "").strip()
        if modelid and modelid == user_query.strip():
            return -80
        
        return 0  # Default, not an exact match
    
    # Sort: first by exact match score, then by any prior intent-based sort
    return sorted(products, key=lambda p: (exact_score(p),), reverse=False)

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
import functools
def log_api_call(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        import time
        start = time.time()
        try:
            req_data = None
            try:
                req_data = request.get_json(force=True)
            except Exception:
                req_data = request.get_data().decode("utf-8")
            response = fn(*args, **kwargs)
            status = response[1] if isinstance(response, tuple) else 200
            end = time.time()
            duration_ms = int((end - start) * 1000)
            # Log what matters: endpoint, status, duration, top-level params
            log_obj = {
                "endpoint": request.path,
                "method": request.method,
                "status": status,
                "duration_ms": duration_ms,
                "remote_addr": request.remote_addr,
                "query": req_data,
            }
            msg = "[KPI-API] " + json.dumps(log_obj)
            logging.info(msg)
            # Print for tmux/debug if needed:
            # print(msg)  # <--- Uncomment for tmux
            return response
        except Exception as ex:
            end = time.time()
            logging.error("[KPI-API-ERROR] %s | %s" % (request.path, str(ex)))
            # print("[KPI-API-ERROR]", request.path, str(ex)) # Uncomment for tmux
            raise
    return wrapper

#########################Exactt_SKU_MAtch########################



# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
def preprocess_query(query):
    import re
    q = query.lower()

    # --- existing normalizations (kept as-is) ---
    # q = q.replace("kapde machine", "washing")
    # q = q.replace("kapda machine", "washing")
    # q = q.replace("apple", "@@@@@")
    q = q.replace("maruti suzuki", "maruti")
    q = q.replace("samsung fold", "samsung fold phone")
    q = q.replace("scooter", "activa")
    q = q.replace("scooty", "activa")
    q = q.replace("treadmill", "tread")
    q = q.replace("treadmills", "tread")
    q = q.replace("tredmill", "tread")
    q = q.replace("tredmills", "tread")
    q = q.replace("tread-mill", "tread")
    q = q.replace("tred-mill", "tread")
    q = q.replace("tredmill", "tread")
    q = q.replace("tread mill", "tread")
    q = q.replace("flour mill", "flour")
    q = q.replace("flourmill", "flour")
    q = q.replace("flour-mill", "flour")
    q = q.replace("flour mills", "flour")
    q = q.replace("flor mills", "flour")
    q = q.replace("flor mill", "flour")
    q = q.replace("one plus", "oneplus")
    q = q.replace("microwave", "oven")
    q = q.replace("oppo mobile", "oppo")
    q = q.replace("oppo smartphone", "oppo")
    q = q.replace("vivo smartphone", "vivo")
    q = q.replace("nothing phone", "nothing")
    q = q.replace("nothing phones", "nothing")
    q = q.replace("nothing smartphones", "nothing")
    q = q.replace("nothing smartphone", "nothing")
    q = q.replace("tvs", "iqube")
    # q = q.replace("maruti suzuki", "maruti")
    
 

    return q

# --- Stopword list (expand as needed) ---
STOPWORDS = set("""
a about above after again against all am an and any are aren't as at be because been before being below between both
but by can't cannot could couldn't did didn't do does doesn't doing don't down during each few for from further had hadn't
has hasn't have haven't having he he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm
i've if in into is isn't it it's its itself let's me more most mustn't my myself no nor not of off on once only or other
ought our ours ourselves out over own same shan shan't she she'd she'll she's should shouldn't so some such than that that's
the their theirs them themselves then there there's these they they'd they'll they're they've this those through to too under
until up very was wasn't we we'd we'll we're we've were weren't what what's when when's where where's which while who who's whom
why why's with won't would wouldn't you you'd you'll you're you've your yours yourself yourselves
""".split())

def remove_stopwords(text):
    tokens = text.lower().split()
    tokens = [t for t in tokens if t not in STOPWORDS]
    return ' '.join(tokens)

def stem_rule(token):
    # Example: very lightweight stemming (expand as needed)
    token = token.replace("colours", "color")
    if token.endswith("s") and not token.endswith("ss"):
        token = token[:-1]
    return token

def rank_exact_query_first(products, user_query):
    norm_query = normalize(user_query)
    exact = []
    others = []
    for p in products:
        pn = (p.get("product_name") or "").lower()
        sn = (p.get("search_field") or "").lower()
        if norm_query in pn or norm_query in sn:
            exact.append(p)
        else:
            others.append(p)
    return exact + others

ALL_ATTRIBUTE_OPTIONS = [
    {"es_key": es_key, "display_key": display_key}
    for es_key, display_key in zip(ALL_ATTRIBUTE_FILTERS, ALL_ATTRIBUTE_FILTERS_Name)
]

def get_es_aggregations():
    aggs = {}
    for option in ALL_ATTRIBUTE_OPTIONS:
        es_key = option['es_key']
        aggs[es_key] = {
            "terms": {
                "field": es_key,
                "size": 1000,
                "missing": "__missing__"
            }
        }
    return aggs

def canonicalize_facet_value(es_key, value):
    # Add per-attribute normalization logic if needed
    if es_key in ("manufacturer_desc", "brand", "brand_name"):
        return value.lower().strip()
    elif es_key in ("color", "attribute_color"):
        return value.lower().strip()
    else:
        return value

def parse_filter_aggregations(aggs_response):
    filters = {}
    
    for option in ALL_ATTRIBUTE_OPTIONS:
        es_key = option["es_key"]
        display_key = option["display_key"].replace("_value", "")
        
        if es_key in aggs_response:
            buckets = aggs_response[es_key]["buckets"]
            values = []
            id_name_map = ATTRIBUTE_ID_NAME_MAP.get(es_key, {})
            
            for bucket in buckets:
                if bucket["key"] == "__missing__":
                    continue
                attr_id = str(bucket["key"])
                attr_name = id_name_map.get(attr_id, attr_id)
                values.append({
                    "id": attr_id,
                    "name": attr_name,
                    "count": bucket["doc_count"]
                })
            
            if values:
                filters[display_key] = values
    
    print("[DEBUG] Final filters dict for response:", json.dumps(filters, indent=2))
    return filters








def is_apple_or_blynk_intent(q: str) -> bool:
    """Return True if the query is about Apple/iPhone/Apple ecosystem or Blynk."""
    if not q:
        return False
    qflat = normalize(q).replace(" ", "")

    # Build a compact token set from your constants + a few common Apple ecosystem terms
    tokens = set()
    try:
        tokens |= {normalize(t).replace(" ", "") for t in APPLE_TERMS}
    except Exception:
        pass
    tokens |= {normalize(t).replace(" ", "") for t in BUSINESS_SYNONYMS.get("apple", [])}
    tokens |= {
        "apple", "iphone", "iphones", "ios",
        "ipad", "ipads", "macbook", "macbooks",
        "airpods", "applewatch", "iwatch",
        "applemobile", "applephone",
        "blynk"
    }
    return any(t for t in tokens if t and t in qflat)

from rapidfuzz import fuzz, process

GENERIC_MODIFIERS = {
    "latest", "new", "newest", "upcoming", "recent", "brandnew",
    "trending", "current", "fresh", "modern"
}

def clean_generic_modifiers(q: str) -> str:
    tokens = q.lower().split()
    cleaned = []
    for t in tokens:
        # direct match
        if t in GENERIC_MODIFIERS:
            continue
        # fuzzy check (catch 'laterst', 'latets', etc.)
        best, score, _ = process.extractOne(
            t, GENERIC_MODIFIERS, scorer=fuzz.ratio
        )
        if score >= 80:  # high similarity → treat as a modifier
            continue
        cleaned.append(t)
    return " ".join(cleaned).strip()






# =================== MODIFIED MALL_SEARCH_API FUNCTION ===================
@app.route("/api/mall_search", methods=["POST"])
@log_api_call
def mall_search_api():
    try:
        data = json.loads(request.get_data().decode("utf-8"))
        
        # === NEW: Memory Pattern Detection ===
        raw_query = data.get('query', '').strip().lower()
        import re
        # Define memory patterns to detect
        memory_patterns = [
            r'\b(\d+)\s*gb\b',  # Matches 8gb, 8 gb, 16gb, etc.
            r'\b(\d+)\s*ram\b',  # Matches 8 ram, 16 ram, etc.
        ]
        
        # Define phone-related terms to avoid duplication
        phone_terms = {'phone', 'mobile', 'smartphone', 'iphone', 'android'}
        
        # Check if query contains memory patterns but no phone terms
        has_memory = any(re.search(pattern, raw_query) for pattern in memory_patterns)
        has_phone_term = any(term in raw_query for term in phone_terms)
        
        # Modify query if memory pattern found but no phone term
        if has_memory and not has_phone_term:
            # Extract the memory value for better context
            memory_match = None
            for pattern in memory_patterns:
                match = re.search(pattern, raw_query)
                if match:
                    memory_match = match.group(0)
                    break
            
            if memory_match:
                # Create enhanced query with "phone" context
                enhanced_query = f"{raw_query} phone"
                data['query'] = enhanced_query
                print(f"[Memory Enhancement] Modified query: '{raw_query}' -> '{enhanced_query}'")
        
        # === [NEW] Save original user query, apply stopword and stemming normalization ===
        q_raw = data.get('query', '')  # Now this might be the enhanced query
        q_norm = preprocess_query(q_raw)
        q_nostop = remove_stopwords(q_norm)
        q_tokens = [stem_rule(t) for t in q_nostop.split()]
        data['query'] = " ".join(q_tokens)
        # Save q_raw for later use in exact match boosting
        orig_user_query = q_raw
        
        # --- EARLY EXIT: Apple/iPhone/Apple-ecosystem/Blynk queries => empty response ---
        try:
            user_q_check = orig_user_query  # original (preprocessed) text the user typed
            normalized_q_check = data.get('query', '')  # after light preprocessing

            if is_apple_or_blynk_intent(user_q_check) or is_apple_or_blynk_intent(normalized_q_check):
                empty_resp = {
                    "data": {
                        "PostV1Productlist": {
                            "status": True,
                            "message": "Success",
                            "data": {
                                "products": [],
                                "totalrecords": 0,
                                "suggested_search_keyword": user_q_check,
                                "filters": [],
                                # include slider so frontend never KeyErrors
                                "emi_slider_range": {"min": 0, "max": 0}
                            }
                        }
                    }
                }
                return jsonify(empty_resp), 200
        except Exception:
            # Fail-closed to normal flow if anything unexpected happens
            pass

        
        size = data.get("size", 26)
        from_index = data.get("from_index", data.get("fromIndex", 0))
        # ... rest of the function continues unchanged ...
        try:
            size = int(size)
        except Exception:
            size = 26
        try:
            from_index = int(from_index)
        except Exception:
            from_index = 0
        
        logging.info(f"Received from_index: {from_index}, size: {size}")
        
        city_id_val = data.get("city_id", data.get("cityId", None))
        city_id = f"citi_id_{city_id_val}" if city_id_val else None
        
        sort_by = data.get("sortBy", {})
        
        # === Resolve Query/Filters/Category ===
        query, filters, mapped_category = resolve_query_and_filters(data, es)
        chip = data.get("chip", "").strip()
        base_category = data.get("base_category", "").strip()
        
        # === 1. Calculate Global EMI Min/Max (before applying any EMI filter!) ===
        filters_for_emi_agg = dict(filters)
        emi_filter_for_agg = filters_for_emi_agg.pop("emi", None)
        
        # Build query for EMI aggregation with city filter
        es_query_for_emi_agg = build_advanced_search_query(
            query, filters_for_emi_agg, city_id, mapped_category, price_filter=None, emi_range=None
        )
        
        # Add city-specific aggregation for EMI range
        agg_query = {
            "query": es_query_for_emi_agg,
            "aggs": build_emi_aggregation(city_id),
            "size": 0
        }
        
        try:
            agg_response = es.search(
                index=PRODUCT_INDEX_NAME,
                body=agg_query,
            )
            emi_slider_min = int(agg_response["aggregations"]["city_offers"]["filtered_offers"]["min_emi"]["value"] or 0)
            emi_slider_max = int(agg_response["aggregations"]["city_offers"]["filtered_offers"]["max_emi"]["value"] or 0)
        except Exception as agg_ex:
            logging.warning(f"EMI agg failed: {agg_ex}")
            emi_slider_min, emi_slider_max = 0, 0
        
        # Always combine query + chip (for suggestions & audit)
        combined_query = f"{query} {chip}".strip() if chip else query
        
        # === Correction and Price/EMI Parsing ===
        corrected_query_raw = correct_query(query)
        corrected_query, price_filter = parse_price_from_query(corrected_query_raw)
        corrected_query = expand_brand_aliases_in_query(corrected_query)

        chip_corrected = chip  # (hook for chip spell correction)
        print("===========", chip_corrected, "=============")
        corrected_combined_query = f"{corrected_query} {chip_corrected}".strip() if chip else corrected_query
        print("===========", corrected_combined_query, "=============")
        query_n = normalize(corrected_query)
        show_apple_redirect = False
        
        
        # Enhanced query understanding for better category detection
        query_lower = corrected_query.lower()
        query_tokens = query_lower.split()
        
        # Detect specific product types with more precision
        is_washing_machine_query = any(
            indicator in query_lower 
            for indicator in ["washing", "machine", "laundry", "kapde", "clothes"]
        )
        
        is_ac_query = "ac" in query_tokens or "air conditioner" in query_lower
        
        # Extract brand from query if present
        query_brand = None
        for brand in BRAND_NAMES:
            if brand.lower() in query_lower:
                query_brand = brand
                break
        
        # Force category mapping based on query understanding
        if is_washing_machine_query:
            mapped_category = "washing machines"
            # Only correct the category part, preserve the brand
            if query_brand:
                # Keep the original query but ensure category is set correctly
                corrected_query = f"{query_brand} washing machine"
            else:
                corrected_query = "washing machine"
            query_n = "washing machine"
        elif is_ac_query and query_brand:
            mapped_category = "air conditioner"
            # Keep the original query but ensure AC category
        elif is_apple_only_query(corrected_query):
            corrected_query = "@@@@"
            query_n = "@@@@@@"
            chip = ""
            show_apple_redirect = True
        
        if not mapped_category:
            mapped_category = CATEGORY_CANONICAL.get(query_n) or CATEGORY_CANONICAL.get(query_n.replace(" ", ""))
        
        # Scooter synonym normalization
        if query_n in SCOOTER_SYNONYMS:
            query_n = "two wheeler"
            corrected_query = "two wheeler"
            corrected_combined_query = f"{corrected_query} {chip}".strip() if chip else corrected_query
        
        if not mapped_category:
            mapped_category = CATEGORY_CANONICAL.get(query_n) or CATEGORY_CANONICAL.get(query_n.replace(" ", ""))
        
        emi_filter = filters.pop("emi", None)
        emi_range = None
        
        # FIXED: Improved EMI filter parsing
        if emi_filter:
            emi_range = {}
            try:
                if isinstance(emi_filter, str):
                    import re
                    s = emi_filter.replace(' ', '').replace(',', '-')
                    match = re.match(r"(\d+)[^\d]+(\d+)", s)
                    if match:
                        emi_min, emi_max = int(match.group(1)), int(match.group(2))
                        if emi_min > emi_max:
                            emi_min, emi_max = emi_max, emi_min
                        emi_range["gte"] = emi_min
                        emi_range["lte"] = emi_max
                    else:
                        try:
                            val = int(s)
                            emi_range["gte"] = emi_range["lte"] = val
                        except Exception:
                            pass
                elif isinstance(emi_filter, dict):
                    emi_min = emi_filter.get("min") or emi_filter.get("gte")
                    emi_max = emi_filter.get("max") or emi_filter.get("lte")
                    if emi_min is not None and emi_max is not None and emi_min > emi_max:
                        emi_min, emi_max = emi_max, emi_min
                    if emi_min is not None and emi_min >= 0:
                        emi_range["gte"] = emi_min
                    if emi_max is not None and emi_max >= 0:
                        emi_range["lte"] = emi_max
            except Exception as e:
                logging.warning(f"Invalid EMI filter input: {emi_filter} ({e})")
        elif "emi_min" in filters or "emi_max" in filters:
            emi_range = {}
            emi_min = filters.pop("emi_min", None)
            emi_max = filters.pop("emi_max", None)
            if emi_min is not None:
                emi_range["gte"] = emi_min
            if emi_max is not None:
                emi_range["lte"] = emi_max
        
        # === Build Elasticsearch Query ===
        es_query = build_advanced_search_query(
            corrected_query, filters, city_id, mapped_category, price_filter, emi_range=emi_range
        )
        
        query_body = {"query": es_query}
        query_body["aggs"] = get_es_aggregations()
        
        apple_or_blynk_intent = any(x in corrected_query.lower() for x in ["apple","iphone","ios","blynk", "refurbished", "ipda"])

        if apple_or_blynk_intent and not sort_by:
            # Premium alternatives first: highest price first
            query_body["sort"] = [
                {"city_offers.offer_price": {"order": "desc", "nested": {"path": "city_offers"}}},
                {"modelid": {"order": "asc"}}  # tiebreaker for stable pagination
            ]
    
        # Fix the sorting syntax for nested fields and add tie-breaker for pagination
        if sort_by and "by" in sort_by:
            order = sort_by.get("order", "asc")
            if sort_by["by"] == "emi":
                query_body["sort"] = [
                    {"city_offers.lowest_emi": {"order": order, "nested": {"path": "city_offers"}}},
                    {"modelid": {"order": "asc"}}  # FIXED: Use correct field name 'modelid' instead of 'model_id'
                ]
            elif sort_by["by"] == "price":
                query_body["sort"] = [
                    {"city_offers.offer_price": {"order": order, "nested": {"path": "city_offers"}}},
                    {"modelid": {"order": "asc"}}  # FIXED: Use correct field name 'modelid' instead of 'model_id'
                ]
        else:
            query_body["sort"] = [
                {"city_offers.lowest_emi": {"order": "asc", "nested": {"path": "city_offers"}}},
                {"modelid": {"order": "asc"}}  # FIXED: Use correct field name 'modelid' instead of 'model_id'
            ]
            
        response = es.search(
            index=PRODUCT_INDEX_NAME,
            body=query_body,
            from_=from_index,
            size=size
        )
        
        hits = response["hits"]["hits"]
        total = response["hits"]["total"]["value"]
        filter_aggs = response.get("aggregations", {})
        filters_from_aggs = parse_filter_aggregations(filter_aggs)
        
        logging.info(f"User query: {query}")
        logging.info(f"Elasticsearch query payload: {json.dumps(query_body)}")
        logging.info(f"ES Response count: {len(hits)}")
        logging.info(f"Sample product: {json.dumps(hits[0], default=str) if hits else 'No hits'}")
        
        resp = process_response(hits, total, city_id=city_id, emi_range=None, filters=filters_from_aggs)
        resp = clean_products_for_plp(resp)
        logging.info(f"Full API response: {json.dumps(resp, default=str)}")
        
        resp["data"]["PostV1Productlist"]["data"]["emi_slider_range"] = {
            "min": emi_slider_min,
            "max": emi_slider_max
        }
        
        # ------- Enhanced product ranking with exact match priority -------
        try:
            user_query = combined_query
            
            products_list = resp["data"]["PostV1Productlist"]["data"]["products"]
            if products_list:
                # NEW: Enhanced exact match ranking
                def rank_by_exact_match(products, query):
                    """
                    Rank products with exact matches to product_name, search_field, or product_keywords at the top
                    """
                    query_normalized = normalize(query)
                    query_terms = query_normalized.split()
                    
                    # Define match tiers
                    exact_matches = []
                    partial_matches = []
                    other_matches = []
                    
                    for product in products:
                        match_score = 0
                        match_reasons = []
                        
                        # Normalize product fields
                        product_name = normalize(product.get("product_name", ""))
                        search_field = normalize(product.get("search_field", ""))
                        product_keywords = normalize(product.get("product_keywords", ""))
                        
                        # Check for exact match on product_name
                        if query_normalized == product_name:
                            match_score = 100
                            match_reasons.append("exact_product_name_match")
                        
                        # Check for exact match on search_field
                        elif query_normalized == search_field:
                            match_score = 95
                            match_reasons.append("exact_search_field_match")
                        
                        # Check for exact match on modelid
                        elif query_normalized == str(product.get("modelid", "")).lower():
                            match_score = 90
                            match_reasons.append("exact_model_id_match")
                        
                        # Check for exact match in product_keywords
                        elif query_normalized in product_keywords:
                            match_score = 85
                            match_reasons.append("exact_keywords_match")
                        
                        # Check if all query terms are in product_name
                        elif all(term in product_name for term in query_terms):
                            match_score = 60
                            match_reasons.append("all_terms_in_product_name")
                        
                        # Check if all query terms are in search_field
                        elif all(term in search_field for term in query_terms):
                            match_score = 55
                            match_reasons.append("all_terms_in_search_field")
                        
                        # Check if all query terms are in product_keywords
                        elif all(term in product_keywords for term in query_terms):
                            match_score = 50
                            match_reasons.append("all_terms_in_keywords")
                        
                        # Check SKU names for exact match
                        else:
                            for sku in product.get("products", []):
                                sku_name = normalize(sku.get("name", ""))
                                if query_normalized == sku_name:
                                    match_score = 80
                                    match_reasons.append("exact_sku_match")
                                    break
                                elif all(term in sku_name for term in query_terms):
                                    match_score = 40
                                    match_reasons.append("all_terms_in_sku")
                                    break
                        
                        # Categorize based on match score
                        if match_score >= 85:
                            exact_matches.append((product, match_score, match_reasons))
                        elif match_score >= 40:
                            partial_matches.append((product, match_score, match_reasons))
                        else:
                            other_matches.append(product)
                    
                    # Sort each category by score (descending)
                    exact_matches.sort(key=lambda x: x[1], reverse=True)
                    partial_matches.sort(key=lambda x: x[1], reverse=True)
                    
                    # Extract products from sorted tuples
                    exact_products = [p[0] for p in exact_matches]
                    partial_products = [p[0] for p in partial_matches]
                    
                    # Return combined results in order of priority
                    return exact_products + partial_products + other_matches
                
                # First, apply exact match ranking (highest priority)
                exact_match_products = rank_by_exact_match(products_list, orig_user_query)
                
                # Then, rank by exact specifications (second priority)
                spec_ranked_products = rank_by_specifications(exact_match_products, user_query)
                
                # Then, apply intent-based sorting for secondary attributes
                intent_sorted_products = enhanced_sort_products(spec_ranked_products, user_query)
                
                # Finally, apply the new match priority ranking
                match_priority_products = rank_by_match_priority(intent_sorted_products, orig_user_query)
                
                resp["data"]["PostV1Productlist"]["data"]["products"] = match_priority_products
        except Exception as e:
            logging.error(f"Enhanced intent sorting failed: {e}")
        
        # --- Apple brand filtering for mobile/phone/apple/fon/ios queries (FIXED) ---
        def is_query_mobile_or_apple_or_blynk(q):
            
            
            mobile_terms = set(BUSINESS_SYNONYMS.get("mobiles", [])) | {
                "mobile phones","mobiles","smartphone","phone","phones","fon","fones","cellphone","cell phone"
            }
            apple_terms = set(BUSINESS_SYNONYMS.get("apple", [])) | {
                "apple","iphone","iphones","ios","apple phone","apple phones","apple mobile","apple mobiles"
            }
            blynk_terms = {"blynk","apple blynk","blynk accessories","blynk kit"}
            qn = q.lower().replace(" ", "")
            for term in (apple_terms | blynk_terms):
                if term.replace(" ", "@@@") in qn:
                    return True
            if any(x in qn for x in ["apple","iphone","ios","blynk","mobile","smartphone","phone","fon","fones"]):
                return False
            return False
        

            
        # Block Apple & Blynk and surface premium replacements (e.g., expensive Samsung)
        if is_query_mobile_or_apple_or_blynk(query_n) and any(t in query_n for t in ["apple","iphone","ios","blynk"]):
            filtered = []
            for prod in resp["data"]["PostV1Productlist"]["data"]["products"]:
                brand = (prod.get("manufacturer_desc") or "").strip().lower()
                name  = (prod.get("product_name") or "").lower()
                asset = (prod.get("asset_category_name") or "").lower()
                # drop Apple & Blynk completely
                if (
                    brand in {"apple","blynk"} or
                    "apple" in name or "iphone" in name or "ios" in name or "blynk" in name or
                    "apple" in asset or "iphone" in asset or "ios" in asset or "blynk" in asset
                ):
                    continue
                filtered.append(prod)
            resp["data"]["PostV1Productlist"]["data"]["products"] = filtered

            # Also strip Apple/Blynk from facet buckets if present
            filters_resp = resp["data"]["PostV1Productlist"]["data"]["filters"]
            for f in filters_resp:
                if "attributes" in f:
                    for attr_name in f["attributes"]:
                        before = f["attributes"][attr_name]
                        f["attributes"][attr_name] = [
                            v for v in before if not any(
                                bad in (v["name"] or "").lower() for bad in ["apple","iphone","ios","blynk"]
                            )
                        ]

      
        # --- Mobile brand sorting ---
        elif is_mobile_query(query_n):
            priority_brands = ["Samsung", "Oppo", "Vivo", "Realme", "Redmi", "Nothing"]
            buckets = {b: [] for b in priority_brands}
            others = []
            products = resp["data"]["PostV1Productlist"]["data"]["products"]
            for prod in products:
                brand = (prod.get("manufacturer_desc") or "").strip()
                brand_cmp = brand.lower()
                matched = False
                for pb in priority_brands:
                    if brand_cmp == pb.lower():
                        buckets[pb].append(prod)
                        matched = True
                        break
                if not matched:
                    others.append(prod)
            ordered_products = []
            for pb in priority_brands:
                ordered_products.extend(buckets[pb])
            ordered_products.extend(sorted(others, key=lambda p: (p.get("manufacturer_desc") or "").lower()))
            resp["data"]["PostV1Productlist"]["data"]["products"] = ordered_products
        
        # --- Set suggested_search_keyword, original_query, query_corrected ---
        # combined_query_normalized = normalize(combined_query)
        # corrected_combined_normalized = normalize(corrected_combined_query)
        # query_was_corrected = (combined_query_normalized != corrected_combined_normalized)
        
        combined_query_normalized = normalize(combined_query)
        corrected_combined_normalized = normalize(corrected_combined_query)
        query_was_corrected = (combined_query_normalized != corrected_combined_normalized)

        # --- Clean generic modifiers ---
        query_n = clean_generic_modifiers(query_n)
        combined_query = clean_generic_modifiers(combined_query)
        corrected_combined_query = clean_generic_modifiers(corrected_combined_query)
        if query_n in {"mobile", "mobiles", "phone", "phones", "smartphone", "smartphones"}:
            query_n = "mobiles"
        

        # --- EARLY EXIT: Apple/Blynk queries ---
        if is_apple_or_blynk_intent(query_n):
            empty_resp = {...}
            return jsonify(empty_resp), 200


        
        resp["data"]["PostV1Productlist"]["data"]["original_query"] = combined_query
        resp["data"]["PostV1Productlist"]["data"]["suggested_search_keyword"] = (
            corrected_combined_query if query_was_corrected else combined_query
        )
        resp["data"]["PostV1Productlist"]["data"]["query_corrected"] = query_was_corrected
        
        if show_apple_redirect:
            resp["data"]["PostV1Productlist"]["data"]["info_message"] = (
                "No Apple products found. Showing top smartphones from OnePlus, Nothing, Oppo, Samsung, and more."
            )
        
        return jsonify(resp), 200
    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({"message": "Internal server error", "error": str(e)}), 500 

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
def get_business_autosuggestions(query):
    n_query = normalize(query)
    suggestions = set()
    # Exact match
    if n_query in BUSINESS_AUTOSUGGEST:
        suggestions.update(BUSINESS_AUTOSUGGEST[n_query])
    # Partial/startswith match
    for key in BUSINESS_AUTOSUGGEST:
        if n_query and (n_query in key or key in n_query):
            suggestions.update(BUSINESS_AUTOSUGGEST[key])
    # Enhanced: Partial/startswith matching from BUSINESS_SYNONYMS
    for synlist in BUSINESS_SYNONYMS.values():
        for syn in synlist:
            syn_norm = normalize(syn)
            if n_query and (n_query in syn_norm or syn_norm.startswith(n_query)):
                suggestions.add(syn)
    return list(dict.fromkeys(suggestions))

def get_synonym_suggestions(query):
    n_query = normalize(query)
    close_syns = []
    for canon, syns in BUSINESS_SYNONYMS.items():
        canon_norm = normalize(canon)
        if n_query and (n_query in canon_norm or canon_norm.startswith(n_query)):
            close_syns.append(canon)
        for syn in syns:
            syn_norm = normalize(syn)
            if n_query and (n_query in syn_norm or syn_norm.startswith(n_query)):
                close_syns.append(syn)
    # Deduplicate, prioritize shorter matches, return top 5
    close_syns = sorted(set(close_syns), key=len)
    return close_syns[:5]

@lru_cache(maxsize=4096)
def _cached_autosuggest_es(query_terms_tuple):
    expanded_terms = query_terms_tuple
    should_clauses = []
    PRIORITY_BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "Realme", "Redmi", "Nothing"]
    query_string = " ".join(expanded_terms).lower()
    
    # Stricter brand boosting
    for brand in PRIORITY_BRANDS:
        if query_string.startswith(brand.lower()) or brand.lower().startswith(query_string):
            should_clauses.append({"term": {"value.keyword": {"value": brand, "boost": 50}}})
    
    # Stricter synonym & canonical expansion
    for term in expanded_terms:
        if len(term) < 2: continue
        should_clauses += [
            {"match_phrase_prefix": {"value": {"query": term, "boost": 7}}},
            {"match": {"value": {"query": term, "fuzziness": "AUTO", "boost": 4}}},
        ]
    
    # Add canonical category boost for exact synonym match
    for cat, syns in BUSINESS_SYNONYMS.items():
        for s in syns:
            if query_string == s.lower():
                should_clauses.append({"term": {"value.keyword": {"value": CATEGORY_CANONICAL.get(cat, cat), "boost": 25}}})
    
    es_query = {
        "bool": {
            "should": should_clauses,
            "minimum_should_match": 1
        }
    }
    
    response = es.search(
        index=AUTOSUGGEST_INDEX_NAME,
        body={
            "query": es_query,
            "_source": ["value"],
            "size": 50
        }
    )
    return response

def get_enhanced_intent(query):
    colors = ["black", "white", "silver", "blue", "red", "grey", "gold", "green", "yellow", "pink", "purple"]
    storages = ["256 gb", "512 gb", "128 gb", "64 gb", "32 gb", "16 gb", "8 gb", "4 gb", "1 tb", "2 tb"]
    sizes = ["43 inch", "50 inch", "55 inch", "65 inch", "32 inch", "40 inch", "1.5 ton", "2 ton", 
             "6 kg", "7 kg", "8 kg", "9 kg", "10 kg"]
    variants = ["pro", "max", "plus", "mini", "air", "smart", "ultra", "suv", "sedan", "hatchback"]
    
    # Extract brands from query
    brands = []
    for brand in BRAND_NAMES:
        if brand.lower() in query.lower():
            brands.append(brand)
    
    query = query.lower()
    
    intent = {
        "colors": [color for color in colors if color in query],
        "storages": [storage for storage in storages if storage in query],
        "sizes": [size for size in sizes if size in query],
        "variants": [variant for variant in variants if variant in query],
        "brands": brands
    }
    return intent

def rank_by_specification_match(products, query):
    query_lower = query.lower()
    
    # Extract specifications from query
    size_match = re.search(r'(\d+)\s*inch', query_lower)
    size = size_match.group(1) if size_match else None
    
    brand_match = None
    for brand in BRAND_NAMES:
        if brand.lower() in query_lower:
            brand_match = brand
            break
    
    type_match = None
    if "led" in query_lower:
        type_match = "led"
    elif "lcd" in query_lower:
        type_match = "lcd"
    elif "oled" in query_lower:
        type_match = "oled"
    
    # Score products based on specification matches
    def spec_score(prod):
        score = 0
        
        # Size matching
        if size:
            prod_size = prod.get("attribute_screen_size_in_inches", "").lower()
            if size in prod_size:
                score += 10  # Exact size match gets highest score
        
        # Brand matching
        if brand_match:
            prod_brand = prod.get("manufacturer_desc", "").lower()
            if brand_match.lower() in prod_brand:
                score += 8
        
        # Type matching
        if type_match:
            prod_type = prod.get("product_type", "").lower()
            if type_match in prod_type:
                score += 5
        
        return -score  # Negative for descending sort
    
    return sorted(products, key=spec_score)
 
def robust_extract_attribute(prod, keys):
    for key in keys:
        value = prod.get(key, "")
        if not value:
            continue
        # If value is a dict, try extracting 'name'
        if isinstance(value, dict):
            value = value.get("name", "") or value.get("value", "") or str(value)
        # If value is a list, try extracting from first element
        elif isinstance(value, list):
            if value and isinstance(value[0], dict):
                value = value[0].get("name", "") or value[0].get("value", "") or str(value[0])
            elif value and isinstance(value[0], str):
                value = value[0]
            else:
                value = str(value)
        # Now, make sure it's a string
        if isinstance(value, str):
            return value.lower()
        else:
            return str(value).lower()
    # Fallback for nested products
    if "products" in prod and prod["products"]:
        for sku in prod["products"]:
            out = robust_extract_attribute(sku, keys)
            if out:
                return out
    return ""
 
def enhanced_score_product(prod, intent):
    score = 0
    
    prod_color = get_attribute(prod, ["color", "attribute_color", "attribute_swatch_color"])
    prod_storage = get_attribute(prod, ["storage", "attribute_storage", "attribute_internal_storage"])
    prod_size = get_attribute(prod, ["size", "screen_size", "attribute_screen_size_in_inches"])
    prod_brand = get_attribute(prod, ["manufacturer_desc", "brand"])
    prod_model = get_attribute(prod, ["model", "variant", "attribute_variant"])
    
    # Enhanced scoring based on user intent
    if intent["colors"]:
        if any(c in prod_color for c in intent["colors"]):
            score += 5
    if intent["storages"]:
        if any(s in prod_storage for s in intent["storages"]):
            score += 4
    if intent["sizes"]:
        # Handle size matching more intelligently (e.g., "55 inch" vs "55")
        size_val = prod_size.replace("inch", "").replace("in", "").strip()
        for sz in intent["sizes"]:
            sz_val = sz.replace("inch", "").replace("in", "").strip()
            if sz_val in size_val or size_val in sz_val:
                score += 3
    if intent["brands"]:
        if any(b in prod_brand for b in intent["brands"]):
            score += 6  # Higher score for brand matches
    if intent["models"]:
        if any(m in prod_model for m in intent["models"]):
            score += 2
    
    # Negative for descending sort (highest score first)
    return -score
 
def enhanced_sort_products(products, query):
    intent = get_enhanced_intent(query)
    return sorted(products, key=lambda p: enhanced_score_product(p, intent))

# =================== EXISTING HELPER FUNCTIONS (UNCHANGED) ===================
@app.route("/api/mall_autosuggest", methods=["POST"])
@log_api_call
def mall_autosuggest_api():
    try:
        start_time = time.time()
        data = json.loads(request.get_data().decode("utf-8"))
        query = data.get("query", "")
        if not query or not query.strip():
            return jsonify({"error": "Query parameter 'query' is required"}), 400
        
        corrected_query = correct_query(query)
        expanded_terms = tuple(expand_search_terms(corrected_query))
        
        # BUSINESS SUGGESTIONS FIRST
        business_suggestions = get_business_autosuggestions(corrected_query)
        
        # SYNONYM SUGGESTIONS (NEW)
        synonym_suggestions = get_synonym_suggestions(corrected_query)
        
        # THEN ES SUGGESTIONS
        es_start = time.time()
        response = _cached_autosuggest_es(expanded_terms)
        es_end = time.time()
        
        variant_map = defaultdict(set)
        for hit in response["hits"]["hits"]:
            val = hit["_source"]["value"].title().strip()
            # Defensive: split on commas if the value accidentally contains multiple products
            suggestions = [v.strip() for v in val.split(",") if v.strip()]
            for sug in suggestions:
                base = re.sub(r"\s*\(.*\)$", "", sug)
                variant = re.search(r"\((.*?)\)$", sug)
                variant_name = variant.group(1) if variant else ""
                variant_map[base].add(variant_name)
        PRIORITY_BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "Realme", "Redmi", "Nothing"]
        keywords = []
        query_lower = corrected_query.lower()
        
        for brand in PRIORITY_BRANDS:
            if any(base.lower().startswith(brand.lower()) or brand.lower().startswith(query_lower)
                   for base in variant_map.keys()):
                for base in variant_map.keys():
                    if base.lower().startswith(brand.lower()):
                        n_var = len([v for v in variant_map[base] if v])
                        if n_var > 1:
                            keywords.append(f"{base} (+{n_var} variants)")
                        else:
                            keywords.append(base)
        
        already = set([k.split(" (+")[0] for k in keywords])
        for base, variants in variant_map.items():
            if base in already:
                continue
            n_var = len([v for v in variants if v])
            if n_var > 1:
                keywords.append(f"{base} (+{n_var} variants)")
            else:
                keywords.append(base)
        
        keywords = keywords[:10]
        
        # Merge business and ES results: business always first, deduped
        all_keywords = list(dict.fromkeys(business_suggestions + keywords))
        all_keywords = all_keywords[:15]
        
        # ===== Chips/filter_text logic (robust for synonyms and major categories) =====
        q_lower = normalize(corrected_query)
        mapped_category = CATEGORY_CANONICAL.get(q_lower)
        
        if not mapped_category:
            for cat_key, syns in BUSINESS_SYNONYMS.items():
                # Check normalized synonyms
                if q_lower in [normalize(s) for s in syns]:
                    mapped_category = cat_key
                    break
        
        # ========== ENHANCED CHIPS & FILTER LOGIC ==========
        chips, filter_text = [], ""
        q_lower = normalize(corrected_query)
        mapped_category = CATEGORY_CANONICAL.get(q_lower)
        
        # 1. Try direct mapping (canonical)
        if mapped_category and mapped_category in CATEGORY_HARDCODED_CHIPS:
            chips, filter_text = CATEGORY_HARDCODED_CHIPS[mapped_category]
        
        # 2. If not found, try synonyms (BUSINESS_SYNONYMS)
        elif not chips:
            for cat_key, syns in BUSINESS_SYNONYMS.items():
                # Check normalized synonyms
                if q_lower in [normalize(s) for s in syns]:
                    if cat_key in CATEGORY_HARDCODED_CHIPS:
                        chips, filter_text = CATEGORY_HARDCODED_CHIPS[cat_key]
                        break
        
        # 3. If still not found, try business autosuggest mapping
        elif not chips:
            for key in BUSINESS_AUTOSUGGEST:
                if q_lower == key or q_lower in key or key in q_lower:
                    # Suggest chips for business intent if mapped to a known category
                    cat_map = CATEGORY_CANONICAL.get(key)
                    if cat_map and cat_map in CATEGORY_HARDCODED_CHIPS:
                        chips, filter_text = CATEGORY_HARDCODED_CHIPS[cat_map]
                        break
        
        # 4. Fallback for mobiles (most common use-case)
        if not chips and q_lower in BUSINESS_SYNONYMS.get("mobiles", []):
            chips, filter_text = CATEGORY_HARDCODED_CHIPS["mobiles"]
        
        total_time = (time.time() - start_time) * 1000
        es_time = (es_end - es_start) * 1000
        print(f"[Autosuggest Timing] ES: {es_time:.2f} ms | Total: {total_time:.2f} ms | Query: {query}")
        
        out = {
            "message": "no autosuggest issue",
            "response": {
                "chips": chips,
                "filter_text": filter_text,
                "keywords": all_keywords,
                "synonym_suggestions": synonym_suggestions,  
                "language": "english"
            }
        }
        return jsonify(out), 200
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"message": "Internal server error", "error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8007)

