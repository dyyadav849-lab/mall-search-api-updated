# Mall Search API

This repository contains the Mall Search API service packaged as a Flask application.

## Project layout

```
mall-search-api-updated/
├── requirements.txt       # Python dependencies for deployment
├── src/
│   └── mall_search_api/
│       ├── __init__.py    # Package exports for the Flask app
│       ├── app.py         # Full API implementation and route handlers
│       └── utils.py       # Domain specific constants and helper data
└── wsgi.py                # WSGI entrypoint for production servers
```

The original application code is preserved without modification in `src/mall_search_api/app.py`
and `src/mall_search_api/utils.py`. The package exposes a `create_app` factory that can be
used by WSGI servers such as Gunicorn or uWSGI for deployment.

## Running locally

Install the dependencies and run the service:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
python wsgi.py
```

The API will be available at `http://localhost:8007`.
