"""Central configuration for the Colima real-estate scraper."""

from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_DIR = BASE_DIR / "db"
LOGS_DIR = BASE_DIR / "logs"
EXPORTS_DIR = BASE_DIR / "exports"

for _d in (DATA_DIR, LOGS_DIR, EXPORTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{DATA_DIR / 'listings.db'}",
)

# ---------------------------------------------------------------------------
# Municipios de Colima
# ---------------------------------------------------------------------------
MUNICIPIOS: list[str] = [
    "Colima",
    "Villa de Álvarez",
    "Coquimatlán",
    "Cuauhtémoc",
    "Comala",
    "Armería",
    "Tecomán",
    "Manzanillo",
    "Ixtlahuacán",
    "Minatitlán",
]

# Alias / variantes que usan los portales
MUNICIPIOS_ALIASES: dict[str, str] = {
    "villa de alvarez": "Villa de Álvarez",
    "villa alvarez": "Villa de Álvarez",
    "colima capital": "Colima",
    "manzanillo colima": "Manzanillo",
    "tecoman": "Tecomán",
}

STATE = "Colima"
STATE_CODE = "COL"          # Código MercadoLibre
COUNTRY_CODE = "MX"

# ---------------------------------------------------------------------------
# Scraping parameters
# ---------------------------------------------------------------------------
SCRAPING = {
    # Delay entre requests (segundos)
    "request_delay_min": 1.0,
    "request_delay_max": 3.5,
    # Máximo de páginas por portal / municipio
    "max_pages": 50,
    # Tamaño de página (listados por request)
    "page_size": 48,
    # Timeout HTTP (segundos)
    "http_timeout": 30,
    # Reintentos
    "max_retries": 4,
    "retry_wait_min": 2,
    "retry_wait_max": 10,
    # Concurrencia máxima de requests async
    "max_concurrent": 5,
    # User-Agent fallback si fake-useragent falla
    "fallback_ua": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

# ---------------------------------------------------------------------------
# Portal URLs
# ---------------------------------------------------------------------------
PORTALS = {
    "inmuebles24": {
        "base_url": "https://api.inmuebles24.com/v3",
        "search_path": "/properties/search",
        "listing_url": "https://www.inmuebles24.com",
    },
    "lamudi": {
        "base_url": "https://www.lamudi.com.mx",
        "search_path": "/colima/",
    },
    "vivanuncios": {
        "base_url": "https://www.vivanuncios.com.mx",
        "search_path": "/s-inmuebles/colima/",
    },
    "mercadolibre": {
        "base_url": "https://api.mercadolibre.com",
        "search_path": "/sites/MLM/search",
        "category": "MLM1459",   # Inmuebles
        "state_id": "TUxNQ09MTzMwNzk",  # Colima
    },
}

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
SCHEDULER = {
    "timezone": "America/Mexico_City",
    "jobs": [
        {"hour": 6,  "minute": 0,  "id": "morning_scrape"},
        {"hour": 18, "minute": 0,  "id": "evening_scrape"},
    ],
    "misfire_grace_time": 600,   # segundos
    "coalesce": True,
}

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
ANALYSIS = {
    "price_outlier_std": 3.0,    # desvs. estándar para filtrar outliers
    "min_listings_for_stats": 5, # mínimo de listados para calcular estadísticas
    "amenities": [
        "alberca",
        "estacionamiento",
        "seguridad",
        "jardín",
        "bodega",
        "gimnasio",
        "elevador",
    ],
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE: Path = LOGS_DIR / "scraper.log"
LOG_ROTATION: str = "10 MB"
LOG_RETENTION: str = "30 days"
