# WebScraperInmobiliario — Colima

Scraper inmobiliario automatizado para el estado de **Colima, México**.
Recopila listados de venta y renta de cuatro portales, los almacena en SQLite (o PostgreSQL) con historial de precios y ofrece análisis de mercado con Pandas.

---

## Tabla de contenidos

1. [Características](#características)
2. [Arquitectura](#arquitectura)
3. [Requisitos](#requisitos)
4. [Instalación](#instalación)
5. [Configuración](#configuración)
6. [Uso de la CLI](#uso-de-la-cli)
7. [Portales soportados](#portales-soportados)
8. [Modelos de datos](#modelos-de-datos)
9. [Análisis de mercado](#análisis-de-mercado)
10. [Scheduler](#scheduler)
11. [Estructura de archivos](#estructura-de-archivos)

---

## Características

- **4 portales**: Inmuebles24, Lamudi, Vivanuncios (OLX) y MercadoLibre
- **Async + HTTP/2** con `httpx`, rate-limiting configurable y reintentos exponenciales (`tenacity`)
- **Upsert inteligente**: detecta cambios de precio y guarda historial automáticamente
- **Scheduler**: ejecuciones automáticas a las 6:00 AM y 6:00 PM (zona horaria `America/Mexico_City`)
- **Análisis Pandas**: precio mediano, precio/m², impacto de amenidades, evolución histórica
- **Exportación Excel** con `openpyxl`
- **CLI unificada** con subcomandos: `init-db`, `scrape`, `schedule`, `analyze`
- Logging estructurado con `loguru` + consola rich con `rich`

---

## Arquitectura

```
main.py          ← CLI (argparse + rich)
pipeline.py      ← Orquestador async
scheduler.py     ← APScheduler daemon (cron 6AM / 6PM)
config.py        ← Configuración central
│
├── scrapers/
│   ├── base.py          ← Clase base async (httpx, tenacity, normalización)
│   ├── inmuebles24.py   ← API JSON interna
│   ├── lamudi.py        ← __NEXT_DATA__ + fallback HTML
│   ├── vivanuncios.py   ← __NEXT_DATA__ + fallback HTML (OLX)
│   └── mercadolibre.py  ← API pública MLM
│
├── db/
│   ├── models.py        ← Listing, PriceHistory, ScrapingRun (SQLAlchemy 2.x)
│   └── database.py      ← Sesiones, upsert_listing, mark_inactive, run helpers
│
└── analysis/
    └── market.py        ← Funciones Pandas de análisis de mercado
```

---

## Requisitos

- Python **3.11+**
- (Opcional) PostgreSQL si se prefiere sobre SQLite

---

## Instalación

```bash
# 1. Clonar repositorio
git clone <repo-url>
cd WebScraperInmobiliario

# 2. Crear entorno virtual
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Instalar navegadores de Playwright (necesario solo si se usan scrapers con JS)
playwright install chromium

# 5. Inicializar la base de datos
python main.py init-db
```

---

## Configuración

Copia `.env.example` a `.env` y ajusta los valores:

```dotenv
# Base de datos (por defecto SQLite en data/listings.db)
DATABASE_URL=sqlite:///data/listings.db
# Para PostgreSQL:
# DATABASE_URL=postgresql+psycopg2://user:pass@localhost/inmobiliario

# Nivel de logging: DEBUG | INFO | WARNING | ERROR
LOG_LEVEL=INFO
```

El archivo `config.py` centraliza todos los parámetros:

| Variable | Descripción |
|---|---|
| `MUNICIPIOS` | Lista de municipios de Colima a rastrear |
| `SCRAPING["max_pages"]` | Páginas máximas por portal/municipio |
| `SCRAPING["request_delay_min/max"]` | Delay aleatorio entre requests (segundos) |
| `SCRAPING["max_concurrent"]` | Máximo de requests simultáneos |
| `SCHEDULER["jobs"]` | Horas de ejecución del scheduler |

---

## Uso de la CLI

### Inicializar la base de datos

```bash
python main.py init-db
```

### Ejecutar scraping inmediato

```bash
# Todos los portales, venta y renta
python main.py scrape

# Solo Inmuebles24 y MercadoLibre, solo ventas
python main.py scrape --portals inmuebles24 mercadolibre --operations venta

# Con logging detallado
python main.py -v scrape
```

### Iniciar scheduler (6AM / 6PM)

```bash
python main.py schedule

# Solo portales específicos
python main.py schedule --portals lamudi vivanuncios
```

### Análisis de mercado

```bash
# Precio mediano por municipio (ventas)
python main.py analyze median

# Precio/m² (rentas, solo casas)
python main.py analyze ppm2 --operation renta --property-type casa

# Impacto de amenidades
python main.py analyze amenities --operation venta

# Evolución histórica semanal
python main.py analyze history --municipio Manzanillo --freq W

# Exportar todo a Excel
python main.py analyze export --operation venta
```

---

## Portales soportados

| Portal | Estrategia | URL |
|---|---|---|
| **Inmuebles24** | API JSON interna (`/v3/properties/search`) | inmuebles24.com |
| **Lamudi** | `__NEXT_DATA__` (Next.js) + fallback HTML | lamudi.com.mx |
| **Vivanuncios** | `__NEXT_DATA__` (OLX/Next.js) + fallback HTML | vivanuncios.com.mx |
| **MercadoLibre** | API pública (`/sites/MLM/search`, categoría MLM1459) | mercadolibre.com.mx |

---

## Modelos de datos

### `Listing`

| Campo | Tipo | Descripción |
|---|---|---|
| `portal` | str | Nombre del portal de origen |
| `external_id` | str | ID único en el portal |
| `operation` | str | `venta` o `renta` |
| `property_type` | str | `casa`, `departamento`, `terreno`, etc. |
| `municipio` | str | Municipio normalizado de Colima |
| `colonia` | str | Colonia / fraccionamiento |
| `price` | float | Precio en la moneda indicada |
| `currency` | str | `MXN` o `USD` |
| `price_per_m2` | float | Calculado automáticamente |
| `area_total` | float | Superficie total (m²) |
| `area_built` | float | Superficie construida (m²) |
| `bedrooms` | int | Recámaras |
| `bathrooms` | float | Baños |
| `parking` | int | Lugares de estacionamiento |
| `amenities` | JSON | Lista de amenidades |
| `is_active` | bool | Activo en el portal |
| `first_seen` | datetime | Primera vez que se detectó |
| `last_seen` | datetime | Última actualización |

### `PriceHistory`

Registra cada cambio de precio: `listing_id`, `price`, `currency`, `recorded_at`.

### `ScrapingRun`

Metadatos de cada ejecución: portales, totales, estado, tiempos de inicio/fin.

---

## Análisis de mercado

Las funciones en `analysis/market.py`:

| Función | Descripción |
|---|---|
| `median_price_by_municipio(operation)` | Precio mediano, media, min, max por municipio |
| `price_per_m2_stats(operation)` | Estadísticas de precio/m² por municipio |
| `amenity_impact(operation)` | % de premium por amenidad (alberca, gym, etc.) |
| `price_history_evolution(municipio, freq)` | Serie temporal de precio mediano |
| `export_summary(operation)` | Exporta todas las tablas a un `.xlsx` |

Los outliers se filtran automáticamente usando 3 desviaciones estándar (configurable en `config.py → ANALYSIS["price_outlier_std"]`).

---

## Scheduler

El scheduler usa **APScheduler** con `AsyncIOScheduler` y `CronTrigger`:

- **6:00 AM** `America/Mexico_City` → job `morning_scrape`
- **6:00 PM** `America/Mexico_City` → job `evening_scrape`

Configurable en `config.py → SCHEDULER["jobs"]`.

```bash
# Iniciar daemon (bloquea hasta Ctrl+C)
python main.py schedule
```

---

## Estructura de archivos

```
WebScraperInmobiliario/
├── main.py                  # CLI unificada
├── pipeline.py              # Orquestador
├── scheduler.py             # Daemon APScheduler
├── config.py                # Configuración central
├── requirements.txt
├── .env                     # Variables de entorno (no comitear)
├── data/                    # Base de datos SQLite (auto-creado)
├── logs/                    # Logs rotativos (auto-creado)
├── exports/                 # Archivos Excel exportados (auto-creado)
├── db/
│   ├── __init__.py
│   ├── models.py            # ORM models
│   └── database.py          # Sesiones y helpers
├── scrapers/
│   ├── __init__.py
│   ├── base.py              # Clase base async
│   ├── inmuebles24.py
│   ├── lamudi.py
│   ├── vivanuncios.py
│   └── mercadolibre.py
└── analysis/
    ├── __init__.py
    └── market.py            # Análisis Pandas
```

---

## Licencia

MIT
