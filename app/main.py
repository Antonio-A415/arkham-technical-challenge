"""
Nuclear Ouatges Pipeline via FastAPI application entry point
"""
import sys
import os

# Asegura que el directorio de la app esté en el path
sys.path.insert(0, os.path.dirname(__file__))
import logging 


from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routers import data, refresh

#model response
from api.schemas import HealthResponse

from api.dependencies import get_db, get_writer

#cofniguracion de Logging
LOG_DIR = Path(os.getenv("LOG_DIR","logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR/"pipeline.log"),
    ],
)

logger = logging.getLogger(__name__)

#App

app = FastAPI(
    title="Nuclear Outages Pipeline by Antonio May Couoh",
    description=(
        "Extarcts daily U.S. nuclear outage data from the EIA Open Data API"
        "stores it in Parquet, adn exposes a querybale REST API."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

ALLOWED_ORIGINS = os.getenv("CORS_ORIGIN", "http://localhost:5173,http://localhost:3000","https://visor-qbv25xxbr-antonios-projects-40aa555e.vercel.app").split(",")
print("ALLOWED_ORIGINS:", ALLOWED_ORIGINS)
#adding middlewares

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"], #all methos
    allow_headers=["*"],
)


#routers

app.include_router(data.router)
app.include_router(refresh.router)

#root
@app.get("/", tags=["system"])
def root():
    return {"message": "Nuclear Outages  Pipeline API is running. Check /docs for usage details."}

#health check
@app.get("/health", response_model=HealthResponse, tags=["system"])
def health():
    """Basic health check — confirms service is up and data files exist."""
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    return HealthResponse(
        status="ok",
        data_dir=str(data_dir.resolve()),
        snapshots_available=(data_dir / "outage_snapshots.parquet").exists(),
        us_totals_available=(data_dir / "us_totals.parquet").exists(),
    )
 
@app.on_event("startup")
async def startup():
    logger.info("Nuclear Outages API starting up...")

    get_writer()
    get_db()
    logger.info("Storage layer ready.")
