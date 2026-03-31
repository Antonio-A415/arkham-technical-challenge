# Nuclear Outages Pipeline

Aplicación FastAPI para extraer datos diarios de outages nucleares de la API pública de EIA, almacenarlos en Parquet/DuckDB y exponerlos a través de una API REST.

## Requisitos

- Python 3.11+
- `pip`
- `docker` / `docker compose` (opcional)

## Opciones de uso

### Opción 1: Ver localmente

1. Activa el entorno virtual.
   - Si usas el entorno ya incluido:
     ```powershell
     .\intern-challenge\Scripts\Activate.ps1
     ```
   - O crea uno nuevo:
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```

2. Instala dependencias:
   ```powershell
   pip install -r requirements.txt
   ```

3. Configura variables de entorno.
   Crea un archivo `.env` en la raíz del proyecto o exporta las variables en tu entorno.

   Variables recomendadas:
   - `API_KEY` — clave de la EIA API (requerida para extracción de datos)
   - `EIA_BASE_URL` — opcional, por defecto `https://api.eia.gov/v2`
   - `DATA_DIR` — opcional, por defecto `data`
   - `LOG_DIR` — opcional, por defecto `logs`
   - `CORS_ORIGIN` — opcional, por defecto `http://localhost:5173,http://localhost:3000`
   - `APP_API_KEY` — opcional; si se define, habilita auth mediante el header `X-API-Key`

4. Ejecuta la API localmente:
   ```powershell
   cd app
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

5. Abre la documentación Swagger:
   - `http://localhost:8000/docs`

### Opción 2: Usar los endpoints en la nube

- API desplegada (Railway):
  - `https://arkham-technical-challenge-production.up.railway.app/`

- UI front-end desplegada (Vercel):
  - `https://visor-qbv25xxbr-antonios-projects-40aa555e.vercel.app/`

La UI en Vercel ya consume la API desplegada. Si sólo quieres revisar la aplicación sin instalación local, usa estos enlaces.

## Ejecución con Docker

- Local con Docker Compose:
  ```powershell
  docker compose up --build
  ```

- Con Dockerfile directo:
  ```powershell
  docker build -t nuclear-outages-api .
  docker run -p 8000:8000 nuclear-outages-api
  ```

## Endpoints principales

- `GET /` — mensaje de bienvenida
- `GET /health` — verificación del servicio
- `GET /docs` — documentación OpenAPI
- `POST /refresh` — dispara la extracción de datos desde EIA

> Nota: si configuras `APP_API_KEY`, añade la cabecera `X-API-Key` en las peticiones.

## Tests

Ejecuta los tests con:

```powershell
pytest
```

## Despliegue

- La API ya está desplegada.
- La UI front-end que consume la API está desplegada en Vercel y Railway.

Si deseas ejecutar la UI localmente, asegúrate de que apunte al endpoint de la API desplegada o a `http://localhost:8000` durante desarrollo.

- API desplegada (Railway): https://arkham-technical-challenge-production.up.railway.app/
- UI desplegada (Vercel): https://visor-qbv25xxbr-antonios-projects-40aa555e.vercel.app/
