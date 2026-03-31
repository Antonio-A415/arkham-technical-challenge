# Nuclear Outages Pipeline

Aplicación FastAPI para extraer datos diarios de outages nucleares de la API pública de EIA, almacenarlos en Parquet/DuckDB y exponerlos a través de una API REST.

## Requisitos

- Python 3.11+
- `pip`
- `docker` / `docker compose` (opcional)

## Quick start

### Local
1. Abre el proyecto en tu terminal.
2. Activa el entorno virtual:
   ```powershell
   .\intern-challenge\Scripts\Activate.ps1
   ```
3. Instala dependencias:
   ```powershell
   pip install -r requirements.txt
   ```
4. Crea un archivo `.env` en la raíz con las variables necesarias.
5. Ejecuta la API:
   ```powershell
   cd app
   uvicorn main:app --host 0.0.0.0 --port 8000
   ```

### Nube
- Usa la API desplegada en Railway.
- Usa la UI desplegada en Vercel.

## API key setup

- `API_KEY`: clave de la EIA API, requerida para que `/refresh` pueda obtener datos externos.
- `APP_API_KEY`: clave opcional para proteger los endpoints de la API en `X-API-Key`.
- Ejemplo de `.env`:
  ```env
  API_KEY=tu_api_key_de_eia
  APP_API_KEY=tu_api_key_para_api
  EIA_BASE_URL=https://api.eia.gov/v2
  DATA_DIR=data
  LOG_DIR=logs
  CORS_ORIGIN=http://localhost:5173,http://localhost:3000
  ```

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
  - `https://visor-alpha.vercel.app/`

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

## Assumptions made

- El proyecto implementa una API mínima con dos servicios clave: `/data` y `/refresh`.
- La extracción se realiza desde la API pública de EIA y se almacena localmente en `data/`.
- El servicio puede ejecutarse localmente o consumirse desde la nube.
- La autenticación es opcional y se habilita solo con `APP_API_KEY`.
- El frontend desplegado consume la API remota; el backend se puede ejecutar local o en Railway.

## Result examples

### Ejemplo: `GET /data?limit=10&page=1`

```json
{
  "items": [
    {
      "snapshot_id": "2024-03-01-123",
      "report_date": "2024-03-01",
      "facility_name": "Plant A",
      "state": "VA",
      "status": "Outage",
      "duration_hours": 12
    }
  ],
  "page": 1,
  "limit": 10,
  "total": 42
}
```

### Ejemplo: `POST /refresh`

```json
{
  "success": true,
  "message": "Datos extraídos correctamente",
  "records_loaded": 15
}
```

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
- UI desplegada (Vercel): https://visor-alpha.vercel.app/
