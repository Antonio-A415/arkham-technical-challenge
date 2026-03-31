import logging
import os
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

#import error.EIAAuthError as AuthError
from connector.error.EIANetworkError import EIANetworkError
#import error.EIANetworkError as NetworkError
from connector.error.EIAAuthError import EIAAuthError
from typing import Any, Generator

import time

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY")
EIA_BASE_URL = os.getenv("EIA_BASE_URL")


#routes for the endpoint, one per level according to the documentation

ROUTE_GENERATOR_OUTAGES = "nuclear-outages/generator-nuclear-outages"  # unit level
ROUTE_FACILITY_OUTAGES  = "nuclear-outages/facility-nuclear-outages"   # plant level
ROUTE_US_OUTAGES        = "nuclear-outages/us-nuclear-outages"         # national totals

class EIAClient:
    """
        API_KEY and BASE_URL are required

        Both can also be passed as constructor arguments, which takes precedence over
        the enviroment, this is so useful in unit tests.
    """

    def __init__(self, api_key : str | None=None, base_url : str | None=None):
        self.api_key = api_key or API_KEY

        if not self.api_key :
            raise EIAAuthError(
                "EIA_API is not set :/"
                "Export it before running: export EIA_API=HDHASHDHAHSDHHASJDHASHJDHASH"
            )
        
        #self.base_url = (base_url or EIA_BASE_URL).rstrip("/")
        # DESPUÉS
        self.base_url = (base_url or os.getenv("EIA_BASE_URL") or "https://api.eia.gov/v2").rstrip("/")

        self.session = self._build_session()
        logger.info(
            "EIAClient ready | endpoints=%s | key=%s",
            self.base_url,
            self.api_key[:6], # importante mostrar solo del 0-6 caracteres
        )
    
    """
    We ought to build a requests.Session with automatic retry on transient server errors.

    Retries up to retries times with exponential backoff on 5xx responses and connectin failures.
    """
    def _build_session (self, retries : int = 2, backoff : float = 1.0) -> requests.Session:
        
        session = requests.Session() #creamos una sesion / creating one session for our client

        retry = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_redirect=False # we handle status ourselves in _get
        )

        adapter = HTTPAdapter(
            max_retries=retry
        )

        session.mount("https://",adapter)
        session.mount("http://", adapter)

        return session
    
    #Helpers HTTP
    
    def __get(self, route : str, params : dict[str, Any]) -> dict :
        url = f"{self.base_url}/{route}/data"

        #we need to inject the key with no mutating the caller's dict
        full_params = {**params, "api_key" : self.api_key}

        logger.debug(
            "GET %s | params=%s",
            url,
            { k: v for k, v in full_params.items() if k != "api_key"}
        )

        try:
            resp = self.session.get(url, params=full_params, timeout=30)
        
        except requests.exceptions.ConnectionError as exc:
            raise EIANetworkError(f"Connection failed : {exc} ") from exc
        except requests.exceptions.Timeout as exc:
            raise EIANetworkError(f"Request timed out after 30 s: {exc}") from exc
        
        if resp.status_code == 401:
            raise EIAAuthError("EAI returned 401 Unauthorized. Verify your EIA_APIK")
        
        if(resp.status_code == 403):
            raise EIAAuthError("EIA returned Forbidden. Your key may lac access to this route.")
        if resp.status_code == 404:
            raise EIAAuthError(f"Route not found {url}")
        
        resp.raise_for_status()

        return resp.json()
    
    #Pagination

    def paginate(
            self,
            route: str,
            extra_params: dict[str, Any] | None=None,
            page_size : int= 5000,
    ) -> Generator[list[dict], None, None]:
        
        params ={
            "length" : page_size,
            "offset" : 0,
            **(extra_params or {}),
        }

        total_fetched = 0

        while True:
            try:
                body = self.__get(route, params)
            except EIANetworkError as exc:
                logger.error("Network error at offset %d : %s", params["offset"], exc)

                raise
            response_data = body.get("response",{})
            records : list[dict] = response_data.get("data", [])

            total : int = int(response_data.get("total", 0) or 0)

            if not records:
                logger.info("Empty page at offset %d ---- pagination complete. :3", params["offset"])
                break
            yield records

            total_fetched += len(records)
            logger.info("Pagination %s | fetched %d / %d (offset=%d)",
                        route, total_fetched, total or total_fetched, params["offset"],)
            
            if total and total_fetched >= total:
                break
            params["offset"] += page_size

            time.sleep(0.2) #rate-limiting between pages

    def _outage_params(
            self,
            start_date : str | None,
            end_date : str | None,
            data_fields : list[str],

    ) -> dict[str, Any]:
        
        """
        Build the common query-parameter block shared by all three outage routes.

        data_fields control which metric columns the API includes in the response.
        """

        params : dict[str, Any] ={
            "frequency" : "daily",
            "sort[0][column]" : "period",
            "sort[0][direction]" : "asc",
        }

        for i, field in enumerate(data_fields):
            params[f"data[{i}]"] = field
        if start_date:
            params["start"] = start_date
        if end_date:
            params["end"] = end_date
        
        return params
    
    def get_generator_outages(
            self,
            start_date : str | None=None,
            end_date : str | None=None,
    ) -> Generator[list[dict], None, None] :
        
        """
        Yiel generator unit level outage records.

        Response fields per record:

        period, facility,(int), facilityName, generator int,
        capacity, outage, percenOutage, 
        capacity-units, outage-units, percentOutage-units
        """

        params = self._outage_params(
            start_date,
            end_date,
            data_fields=["capacity", "outage", "percentOutage"],
        )

        logger.info(
            "Fetching generator outages | start=%s end=%s",
            start_date or "all", end_date or "all",
        )

        yield from self.paginate(ROUTE_GENERATOR_OUTAGES, extra_params=params)

    def get_facility_outages(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Generator[list[dict], None, None]:
        """
        Yield facility (plant) level outage records.
 
        Response fields per record:
          period, facility (int), facilityName,
          capacity, outage, percentOutage,
          capacity-units, outage-units, percentOutage-units
        """
        params = self._outage_params(
            start_date, end_date,
            data_fields=["capacity", "outage", "percentOutage"],
        )
        logger.info(
            "Fetching facility outages | start=%s end=%s",
            start_date or "all", end_date or "all",
        )
        yield from self.paginate(ROUTE_FACILITY_OUTAGES, extra_params=params)
 
    def get_us_outages(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> Generator[list[dict], None, None]:
        """
        Yield US national aggregate outage records (one row per day).
 
        Response fields per record:
          period,
          capacity, outage, percentOutage,
          capacity-units, outage-units, percentOutage-units
 
        These are the daily nationwide totals pulled from the NRC Power
        Reactor Status Report. One record per day — no facility or generator
        dimension. Stored in us_totals.parquet.
        """
        params = self._outage_params(
            start_date, end_date,
            data_fields=["capacity", "outage", "percentOutage"],
        )
        logger.info(
            "Fetching US national outages | start=%s end=%s",
            start_date or "all", end_date or "all",
        )
        yield from self.paginate(ROUTE_US_OUTAGES, extra_params=params)


