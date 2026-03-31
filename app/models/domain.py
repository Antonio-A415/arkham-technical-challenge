from dataclasses import dataclass, field
from typing import Optional
from datetime import date, datetime

@dataclass
class Plant:
    plant_id : str
    plant_name : str
    state : Optional[str]
    region : Optional[str]

@dataclass
class Generator:
    generator_id : str
    plant_id : str
    unit_name : str
    total_capacity_mw : Optional[float] = None

@dataclass
class OutageSnapshot:
    snapshot_id : str
    report_date : date
    level : str #En el endpoint tenemos facility, generator, us
    capacity_mw : Optional[float] = None
    outage_mw : Optional[float] = None
    percent_outage : Optional[float] = None
    plant_id : Optional[str] = None
    generator_id : Optional[str] = None

@dataclass
class USTotals :
    report_date : date
    total_capacity_mw : Optional[float] = None
    total_outage_mw : Optional[float] = None
    percent_outage : Optional[float] = None

@dataclass
class RefreshLog:
    triggered_at : datetime
    trigger_source: str #It depends on whether the request comes from the API or i donde manually
    status : str # on running, success, error
    error_msg : Optional[str]
    records_added : int = 0
    run_id : Optional[int] = None
    