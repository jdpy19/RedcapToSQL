from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Date, Float, Integer, String, Boolean
from sqlalchemy.orm import sessionmaker
import logging

from config import SERVER, DATABASE, USER, PASSWORD, IP_ADDRESS, MYSQL_DATABASE

Base = declarative_base()

def create_sqlserver_engine():
  """Return a SQL server engine"""
  driver = 'ODBC Driver 17 for SQL Server'
  engine_string = 'mssql+pyodbc://{server}/{database}?driver={driver}'.format(server=SERVER, database=DATABASE, driver=driver)
  engine = create_engine(engine_string)
  return engine

def create_mysql_engine():
  engine_string = "mysql://{user}:{password}@{ip}/{database}".format(user=USER, password=PASSWORD, ip=IP_ADDRESS, database=MYSQL_DATABASE)
  engine = create_engine(engine_string, echo=True)
  return engine

class DatabaseManager:
  def __init__(self, engineType='sqlserver'):
    self.logger = logging.getLogger(__name__)

    if engineType == 'sqlserver':
      self.engine = create_sqlserver_engine()
    elif engineType == 'mysql':
      self.engine = create_mysql_engine()
    else:
      try: 
        raise ValueError
      except:
        self.logger.error("ValueError | Invalid Engine | EngineType: {}".format(engineType))

    self.Base = Base
    self.Base.metadata.create_all(self.engine)
    self.Session = sessionmaker(bind=self.engine)
    self.session = self.Session()

class Enrollment(Base):
  """Create a table for Enrollment events from redcap survey"""
  __tablename__ = "COVID19_ININD_BV_REPORT_ENROLLMENT"
  # Columns  
  id = Column(Integer, primary_key=True)
  record_id = Column(String(15), index=True)
  redcap_event_name = Column(String(25))
  ministry = Column(String(10))
  facility = Column(String(50))
  email = Column(String(50))
  am_email_1 = Column(String(50))
  am_email_2 = Column(String(50))
  am_email_3 = Column(String(50))
  am_email_4 = Column(String(50))
  am_email_5 = Column(String(50))
  am_email_6 = Column(String(50))
  pm_email_1 = Column(String(50))
  pm_email_2 = Column(String(50))
  pm_email_3 = Column(String(50))
  pm_email_4 = Column(String(50))
  pm_email_5 = Column(String(50))
  pm_email_6 = Column(String(50))
  april_trigger = Column(String(5))
  may_trigger = Column(String(5))
  june_trigger = Column(String(5))
  july_trigger = Column(String(5))
  contact_market_facility_complete = Column(String(15))

class Survey(Base):
  """Create a table for survey events from redcap survey"""
  __tablename__ = "COVID19_ININD_BV_REPORT_SURVEY"
  # Columns
  id = Column(Integer, primary_key=True)
  record_id = Column(String(15), index=True)
  redcap_event_name = Column(String(25))
  ministry = Column(String(10))
  facility = Column(String(50))
  am_detector = Column(Integer)
  facility_event = Column(String(50)) # Could be converted into a datetime
  icu_bed_cap = Column(Integer)
  icu_bed_staffed = Column(Integer)
  icu_bed_occupied = Column(Integer)
  pcu_bed_cap = Column(Integer)
  pcu_bed_staffed = Column(Integer)
  pcu_bed_occupied = Column(Integer)
  medsurg_bed_cap = Column(Integer)
  medsurg_bed_staffed = Column(Integer)
  medsurg_bed_occupied = Column(Integer)
  ed_bed_cap = Column(Integer)
  ed_bed_staffed = Column(Integer)
  ed_bed_occupied = Column(Integer)
  other_bed_cap = Column(Integer)
  other_bed_staffed = Column(Integer)
  other_bed_occupied = Column(Integer)
  ped_bed_cap = Column(Integer)
  ped_bed_staffed = Column(Integer)
  ped_bed_occupied = Column(Integer)
  total_bed_calc_cap = Column(Integer)
  total_bed_calc_staffed = Column(Integer)
  total_bed_calc_occupied = Column(Integer)
  adult_bed_calc_cap = Column(Integer)
  adult_bed_calc_staffed = Column(Integer)
  adult_bed_calc_occupied = Column(Integer)
  inpatient_bed_calc_cap = Column(Integer)
  inpatient_bed_calc_staffed = Column(Integer)
  inpatient_bed_calc_occupied = Column(Integer)
  ahnat_medsurg_cap_calc = Column(Integer)
  total_bed_cap = Column(Integer)
  staff_flag = Column(Boolean)
  occupied_flag = Column(Boolean)
  tier_1_available = Column(Integer)
  tier_1_inuse = Column(Integer)
  tier_1_repair = Column(Integer)
  tier_1_total = Column(Integer)
  tier_2_available = Column(Integer)
  tier_2_inuse = Column(Integer)
  tier_2_repair = Column(Integer)
  tier_2_total = Column(Integer)
  tier_3_available = Column(Integer)
  tier_3_inuse = Column(Integer)
  tier_3_repair = Column(Integer)
  tier_3_total = Column(Integer)
  tier_4_available = Column(Integer)
  tier_4_inuse = Column(Integer)
  tier_4_repair = Column(Integer)
  tier_4_total = Column(Integer)
  tier_5_available = Column(Integer)
  tier_5_inuse = Column(Integer)
  tier_5_repair = Column(Integer)
  tier_5_total = Column(Integer)
  vent_inventory_total = Column(Integer)
  vent_inuse_total = Column(Integer)
  vent_repair_total = Column(Integer)
  vent_total = Column(Integer)
  icu_percent_occupied = Column(Float)
  pcu_percent_occupied = Column(Float)
  med_surg_percent_occupied = Column(Float)
  all_ed_percent_occupied = Column(Float)
  other_percent_occupied = Column(Float)
  peds_percent_occupied = Column(Float)
  total_avail_percent_occupied = Column(Float)
  total_exclude_percent_occupied = Column(Float)
  alaris_module_use = Column(Integer)
  alaris_module_on_hand = Column(Integer)
  alaris_pump_use = Column(Integer)
  alaris_pump_on_hand = Column(Integer)
  mech_vent_use_ped = Column(Integer)
  mech_vent_on_hand_ped = Column(Integer)
  mech_vent_use_adult = Column(Integer)
  mech_vent_on_hand_adult = Column(Integer)
  bipap_use = Column(Integer)
  bipap_on_hand = Column(Integer)
  hiflow_o2_use = Column(Integer)
  hiflow_o2_on_hand = Column(Integer)
  telemetry_use = Column(Integer)
  telemetry_on_hand = Column(Integer)
  hardwired_use = Column(Integer)
  hardwired_on_hand = Column(Integer)
  pulse_ox_use = Column(Integer)
  pulse_ox_on_hand = Column(Integer)
  bis_use = Column(Integer)
  bis_on_hand = Column(Integer)
  bad_equipment_flag = Column(Boolean)
  pui_new = Column(Integer)
  pui_current = Column(Integer)
  pui_deaths = Column(Integer)
  covid_new = Column(Integer)
  covid_current = Column(Integer)
  covid_deaths = Column(Integer)
  covid_occupied_flag = Column(Boolean)
  identical_pui_covid_flag = Column(Boolean)
  bed_ventilator_reporting_complete = Column(String(15))

