import logging

from config import API_TOKEN, ENGINE_TYPE
from redcap import RedCapSession
from transform import transform_data
from db import DatabaseManager

def main():
  f_handler = logging.FileHandler('debug.log')
  f_handler.setLevel(logging.INFO)
  f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  f_handler.setFormatter(f_format)

  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  logger.addHandler(f_handler)

  if ENGINE_TYPE == 'sqlserver':
    from config import SERVER, DATABASE
    db = DatabaseManager(engineType=ENGINE_TYPE, server=SERVER, database=DATABASE)
  elif ENGINE_TYPE == 'mysql':
    from config import USER, PASSWORD, MYSQL_DATABASE, IP_ADDRESS
    db = DatabaseManager(engineType=ENGINE_TYPE, server=IP_ADDRESS, database=MYSQL_DATABASE, user=USER, password=PASSWORD) 
  
  api = RedCapSession(API_TOKEN)
  enrollment_data, survey_data = transform_data(api.exportRecord(rawOrLabel='label'))

  try:
    enrollment_data.to_sql('tbl_NAV_COVID19_RedCap_Enrollment', db.engine, if_exists='replace') # Replace table with new export. 
    logger.info("Successfully updated COVID19_ININD_BV_REPORT_ENROLLMENT")
    survey_data.to_sql('tbl_NAV_COVID19_RedCap_Survey', db.engine, if_exists='replace') # If data becomes large, change to INSERT_OR_UPDATE
    logger.info("Successfully updated COVID19_ININD_BV_REPORT_SURVEY")
  except:
    logger.warning("FAILED TO UPDATE TABLES")
if __name__ == '__main__':
  main()

