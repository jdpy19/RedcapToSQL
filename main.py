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

  api = RedCapSession(API_TOKEN)
  db = DatabaseManager(engineType=ENGINE_TYPE)
  enrollment_data, survey_data = transform_data(api.exportRecord(rawOrLabel='label'))

  try:
    enrollment_data.to_sql('COVID19_ININD_BV_REPORT_ENROLLMENT', db.engine, if_exists='replace') # Replace table with new export. 
    logger.info("Successfully updated COVID19_ININD_BV_REPORT_ENROLLMENT")
    survey_data.to_sql('COVID19_ININD_BV_REPORT_SURVEY', db.engine, if_exists='replace') # If data becomes large, change to INSERT_OR_UPDATE
    logger.info("Successfully updated COVID19_ININD_BV_REPORT_SURVEY")
  except:
    logger.warning("FAILED TO UPDATE COVID19_ININ_BV_REPORT TABLES")
if __name__ == '__main__':
  main()


