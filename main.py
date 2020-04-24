from config import API_TOKEN
from redcap import RedCapSession
from transform import transform_data
from db import DatabaseManager

def main():
  api = RedCapSession(API_TOKEN)
  db = DatabaseManager(engineType='mysql')
  enrollment_data, survey_data = transform_data(api.exportRecord(rawOrLabel='label'))

  enrollment_data.to_sql('COVID19_ININD_BV_REPORT_ENROLLMENT', db.engine, if_exists='replace')
  survey_data.to_sql('COVID19_ININD_BV_REPORT_SURVEY', db.engine, if_exists='replace')

if __name__ == '__main__':
  main()


