import pandas as pd
import numpy as np
import logging
import datetime

def transform_data(redcap_data_json):
  """Transform process from redcap data to enrollment and survey dataframes"""
  redcap_data = pd.DataFrame(redcap_data_json)
  redcap_data.to_csv('data.csv')

  # Split data by event type
  enrollment_data = redcap_data[redcap_data['redcap_event_name']=='Enrollment']
  survey_data = redcap_data[redcap_data['redcap_event_name']!='Enrollment']
  survey_data['redcap_event_name'] = survey_data['redcap_event_name'].apply(lambda x: datetime.datetime(int(x[0:4]), int(x[5:7]), int(x[8:10]), 1, 0) if x[11:13] == "AM" else datetime.datetime(int(x[0:4]), int(x[5:7]), int(x[8:10]), 13, 0))

  # Exclude/Include relevant columns
  enrollment_data = enrollment_data[
    [
      'record_id',
      'redcap_event_name',
      'ministry',
      'facility',
      'email',
      'am_email_1',
      'am_email_2',
      'am_email_3',
      'am_email_4',
      'am_email_5',
      'am_email_6',
      'pm_email_1',
      'pm_email_2',
      'pm_email_3',
      'pm_email_4',
      'pm_email_5',
      'pm_email_6',
      'april_trigger',
      'may_trigger',
      'june_trigger',
      'july_trigger',
      'contact_market_facility_complete'
    ]
  ]

  survey_data = survey_data.drop(
    [
      'email',
      'am_email_1',
      'am_email_2',
      'am_email_3',
      'am_email_4',
      'am_email_5',
      'am_email_6',
      'pm_email_1',
      'pm_email_2',
      'pm_email_3',
      'pm_email_4',
      'pm_email_5',
      'pm_email_6',
      'april_trigger',
      'may_trigger',
      'june_trigger',
      'july_trigger',
      'contact_market_facility_complete'
    ], axis=1)

  # Fill Missing Data
  survey_data = survey_data.replace("", np.nan) # Replace all empty strings with nan
  survey_data = survey_data.fillna(survey_data.groupby('record_id').ffill()) # Forward fill missing data
  survey_data = survey_data.fillna(survey_data.groupby('record_id').bfill()) # Backward fill missing data

  # Fill in survey data ministry and facility
  facility_params = {
    '40011': {'facility': 'StV Evansville Med Ctr', 'ministry': 'ININD'},
    '40017': {'facility': 'StV Warrick Hosp', 'ministry': 'ININD'},
    '46006': {'facility': 'StV Heart Ctr Indiana', 'ministry': 'ININD'},
    '46013': {'facility': 'St Vincent Seton Spec Hosp Indianapolis', 'ministry': 'ININD'},
    '46018': {'facility': 'StV Kokomo Hosp', 'ministry': 'ININD'},
    '46019': {'facility': 'StV Anderson Reg Hosp', 'ministry': 'ININD'},
    '46024': {'facility': 'StV Carmel Hosp', 'ministry': 'ININD'},
    '46029': {'facility': 'StV Indianapolis Hosp', 'ministry': 'ININD'},
    '46033': {'facility': 'StV Jennings Hosp', 'ministry': 'ININD'},
    '46036': {'facility': 'StV Mercy Hosp', 'ministry': 'ININD'},
    '46042': {'facility': 'StV Salem Hosp', 'ministry': 'ININD'},
    '46043': {'facility': 'StV Stress Ctr', 'ministry': 'ININD'},
    '46045': {'facility': 'StV Williamsport Hosp', 'ministry': 'ININD'},
    '46047': {'facility': 'StV Clay Hosp', 'ministry': 'ININD'},
    '46048': {'facility': 'StV Randolph Hosp', 'ministry': 'ININD'},
    '46052': {'facility': 'StV Dunn Hosp', 'ministry': 'ININD'},
    '46055': {'facility': 'StV Fishers', 'ministry': 'ININD'}
  }

  survey_data['ministry'] = survey_data.apply(lambda x: facility_params.get(x['record_id'], {}).get('ministry'), axis=1)
  survey_data['facility'] = survey_data.apply(lambda x: facility_params.get(x['record_id'], {}).get('facility'), axis=1)

  return enrollment_data, survey_data
