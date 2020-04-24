import requests
import json
import logging

class RedCapSession():
  """RedCapSession class with methods to export, import, and delete records"""
  def __init__(self, token):
    self.url = 'https://redcap.ascension.org/inind/api/'
    self.session = requests.Session()
    self.token = token
    self.logger = logging.getLogger(__name__)

  def exportRecord(self, record_ids = [], rawOrLabel='label', rawOrLabelHeaders='label'):
    data = {
      'token': self.token,
      'format': 'json',
      'type': 'flat',
      'rawOrLabel':rawOrLabel,
      'rawOrLabelHeaders': rawOrLabel,
      'content': 'record',
      'records': record_ids,
    }
  
    r = self.session.post(self.url, data=data)
    
    try:
      return r.json()
    except:
      self.logger.error("Export Record | Status: {} | Message: {}".format(r.status_code, r.text))
      return r.status_code

  def importRecord(self, record):
    record = json.dumps([record]) # Convert record to JSON
    
    data = {
      'token': self.token,
      'format': 'json',
      'type': 'flat',
      'content': 'record',
      'overwriteBehavior':'normal',
      'data':record
    }
    
    r = self.session.post(self.url, data=data)
    
    try:
      return r.status_code
    except:
      self.logger.error("Import Record | Status: {} | Message: {}".format(r.status_code, r.text))
      return r.status_code

  def deleteRecord(self, record_ids):
    assert type(record_ids) == list
    assert len(self.exportRecord(record_ids)) == len(record_ids) # Validate record id exists
    assert len(record_ids) == 1

    data = {
      'token': self.token,
      'action': 'delete',
      'content': 'record',
      'records[0]': record_ids
    }
    
    r = self.session.post(self.url, data=data)

    try:
      return r.json()
    except:
      self.logger.error("Delete Record | Status: {} | Message: {}".format(r.status_code, r.text))
      return r.status_code

  @staticmethod
  def validateRecord(record):
    keys = ['record_id', 'name', 'email', 'abst_site', 'activity', 'event_number', 'oth_explain', 'in_training', 'reg_number', 'pt_name', 'px_date', 'dis_date', 'abs_del', 'start_time', 'finish_time', 'time_check', 'posi_nega', 'case_complete', 'notes', 'data_abstraction_time_log_data_collection_complete'] # Survey specific keys
    if all(key in keys for key in record.keys()):
      return True
    else:
      return False

### Rate Limiting functions for redcap, need to weave them into export, import, delete
import time, threading
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
MAX_RATE = 5 # Requests per second, 300 requests per minute

def RateLimited(max_per_second):
  lock = threading.Lock()
  minInterval = 1.0 / float(max_per_second)
  
  def decorate(func):
    lastTimeCalled = [0.0]
    def rateLImitedFunction(*args, **kwargs):
      lock.acquire()
      elapsed = time.clock() - lastTimeCalled[0]
      leftToWait = minInterval - elapsed

      if leftToWait > 0:
        time.sleep(leftToWait)
      
      lock.release()
      ret = func(*args, **kwargs)
      lastTimeCalled[0] = time.clock()
      return ret
    return rateLImitedFunction
  return decorate

@RateLimited(5)
def task(**kwargs):
  redcap = kwargs.get('redcap')
  item = kwargs.get('item')
  row = redcap.importRecord({'record_id':item,'abst_site':37})
  print("Task completed for {}. Status Code: {}".format(item, row))
  time.sleep(1)
  if row != 200:
    raise Exception('Did not receive status code 200, recieved {}.'.format(row))
  return row

def post_data_asynchronous():
  #token = config.api_token
  rc = RedCapSession(token)
  # Get all records
  # data = rc.exportRecord([], rawOrLabel='label')
  # # Filter records where date is greater than 7/1/2019 and case_complete is not 'No' or 'Yes'
  # items = []

  # # for record in data:
  # #   date = record.get('start_time')
  # #   complete = record.get('case_complete')
  # #   if len(date) > 0:
  # #     if (dt.datetime.strptime(date, '%Y-%m-%d %H:%M') > dt.datetime(2019, 7, 1, 0, 0, 0)) and (complete not in ['Yes', 'No']):
  # #       print(record)
  # #       items.append(record.get('record_id'))

  # Get some records: 
  #items = config.items
  #data = rc.exportRecord([21], rawOrLabel='label')

  print('Items to be changed: {}'.format(len(items)))

  with ThreadPoolExecutor(max_workers=3) as executor:
    jobs = []
    results = []

    for item in items:
      kw = {'redcap': rc, 'item': item}
      jobs.append(executor.submit(task, **kw))

    for job in futures.as_completed(jobs):
      x = job.result()
      results.append(x)

    print("{} jobs completed.".format(len(jobs)))

def main():
  post_data_asynchronous()

if __name__ == '__main__':
  main()
 