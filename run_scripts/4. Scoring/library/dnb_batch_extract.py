# Databricks notebook source
# MAGIC %run ./dnb_utils

# COMMAND ----------

# from studio_utils.batchapi import dnbbatchextract
from dnb.studio.batchextract.service import BATCHExtract
import time
import datetime as dt

# COMMAND ----------

class dnb_batch_extract:
  batch        = None  # batch handle  
  last_request = None  # last request
  last_result  = None  # last result
  
  # initialized batch extract object (using user's workarea)
  def __init__(self):
    self.batch = BATCHExtract(schema.workarea)
    pass
  
  
  # change signature
  # requested_df datafame
  # layout = 'duns_only'
  def request_data_extract(self, request_df, layout, passcode=None):
    # read request_df & save as a file
    request_file = f'dbfs:/FileStore/tables/batch_extract_{dt.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    dnb_dataframe_to_csv(request_df,request_file)

    self.last_request = self.batch.submitRequest(file=request_file, layout=layout, passcode=passcode)
    
    return self.last_request

  
  # check status of last request
  def check_request_status(self, request_id=None):
    # check the status of the request
    status=None
    
    # check last request if request is not given
    if request_id is None:
      request_id = self.get_request_id()
    
    # check status log
    if not request_id is None:
        self.last_result = self.batch.checkRequestLog(request_id)
        
    return self.last_result
  
  # returns last request id
  def get_request_id(self, request=None):
    request_id = ""
    if request is None:
      request = self.last_request

    if not request is None:
        request_id = request['batch']['message']['request_id']
      
    return request_id  

  
  # return last result status
  def get_result_status(self, result=None):
    status = ""
    if result is None:
      result = self.last_result

    if not result is None:
        status = self.last_result['batch']['message'] #['info']
      
    return status 
  
  
  # check if processing is done
  def is_complete(self, request_id=None):
    
    self.check_request_status(request_id)
    
    # todo: check if the request is picked up (check request status)
    
    if (not self.last_result is None) and (self.last_result['batch']['status'] != 404):
      if (self.last_result['batch']['message']['content'][0]['status'] == "SUCCESSFUL"):
        return True
      elif (self.last_result['batch']['message']['content'][0]['status'] == "FAILED"):
        raise Exception('Studio Backend Job Failed!')
      elif (self.last_result['batch']['message']['content'][0]['status'] == "INPROGRESS"):
        return False
      else:
        raise Exception('Studio Backend Job Status Unknown!')
    elif (self.last_result['batch']['status'] == 404):
      return True
    else:
      raise Exception('Studio Backend Job Status Unknown!')
    return False
  
  
  def get_results(self, request_id=None):
    self.check_request_status(request_id)
    if request_id is None:
      request_id = self.get_request_id()
      url = self.last_result['batch']["message"]["content"][0]["output_file_url"]
      df = dnb_csv_to_dataframe(url)
      return df

    if not request_id is None:
      url = self.last_result['batch']["message"]["content"][0]["output_file_url"]
      df = dnb_csv_to_dataframe(url)
      return df
  
  def get_results_location(self, request_id=None):
    self.check_request_status(request_id)
    if request_id is None:
      request_id = self.get_request_id()
      transfer_server = self.last_result['batch']['message']['content'][0]['transfer_server']
      transfer_path = self.last_result['batch']['message']['content'][0]['transfer_path']
      output_file = self.last_result['batch']['message']['content'][0]['output_file']
    
    if not request_id is None:
      transfer_server = self.last_result['batch']['message']['content'][0]['transfer_server']
      transfer_path = self.last_result['batch']['message']['content'][0]['transfer_path']
      output_file = self.last_result['batch']['message']['content'][0]['output_file']
    print(f"Please visit {transfer_server} (in {transfer_path}) to access your output file")
      
  
  def quota_check(self):
    request_id = self.get_request_id()
    quota      = self.batch.checkQuota()
    return quota 

  # retrieve dataframe of prev requests
  def get_request_history(self, days=30):
    history = self.batch.checkLogsHistory()
    
    # map to dictionary
    z = [ { 'request_id':      x['request_id'], 
            'requestor_email': x['email_id'], 
            'request_date':    x['request_date'], 
            'layout_type':     x['output_layout'],
            'status':          x['status'],   
            'licensed_quota':  x['licensed_quota'],
            'used_quota':      x['used_quota'],
#             'remaining_quota': x['remaining_quota'],
            'request_type':    x['requested_duns'].strip(),
            'elapsed_time':    x['elapsed_time'],
            'output':          x['output_file_url'] if (x['status'] == 'SUCCESSFUL') else ''
           } 
         for x in history['batch']['message']['content']]

    df = dnb_dict_to_dataframe(z)
#     df = df.select("request_id", "requestor_email", "request_date", "status", "request_type", "elapsed_time", "output", "licensed_quota", "used_quota", "remaining_quota")
    df = df.select("request_id", "requestor_email", "request_date", "layout_type", "status", "request_type", "elapsed_time", "output", "licensed_quota", "used_quota")
    
    return df    
  

# COMMAND ----------

# end of notebook