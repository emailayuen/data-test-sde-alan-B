
from google.cloud import bigquery
import os
import json

PROJECT_ID = 'data-test-sde'
KEY_FILE_PATH = 'key/data-test-sde-f4c4c4a2b7da.json'
DATASET_RAW = 'raw'

def get_biquery_client():
    '''
    return client connection to bigquery
    '''
    client = bigquery.Client.from_service_account_json(KEY_FILE_PATH, project=PROJECT_ID)
    return client

def execute_biquery_insert(ds_name, tbl_name, rows_to_insert):
    '''
    wrapper to execute bigquery table insert
    '''
    client = get_biquery_client()
    # Prepares a reference to the dataset
    ds_ref = client.dataset(ds_name)
    tbl_ref = ds_ref.table(tbl_name)
    table = client.get_table(tbl_ref)  # API call
    errors = client.insert_rows(table, rows_to_insert)  # API request
    return errors

def set_response(code, body):
    resp = {}
    resp['statusCode']=code
    resp['body']=body
    return resp

def lambda_handler(event, context):
  try:
    params=event["queryStringParameters"]
    table = params["table_name"]
    rows = []
    params.pop("table_name", None)
    rows.append(params)
    errors = execute_biquery_insert(DATASET_RAW, table, rows)
    if len(errors)==0:
        response = set_response(200, 'Row added successfully')
    else:
        response = set_response(404, errors[0])
  except Exception as ex:
      response = set_response(500, ex)
  return response

if __name__ == '__main__':
  '''
  event = {
    "queryStringParameters": {
      "category_id": "4", 
      "external_id": "123", 
      "name": "banana lipstick", 
      "table_name": "products", 
      "type": "product"
    }
  }
  '''
  event = {
    "queryStringParameters": {
      "sku_id": "7777", 
      "inventory_count": "77", 
      "table_name": "inventory_items"
    }
  }
  lambda_handler(event, None)