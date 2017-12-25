'''
Created on Nov 5, 2017

@author: nwe.nganesan
'''

from google.cloud import bigquery
from GCS import x

if __name__ == '__main__':
  print(x)
  bigquery_client = bigquery.Client(project='poc-tier1')
  
  #List All Datasets.
  for dataset in bigquery_client.list_datasets():  # API request(s)
    print(dataset)
        
  #Create a new dataset
  DATASET_ID = 'test_api1'
  dataset_ref = bigquery_client.dataset(DATASET_ID)
  dataset = bigquery.Dataset(dataset_ref)
  dataset.description='test_Dataset'
  #dataset = bigquery_client.create_dataset(dataset)

  #Delete a dataset
  #bigquery_client.delete_dataset(dataset)
    
  tables = list(bigquery_client.list_dataset_tables(dataset))
  print(tables)
  
  #Create table without schema
  table_ref = dataset.table('placeiq123_copy')
  table = bigquery.Table(table_ref)
  #table.view_query = QUERY
  #bigquery_client.create_table(table)
  tables = list(bigquery_client.list_dataset_tables(dataset))
  
  #Create table with Schema
  SCHEMA = [
    bigquery.SchemaField('full_name', 'STRING', mode='required'),
    bigquery.SchemaField('age', 'INTEGER', mode='required'),
    ]
  #table_ref = dataset.table('placeiq123_copy')
  #table = bigquery.Table(table_ref, schema=SCHEMA)
  #table = bigquery_client.create_table(table)
  
  table = bigquery_client.get_table(table) 
  print(table)
  
  #for row in bigquery_client.list_rows(table):
    #print(row)
 
  
  
  
  
