'''
Created on Nov 8, 2017

@author: nwe.nganesan
'''
import re
import os
import datetime
import sys
import uuid

from google.cloud import bigquery

from ApplicationConstant import *

class BQClient:
    
    def __init__(self):
        self.__get_project_details()
        self.bq_client = bigquery.Client(self.project_name)
        
    def __get_project_details(self):
        self.project_name = PROJECT_NAME
        self.dataset_name = DATASET_NAME
        
    def createDatasetReference(self):
        self.dataset_ref = self.bq_client.dataset(self.dataset_name)
        dataset = bigquery.Dataset(self.dataset_ref)
        return dataset
    
    def createTableReference(self):
        table_ref = self.createDatasetReference().table(TABLE_NAME)
        #table = bigquery.Table(table_ref)
        return table_ref
    
    def readFromGCS(self,bucket_name,blob_name):
        gcs_url='gs://{}/{}'.format(bucket_name,blob_name)
        print(gcs_url)
        return gcs_url
        
    def jobConfig(self):
        job_config = bigquery.LoadJobConfig()
        job_config.create_disposition = 'CREATE_IF_NEEDED'
        job_config.use_legacy_sql=False
        job_config.allowLargeResult=True
        job_config.allowJaggedRows=True
        #job_config.skip_leading_rows = 1
        job_config.maxBadRecords=100
        job_config.source_format = 'NEWLINE_DELIMITED_JSON'
        #job_config.autodetect=True
        job_config.write_disposition = 'WRITE_TRUNCATE'
        #job_config.allowQuotedNewlines=True
        return job_config
    
    def executeDataLoadFromGCS(self):
        #gcs_url=self.readFromGCS('dm-data-cert-poc-tier1/meraki/2017/11/08', '*')
        job_id_prefix = "my_job"
        load_job=self.bq_client.load_table_from_uri(self.readFromGCS(BUCKET_NAME,BLOB_NAME), 
                                           self.createTableReference(), 
                                           job_config=self.jobConfig(), 
                                           job_id_prefix=job_id_prefix)
        
        load_job.result()
    
    def executeQuery(self,query):
        query_job=self.bq_client.query_rows(query)
        rows=list(query_job)
        return rows
    
    def loadTableFromListWithSchema(self,rows_list,SCHEMA):
        self.bq_client.create_rows(self.createTableReference(),rows_list,selected_fields=SCHEMA)
        
    def loadTableFromListWithoutSchema(self,rows_list):
        table=self.bq_client.get_table(self.createTableReference())
        self.bq_client.create_rows(table,rows_list)
        

if __name__=='__main__':

    bigqueryClient=BQClient()
    
    #Load data from GCS into BigQuery
    #bigqueryClient.executeDataLoadFromGCS()
    
    #Execute the query and return list of rows.
    query="select distinct lower(registration_attributes.email) as email from `raw_retailnext.enrolled_users` where ((registration_attributes.email is not null) or (trim(registration_attributes.email)!=''))"
    #query_result=bigqueryClient.executeQuery(query)

    #execute query and load result into table.
    query="UPDATE `indix.product_subcat` a \
            SET categorynamepath = b.categorynamepath , created_dt=b.created_dt \
            from \
            `indix.indix_feed_load_new` b \
            where a.input_sku=b.input_sku \
            and coalesce(a.categorynamepath,'NULL')!=coalesce(b.categorynamepath,'NULL')"
    #query_result=bigqueryClient.executeQuery(query)
    
    query="insert `indix.product_subcat`  (input_sku,categorynamepath,created_dt) \
            select a.input_sku,a.categorynamepath,a.created_dt \
            from \
            `indix.indix_feed_load_new`  a \
            left JOIN \
            `indix.product_subcat`  b \
            ON a.input_sku = b.input_sku \
            where b.input_sku is null;"
    #query_result=bigqueryClient.executeQuery(query)
    #print(query_result)
    #sys.exit()
    
    print("query completed")
    #Attach uuid to each records
    record_uuid_attach=[]
    for x in query_result:
        test= uuid.uuid3(type('', (), dict(bytes=b''))(), x[0])
        record_uuid_attach.append((x[0],str(test)))
    
    print("party id generated")      

    #Load data into table from rows without schema
    bigqueryClient.loadTableFromListWithoutSchema(record_uuid_attach)
    #table = bigqueryClient.bq_client.get_table(bigqueryClient.createTableReference())
    #bigqueryClient.bq_client.create_rows(table,query_result)
    print("data load completed")
    #Load data into table from rows with schema
    SCHEMA = [
        bigquery.SchemaField('mail_id', 'STRING', mode='required'),
        bigquery.SchemaField('party_id', 'STRING', mode='required')
    ]
    #bigqueryClient.loadTableFromListWithSchema(record_uuid_attach, SCHEMA)
    #bigqueryClient.bq_client.create_rows(bigqueryClient.createTableReference(),query_result,selected_fields=SCHEMA)
    
    
    
    
    
    QUERY = (
        'SELECT * FROM `test.test_mer_123`'
         'LIMIT 100')
    QUERY1='select * from `test.test_mer_123` limit 10'
    TIMEOUT = 30  # in seconds
    #query_job = bigqueryClient.bq_client.query_rows(QUERY1)
    #iterator = query_job.result(timeout=TIMEOUT)
    #rows=list(query_job)
    #print(rows[0][1])
    #for row in rows:
    #    print(row[0][23])
    #rows = list(iterator)
    #print(rows)
    #query_job.state()
    
    
    
    
    
#    load_job=bigqueryClient.bq_client.load_table_from_uri(bigqueryClient.readFromGCS('dm-data-cert-poc-tier1/meraki/2017/11/08', 'meraki-043f3d1d-79fb-472a-a402-26f02dd84e18'), 
#                                           bigqueryClient.createTableReference(), 
#                                           job_config=bigqueryClient.jobConfig(), 
#                                           job_id_prefix="my_job")
    
    #bigqueryClient.executeDataLoad()
    #bigqueryClient.executeDataLoad()
    print("done")
    #dataset_connection=BQClient.createDatasetReference(bigqueryClient)

    #tables = list(bigqueryClient.bq_client.list_dataset_tables(bigqueryClient.createDatasetReference()))
    #table = bigqueryClient.bq_client.get_table(bigqueryClient.createTableReference()) 
    
    
    #bigqueryClient.readfromGCS('dm-data-cert-poc-tier1/meraki/2017/11/08', '*')
    
    #table = bigqueryClient.bq_client.get_table(table) 
    #print(table)
 
    
'''
    
    
    bigquery_client = bigquery.Client('poc-tier1')
    
    DATASET_ID = 'test'
    dataset_ref = bigquery_client.dataset(DATASET_ID)
    dataset = bigquery.Dataset(dataset_ref)

    table_ref = dataset.table('test_mer_123')
    table = bigquery.Table(table_ref)
    
    bucket_name='dm-data-cert-poc-tier1/meraki/2017/11/08'
    blob_name='meraki-043f3d1d-79fb-472a-a402-26f02dd84e18'
    GS_URL = 'gs://{}/{}'.format(bucket_name,blob_name)
    print(GS_URL)
    
    job_id_prefix = "my_job"
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = 'NEVER'
    job_config.skip_leading_rows = 1
    #job_config.source_format = 'CSV'
    job_config.autodetect=True
    job_config.write_disposition = 'WRITE_APPEND'
    
    load_job = bigquery_client.load_table_from_uri(GS_URL, table_ref, job_config=job_config,job_id_prefix=job_id_prefix)
    
    load_job.result()
'''
    
    
    
    