'''
Created on Nov 13, 2017

@author: nwe.nganesan
'''
import uuid
from IndixOutputLoadProcess.BQClient import BQClient
from IndixOutputLoadProcess.GCSClient import GCSClient

if __name__=='__main__':

    bigqueryClient=BQClient()
    gcsClient=GCSClient()
    
    #Load data from GCS into BigQuery
    #bigqueryClient.executeDataLoadFromGCS()
    
    #Execute the query and return list of rows.
    query="select distinct lower(registration_attributes.email) as email from `raw_retailnext.enrolled_users` where ((registration_attributes.email is not null) or (trim(registration_attributes.email)!=''))"
    query_result=bigqueryClient.executeQuery(query)

    print("query completed")
    #Attach uuid to each records
    record_uuid_attach=[]
    mail_str=''
    f = open('/Users/nwe.nganesan/Desktop/junk/junk/email_party.txt', 'w')     


    for x in query_result:
        test= uuid.uuid3(type('', (), dict(bytes=b''))(), x[0])
        f.write("%s\n" % str(x[0] + ',' + str(test)))
        #record_uuid_attach.append((x[0],str(test)))
        #mail_str=mail_str + x[0] + ',' + str(test) + '\n'
    f.close()
    print("party id generated")   
    bucket = gcsClient.gcs_client.get_bucket('testbucketdeletemeuseless')  
    blob=bucket.get_blob('retail_test/test_indix.json')
    
    #blob.upload_from_string(mail_str)

    #Load data into table from rows without schema
    #bigqueryClient.loadTableFromListWithoutSchema(record_uuid_attach)
    #table = bigqueryClient.bq_client.get_table(bigqueryClient.createTableReference())
    #bigqueryClient.bq_client.create_rows(table,query_result)
    #print("data load completed")
    #Load data into table from rows with schema
    #SCHEMA = [
    #    bigquery.SchemaField('mail_id', 'STRING', mode='required'),
    #    bigquery.SchemaField('party_id', 'STRING', mode='required')
    #]
    #bigqueryClient.loadTableFromListWithSchema(record_uuid_attach, SCHEMA)
    #bigqueryClient.bq_client.create_rows(bigqueryClient.createTableReference(),query_result,selected_fields=SCHEMA)
    