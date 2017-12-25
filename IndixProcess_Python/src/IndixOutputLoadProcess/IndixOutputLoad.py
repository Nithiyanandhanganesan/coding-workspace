'''
Created on Nov 8, 2017

@author: nwe.nganesan
'''
import zlib
import json
import sys
from IndixOutputLoadProcess.BQClient import BQClient
from IndixOutputLoadProcess.GCSClient import GCSClient
from IndixOutputLoadProcess import IndixConstant

if __name__=='__main__':

    bigqueryClient=BQClient()
    storageClient=GCSClient('poc-tier1')
    

    #Return reference to the bucket
    #bucket_ref=storage_client.gcs_client.bucket("testbucketdeletemeuseless")
    bucket=storageClient.getBucketRef(IndixConstant.BUCKET_NAME)
    print(bucket)
    
    #Return blob object
    blob=bucket.get_blob('indix/nordstrom_all_product/nordstorm_indix_out.json.gz')
    test=blob.download_as_string()
    #test1=str(test,'utf-8')

    
    #Read gz compressed file and convert it into string for further process.
    decompressed_string=zlib.decompress(test,16+zlib.MAX_WBITS)
    test1=str(decompressed_string,'utf-8')
    
    
    print("indix")
    value=''
    list1=[]
    for x in test1.split("\n"):
        if len(x)!=0:
            j1=json.loads(x)
            addition_attr_update=j1['additionalAttributes']
            xx=str(addition_attr_update).replace("{",'[').replace("}", "]")
            for z in str(xx).split(","):
                value=value + z.replace("': '","=>") + ','
                value1=value.rstrip(",")

            j1['additional_attributes']=value1
            del j1['additionalAttributes']
            
            value=''
            standard_attr_update=j1['standardizedAttributes']
            xx=str(standard_attr_update).replace("{",'[').replace("}", "]")
            for z in str(xx).split(","):
                value=value + z.replace("': '","=>") + ','
                value1=value.rstrip(",")

            j1['standardized_attributes']=value1
            del j1['standardizedAttributes']
            
            
            value=''
            variant_attr_update=j1['variantAttributes']
            xx=str(variant_attr_update).replace("{",'[').replace("}", "]")
            for z in str(xx).split(","):
                value=value + z.replace("': '","=>") + ','
                value1=value.rstrip(",")

            j1['variant_attributes']=value1
            del j1['variantAttributes']
            
            
            
            
            if j1['indix_matching']=='yes':
                #list1.append(json.loads(x))
                #list1.append(json.dumps (json.loads(x)))
                list1.append(json.dumps(j1))
            else:
                continue
        else:
            continue 
        
    bucket = storageClient.gcs_client.get_bucket('testbucketdeletemeuseless')  
    blob=bucket.get_blob('indix/nordstrom_all_product/nordstrom_indix_out.json')
    

    #Write to local file
    f = open('/Users/nwe.nganesan/Desktop/junk/junk/test11.txt', 'w')     

    for y in list1:
        #print(y)
        f.write("%s\n" % str(y))
    #f.close()        #f.write("%s\n" % str(y))    
    f.close()
    #with open(f,'rw') as my_file:
    f1='/Users/nwe.nganesan/Desktop/junk/junk/test11.txt'
    blob.upload_from_filename(f1)
    
    sys.exit()
    bigqueryClient.executeDataLoadFromGCS()
    
    query="insert `indix.prod_category_map_test1`  (master_indix_product_id, product_id, retailer_name, retailer_id, `timestamp`, \
            indix_matching,categorynamepath, \
            cat_lvl_1, cat_lvl_2, cat_lvl_3, cat_lvl_4, cat_lvl_5, cat_lvl_6, cat_lvl_7, cat_lvl_8, cat_lvl_9, cat_lvl_10, \
            brandname, brandtext) \
            select distinct \
            mpid as master_indix_product_id, \
            input_sku as product_id , \
            'nordstrom' as retailer_name, \
            '26111' as retailer_id, \
            created_dt as `timestamp`, \
            a.indix_matching, \
            a.categorynamepath \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(0)] as cat_lvl_1 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(1)] as cat_lvl_2 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(2)] as cat_lvl_3 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(3)] as cat_lvl_4 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(4)] as cat_lvl_5 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(5)] as cat_lvl_6 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(6)] as cat_lvl_7 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(7)] as cat_lvl_8 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(8)] as cat_lvl_9 \
            ,split(a.categorynamepath,'>')[SAFE_OFFSET(9)] as cat_lvl_10 \
            ,a.brandname \
            ,a.brandtext \
            from \
            `indix.test_indix7_test`  a \
            left JOIN \
            `indix.prod_category_map_test1`  b \
            ON a.input_sku = b.product_id \
            and a.mpid=b.master_indix_product_id \
            and b.retailer_id='26111' \
            where b.product_id is null";

    query_result=bigqueryClient.executeQuery(query)
    
    #execute query and load result into table.
    query="UPDATE `indix.prod_category_map_test1` a \
            SET \
            `timestamp`=b.created_dt , \
            indix_matching=b.indix_matching, \
            categorynamepath=b.categorynamepath, \
            cat_lvl_1=split(b.categorynamepath,'->')[SAFE_OFFSET(0)], \
            cat_lvl_2=split(b.categorynamepath,'->')[SAFE_OFFSET(1)], \
            cat_lvl_3=split(b.categorynamepath,'->')[SAFE_OFFSET(2)], \
            cat_lvl_4=split(b.categorynamepath,'->')[SAFE_OFFSET(3)], \
            cat_lvl_5=split(b.categorynamepath,'->')[SAFE_OFFSET(4)], \
            cat_lvl_6=split(b.categorynamepath,'->')[SAFE_OFFSET(5)], \
            cat_lvl_7=split(b.categorynamepath,'->')[SAFE_OFFSET(6)], \
            cat_lvl_8=split(b.categorynamepath,'->')[SAFE_OFFSET(7)], \
            cat_lvl_9=split(b.categorynamepath,'->')[SAFE_OFFSET(8)], \
            cat_lvl_10=split(b.categorynamepath,'->')[SAFE_OFFSET(9)], \
            brandname=b.brandname , \
            brandtext=b.brandtext \
            from \
            (select distinct created_dt,indix_matching,categorynamepath ,brandname,brandtext,input_sku,mpid from \
          `indix.test_indix7_test`) b \
            where a.product_id=b.input_sku \
            and a. master_indix_product_id=b.mpid \
            and a.retailer_id='26111' \
            and coalesce(a.categorynamepath,'NULL')!=coalesce(b.categorynamepath,'NULL')"
    query_result=bigqueryClient.executeQuery(query)
    
   
    print(query_result)

    
    sys.exit()
    #Check for the existance of the bucket.
    #bucket = storageClient.gcs_client.get_bucket('testbucketdeletemeuseless')  
    #print(bucket)
     
    #Check for the existance of the bucket
    #test1=storageClient.gcs_client.lookup_bucket('testbucketdeletemeuseless')
    #print(test1)
    
  
    #print(test1)
    
    #test for indix
    print("indix")
    value=''
    list1=[]
    for x in test1.split("\n"):
        if len(x)!=0:
            j1=json.loads(x)
            addition_attr_update=j1['additionalAttributes']
            xx=str(addition_attr_update).replace("{",'[').replace("}", "]")
            for z in str(xx).split(","):
                value=value + z.replace("': '","=>") + ','
                value1=value.rstrip(",")

            j1['additional_attributes']=value1
            del j1['additionalAttributes']
            
            value=''
            standard_attr_update=j1['standardizedAttributes']
            xx=str(standard_attr_update).replace("{",'[').replace("}", "]")
            for z in str(xx).split(","):
                value=value + z.replace("': '","=>") + ','
                value1=value.rstrip(",")

            j1['standardized_attributes']=value1
            del j1['standardizedAttributes']
            
            
            value=''
            variant_attr_update=j1['variantAttributes']
            xx=str(variant_attr_update).replace("{",'[').replace("}", "]")
            for z in str(xx).split(","):
                value=value + z.replace("': '","=>") + ','
                value1=value.rstrip(",")

            j1['variant_attributes']=value1
            del j1['variantAttributes']
            
            
            
            
            if j1['indix_matching']=='yes':
                #list1.append(json.loads(x))
                #list1.append(json.dumps (json.loads(x)))
                list1.append(json.dumps(j1))
            else:
                continue
        else:
            continue 
    f = open('/Users/nwe.nganesan/Desktop/junk/junk/test10.txt', 'w')     

    for y in list1:
        #print(y)
        f.write("%s\n" % str(y))
    f.close()        #f.write("%s\n" % str(y))
    
    bigqueryClient.executeDataLoadFromGCS()
    
    
    
    #Load data from GCS into BigQuery
    #bigqueryClient.executeDataLoadFromGCS()
    
    #Execute the query and return list of rows.
    query="select distinct lower(registration_attributes.email) as email from `raw_retailnext.enrolled_users` where ((registration_attributes.email is not null) or (trim(registration_attributes.email)!=''))"
    #query_result=bigqueryClient.executeQuery(query)
    print("query completed")
    
    #Attach uuid to each records
    #record_uuid_attach=[]
    #for x in query_result:
    #    test= uuid.uuid3(type('', (), dict(bytes=b''))(), x[0])
    #    record_uuid_attach.append((x[0],str(test)))
    #print("party id generated")      

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
 