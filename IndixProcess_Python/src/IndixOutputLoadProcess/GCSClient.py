'''
Created on Nov 8, 2017

@author: nwe.nganesan
'''
import json
import sys
import zlib

from google.cloud import storage
from google.cloud import error_reporting
from ApplicationConstant import *

class GCSClient:
    
    def __init__(self,project_name):
        self.project_name=project_name
        self.gcs_client = storage.Client(self.project_name)
    
    def getBucketRef(self,bucket_name):
        bucket_ref=self.gcs_client.bucket(bucket_name)
        return bucket_ref
    
    def getBucketName(self,bucket_name):
        try:
            bucket = storage_client.gcs_client.get_bucket(bucket_name)
            return bucket
        except Exception:
            print("Sorry, %s bucket does not exist!" % (bucket_name))
            
    def getBlob(self,bucket_name,blob_name):
        blob=self.getBucketName(bucket_name).get_blob(blob_name)
        return blob
    
    def readFromBlobAsString(self,bucket_name,blob_name):
        file_data=self.getBlob(bucket_name, blob_name).download_as_string()
        gcs_data=str(file_data,'utf-8')
        return gcs_data
        

if __name__ == '__main__':
    
    storage_client=GCSClient('poc-tier1')
    print(storage_client.project_name)
    
    
    #Return reference to the bucket
    #bucket_ref=storage_client.gcs_client.bucket("testbucketdeletemeuseless")
    #bucket=storage_client.getBucketName('testbucketdeletemeuseles')
    #print(bucket)
    
    
    #blob=bucket.get_blob('retail_test/indix_build.json.gz')
    blob=storage_client.getBlob('testbucketdeletemeuseless', 'retail_test/indix_build.json.gz')
    #test=blob.download_as_string()
    #test1=str(test,'utf-8')
    
    sys.exit()
    
    
    #batch_ref=storage_client.gcs_client.batch()
    #print(batch_ref)    
    #test=storage_client.gcs_client.current_batch()
    
    
    #test=storage_client.gcs_client.list_buckets()
    #for bucket in test:
    #    print(bucket)
   
        
    #test1=storage_client.gcs_client.lookup_bucket('testbucketdeletemeuseless')
    #print(test1)
    
    blob=bucket.get_blob('retail_test/indix_build.json.gz')
    test=blob.download_as_string()
    #test1=str(test,'utf-8')

    
    #Read gz compressed file and convert it into string for further process.
    decompressed_string=zlib.decompress(test,16+zlib.MAX_WBITS)
    test1=str(decompressed_string,'utf-8')
    
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
    f = open('/Users/nwe.nganesan/Desktop/junk/junk/test.txt', 'w')     

    for y in list1:
        #print(y)
        f.write("%s\n" % str(y))
    f.close()        #f.write("%s\n" % str(y))
            #j1.append({'additionalAttributes'}:addition_attr_update)
            #print(j1)
            #print(j1['additionalAttributes'])
            #for key,value in dict.items(j1):
            #    print(key,value)
                #if key=='additionalAttributes':
                #    value1='['.join(value)
                #else:
                #    list1.append(json.dumps(json.loads(x)))
                #print(key,value1)
                #print(list1)
                #del j1['additionalAttributes']
    sys.exit()           

    
    sys.exit()
    json_obj=json.loads(test1)
    for key,value in dict.items(json_obj.__getitem__("source")):
        print(key,value)
    
    sys.exit()

    #Iterate json and check condition and write into file as json.
    list1=[]
    for x in test1.split("\n"):
        if len(x)!=0:
            j1=json.loads(x)
            #print(j1)
            #if j1['event']['name']=='Website/URL Visited':
            if j1['indix_matching']=='yes':
                #list1.append(json.loads(x))
                list1.append(json.dumps (json.loads(x)))
            else:
                continue
        else:
            continue   
    f = open('/Users/nwe.nganesan/Desktop/junk/junk/test.txt', 'w')     

    for y in list1:
        #print(y)
        f.write("%s\n" % str(y))
    f.close()
    
    
 
    
    sys.exit()
    json_obj=json.dumps(test1)
    #json_obj.__getitem__("source")
    for key,value in dict.items(json_obj.__getitem__("source")):
        print(key,value)
    #print(json_obj)
    
    
    
    


    
    sys.exit()
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    client = storage.Client(project='poc-tier1')
    bucket = client.get_bucket('testbucketdeletemeuseless')   
    blob = bucket.get_blob('retail_test/retailnext')
    test=blob.download_as_string()
    test1=str(test,'utf-8')
    #print(test1)
   # print(test)
   
    tweets = []
    for line in test1:
        tweets.append(json.loads(line))
    
    #data=json.loads(test1)
    print(tweets['source'])

    # Make an authenticated API request
    #buckets = list(client.list_buckets())
    #print(buckets)
    
'''  
from google.cloud import storage
client = storage.Client(project='poc-tier1')
bucket = client.get_bucket('testdeletemeuseless')
blob = bucket.get_blob('retail_test/retailnext')
print(blob.download_as_string())
'''
