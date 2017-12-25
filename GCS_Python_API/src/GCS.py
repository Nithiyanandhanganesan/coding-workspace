'''
Created on Oct 24, 2017

@author: nwe.nganesan
'''
#def implicit():

   
if __name__ == '__main__':
    from google.cloud import storage
    x="hello"
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    print("hi")
    storage_client = storage.Client(project='poc-tier1')

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)
    