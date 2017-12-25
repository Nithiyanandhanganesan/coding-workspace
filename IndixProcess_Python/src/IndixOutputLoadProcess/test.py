'''
Created on Nov 8, 2017

@author: nwe.nganesan
'''
import os
import sys
from google.cloud import bigquery
import uuid
#print(os.environ['project_name'])
#uuid.uuid1()
test=[(1,3),(2,4)]
for x in test:
    print(x[0])


mail='sallieasmith.ss@outlook.com'
print(mail[:1])
#test=uuid.uuid3(uuid.NULL_NAMESPACE, mail)
test= uuid.uuid3(type('', (), dict(bytes=b''))(), mail)
print(test)

SCHEMA = [
        bigquery.SchemaField('full_name', 'STRING', mode='required'),
        bigquery.SchemaField('age', 'INTEGER', mode='required'),
    ]
print(SCHEMA)