'''
Created on Dec 7, 2017

@author: nwe.nganesan
'''

from math import radians, cos, sin, asin, sqrt
def haversine(lon1, lat1, lon2, lat2):
#     Calculate the great circle distance between two points 
#     on the earth (specified in decimal degrees)

    # convert decimal degrees to radians 
    lat1=40.73943
    lon1=-73.99585
    lat2=40.767
    lon2=-73.983
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    meter = 6367 * c *1000
    print(meter)
    return meter

if __name__=='__main__':
    haversine(-73.99585,40.73943,73.983,40.767)
