# Databricks notebook source
import requests
import json
import time
from pyspark.sql.functions import col, StringType, lit, array

# COMMAND ----------

def geocodeRequest(arr):
  address = arr[0]
  lat = arr[1]
  lng = arr[2]
  key = arr[3]
  
  City = None
  State = None
  PostalCode = None
  Country = None
  County = None
  Address = None
  GPSLatitude = None
  GPSLongitude = None
  PlaceID = None
  MapURL = None
  ResultQuality = None
  FormattedAddress = None
  
  jsonResults = None
  ErrorMessage = None
  StreetNumber = None
  Route = None
  if (lat != None) & (lng !=None):
    latlng=str(lat)+','+str(lng)
    requestUri = "https://maps.googleapis.com/maps/api/geocode/json?sensor=false&latlng={}&key={}".format(latlng, key)
  else:
    requestUri = "https://maps.googleapis.com/maps/api/geocode/json?sensor=false&address={}&key={}".format(address, key)
  response = requests.get(
    requestUri,
    headers={"content-type":"json"}
  )
  responseJson = response.json()
  jsonResults = response.text
  Status = responseJson["status"]
  if Status == "OK":
    geoData = responseJson['results'][0]['address_components']
    types = ['locality', 'administrative_area_level_1', 'postal_code', 'country', 'administrative_area_level_2', 'street_number', 'route']
    geonames = filter(lambda x: len(set(x['types']).intersection(types)), geoData)
    for geoname in geonames:
      common_types = set(geoname['types']).intersection(set(types))
      if 'locality' in common_types:
        City = geoname['long_name']   
      if 'administrative_area_level_1' in common_types:
        State = geoname['short_name']   
      if 'postal_code' in common_types:
        PostalCode = geoname['long_name']   
      if 'country' in common_types:
        Country = geoname['short_name']   
      if 'administrative_area_level_2' in common_types:
        County = geoname['short_name']    
      if 'street_number' in common_types:
        StreetNumber = geoname['long_name']    
      if 'route' in common_types:
        Route = geoname['long_name']    
      if StreetNumber is not None and Route is not None:
        Address = f"{StreetNumber} {Route}"
    GPSLatitude = responseJson["results"][0]["geometry"]["location"]["lat"]
    GPSLongitude = responseJson["results"][0]["geometry"]["location"]["lng"]
    PlaceID = responseJson["results"][0]["place_id"]  
    if "partial_match" in responseJson["results"][0]:
      PartialMatch = responseJson["results"][0]["partial_match"]  
    else:
      PartialMatch = None
    LocationType = responseJson["results"][0]["geometry"]["location_type"]
    FormattedAddress = responseJson["results"][0]["formatted_address"]
    MapURL = f"http://maps.google.com/maps?f=q&hl=en&q={GPSLatitude}+{GPSLongitude}"
  else:
    ErrorMessage = responseJson["error_message"]

  returnJson = {
    "Status": Status,
    "City": City,
    "State": State,
    "PostalCode": PostalCode,
    "Country": Country,
    "County": County,
    "Address": Address,
    "GPSLatitude": GPSLatitude,
    "GPSLongitude": GPSLongitude,
    "PlaceID": PlaceID,
    "MapURL": MapURL,
    "PartialMatch": PartialMatch,
    "LocationType": LocationType,
    "FormattedAddress": FormattedAddress,
    "ErrorMessage": ErrorMessage,
    "jsonResults": jsonResults
  }
  return json.dumps(returnJson)
geocodeUDF = udf(lambda z: geocodeRequest(z),StringType())
  
def truncLatLng(latLng):
  if latLng==None:
    return latLng
  else:
    numInt = int(latLng)
    numLen = len(str(numInt))
    if latLng >= 0:
      res = round(latLng, 6 - numLen)
    else:
      res = round(latLng, 7 - numLen)
    return str(res).rstrip('0').rstrip('.')
spark.udf.register("truncUDF", truncLatLng)

# COMMAND ----------


