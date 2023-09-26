#  first line

# need snowpark, pandas,
import os
import snowflake.connector 
import requests
import json

# Keep the URL's here - maybe in a tuple and loop over it (??)


#  Main function
def main():
    # first list any variables needed and open Snowpark session
    # get_parks_rec()

    # Set up connection to Snowflake
    conn = snowflake.connector.connect(
        user='ETLPROG2023',
        password='NHqLFb2X6#CgoLtn',
        account='omb53008.us-east-1'
    )
#  https://omb53008.us-east-1.snowflakecomputing.com
    
    # Create a cursor object
    cur = conn.cursor()
    cur.execute('USE ROLE COL_ADMIN;')
    cur.execute('USE DATABASE COLORADO;')
    cur.execute('USE SCHEMA EXTERNAL;')
    cur.execute('USE WAREHOUSE COL_WH;')


    # Stage the parks JSON data
    try:
        cur.execute("PUT 'file:///Users/AllHeart/Desktop/Projects_2023/coloradoproject_snowflake/parksrec.json' @api_stage AUTO_COMPRESS=TRUE;")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')


    #  create raw JSON formatted data table
    try:
        raw_script = """
            COPY INTO raw_parks_api_data
            FROM @api_stage
            FILE_FORMAT = (TYPE = JSON); 
        """
        cur.execute(raw_script)
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')
    

     #  create flattened JSON formatted data table
    try:
        flattened_script = """
            CREATE OR REPLACE TABLE flattened_parks_api_data as
            SELECT VALUE
            FROM raw_parks_api_data,
            LATERAL FLATTEN(INPUT => SRC:data)
        """
        cur.execute(flattened_script)
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')
    finally:
        conn.close()




def get_parks_rec():
    print("inside parks")
    apiKey = {"X-Api-Key": "mLeZrzDzwF8fhMmxnhTg4RvgTSclubh8vt4DAFRg"}

    data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=apiKey)
    
    results = data.json()

    if not os.path.exists('parksrec.json'):
        with open('parksrec.json', 'w') as f:
            json.dump(results, f, indent=4)

    
def get_poi_data():
    #  Geoapify API for points of interest base url: https://api.geoapify.com/v2/places?PARAMS
    api_key = 'fca84109c53b4439aa9e4c2ca1348525'
    data = requests.get()
    
    results = data.json()

    if not os.path.exists('parksrec.json'):
        with open('parksrec.json', 'w') as f:
            json.dump(results, f, indent=4)
    
    pass




def get_community_data():
    # getting neighborhood,
    #  api key for ATTOM (onBoard informatics or whatever it was called) base url: https://api.gateway.attomdata.com/v4/
    api_key = 'fd6837b6e4d8b044e387568b072a363f'
    url_base = 'https://api.gateway.attomdata.com/v4/neighborhood/community?geoIdV4='
    header = {'APIKey': api_key}
    geo_ids = {"Adams": "5c3a6ba1bdd969d35b89bbc7944f281d", 
               "Arapahoe": "df0f229947fc78b15e21005d5ba933a9", 
               "Boulder": "4c746922ad37c5e0e9c397a1d56cbd50", 
               "Denver": "1291dc1937525d78f89cebb6a43a50de", 
               "Douglas": "3ba5f4b1a7a6fec2eb0c023ca9e19e91", 
               "El Paso": "ca5cf064d0a92eb05d781c257c087ce5", 
               "Jefferson": "2251fecccd50ac4b7d79f31dc864b8f4", 
               "Larimer": "3eccdb33ca70f1537c5498e2e205ce0b"}
    
    for k,v in geo_ids.items():

        file_name = k + '_data.json'
        url_string = url_base + v

        print(f'File Name: {file_name}')
        print(f'Full URL String: {url_string}')
        # data = requests.get()
        
        # results = data.json()

        # if not os.path.exists('parksrec.json'):
        #     with open('parksrec.json', 'w') as f:
        #         json.dump(results, f, indent=4)
        

"""
1 stage the data
2 copy into table with format JSON (raw_data) 
3 use a create table as select statement to create a table from flattened (flattened_data) in order to keep the JSON format for changes and bug searching
3a drop raw_data table
4 Create final table for parks a'la the below


Below: src is the column of JSON data in raw script
"::" is to cast as a type - if omitted the type is variant
VALUE is the output of the FLATTEN function including the values for the flattened data

create or replace table events as
  select
    src:device_type::string                             as device_type
  , src:version::string                                 as version
  , value:f::number                                     as f
  , value:rv::variant                                   as rv
  , value:t::number                                     as t
  , value:v.ACHZ::number                                as achz
  , value:v.ACV::number                                 as acv
  , value:v.DCA::number                                 as dca
  , value:v.DCV::number                                 as dcv
  , value:v.ENJR::number                                as enjr
  , value:v.ERRS::number                                as errs
  , value:v.MXEC::number                                as mxec
  , value:v.TMPI::number                                as tmpi
  , value:vd::number                                    as vd
  , value:z::number                                     as z
  from
    raw_source
  , lateral flatten ( input => SRC:events );

  
    The SUBSTR , SUBSTRING function to extract city and state values from state_city JSON key.
    The TO_TIMESTAMP / TO_TIMESTAMP_* to cast the sale_date JSON key value to a timestamp.


    Execute the following query to verify data is copied.

    SELECT * from home_sales;

"""






get_community_data()

