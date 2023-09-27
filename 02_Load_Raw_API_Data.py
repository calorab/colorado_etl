#  first line

# need snowpark, pandas,
import os
import snowflake.connector 
import requests
import json

# Keep the URL's here - maybe in a tuple and loop over it (??)


#  Main function
def main():
    # Make the API calls and create files
    # get_community_data()
    # get_parks_rec()

    # Set up connection to Snowflake
    conn = snowflake.connector.connect(
        user='ETLPROG2023',
        password='NHqLFb2X6#CgoLtn',
        account='omb53008.us-east-1'
    )
    
    # Create a the snowflake cursor object
    cur = conn.cursor()
    cur.execute('USE ROLE COL_ADMIN;')
    cur.execute('USE DATABASE COLORADO;')
    cur.execute('USE SCHEMA EXTERNAL;')
    cur.execute('USE WAREHOUSE COL_WH;')

    # Get a list of all the files in the JSON_Docs folder
    folder_path = 'JSON_Docs'
    file_list = os.listdir(folder_path)

    # Loop through the list of files and perform an action on each file
    for file_name in file_list:
        unique_file_name = os.path.join('file:///Users/AllHeart/Desktop/Projects_2023/coloradoproject_snowflake/JSON_Docs', file_name)
        db_addition = os.path.splitext(file_name)[0]

        # Stage the parks JSON data
        try:
            cur.execute(f'PUT {unique_file_name} @api_stage AUTO_COMPRESS=TRUE;')
        except snowflake.connector.errors.ProgrammingError as e:
            print(f'\t {e}')


        #  create raw JSON formatted data table
        try:
            unique_raw_name = 'raw_comm_' + db_addition
            raw_script = f'CREATE OR REPLACE TABLE {unique_raw_name} FROM @api_stage FILE_FORMAT = (TYPE = JSON);'
            cur.execute(raw_script)
        except snowflake.connector.errors.ProgrammingError as e:
            print(f'\t {e}')
        finally:
            # clear the stage of all files
            cur.execute("REMOVE @api_stage")    
        

        #  create flattened JSON formatted data table for files that need it
        if file_name in ('parksrec.json'):
            try:
                unique_flat_name = 'flat_comm_' + db_addition
                # CALEB - need to make a change here to get flattened
                flattened_script = f'CREATE OR REPLACE TABLE {unique_flat_name} as SELECT VALUE FROM {unique_raw_name}, LATERAL FLATTEN(INPUT => SRC:data)'
                cur.execute(flattened_script)
            except snowflake.connector.errors.ProgrammingError as e:
                print(f'\t {e}')
                        
    
    # end the Snowflake session
    conn.close()




def get_parks_rec():
    apiKey = {"X-Api-Key": "mLeZrzDzwF8fhMmxnhTg4RvgTSclubh8vt4DAFRg"}
    target_location = 'JSON_Docs/parksrec.json'

    if not os.path.exists(target_location):
        data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=apiKey)
        
        results = data.json()

        with open(target_location, 'w') as f:
            json.dump(results, f, indent=4)

    
def get_poi_data():
    #  Geoapify API for points of interest base url: https://api.geoapify.com/v2/places?PARAMS
    api_key = 'fca84109c53b4439aa9e4c2ca1348525'
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
    
    # Create the folder for the JSON documents if it doesn't exist
    if not os.path.exists('JSON_Docs'):
        os.makedirs('JSON_Docs')
    
    
    for k,v in geo_ids.items():

        # create file name and url string
        if k == "El Paso":
            file_name = 'ElPaso_data.json'
        else:
            file_name = k + '_data.json'
        
        url_string = url_base + v

        data = requests.get(url_string, headers=header)
        
        results = data.json()

        # Write the JSON data to a file in the JSON_Docs folder
        file_path = os.path.join('JSON_Docs', file_name)
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=4)



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

"""
    Notes on next step:

    # Get a list of all the files in the JSON_Docs folder
    folder_path = 'JSON_Docs'
    file_list = os.listdir(folder_path)

    # Loop through the list of files and perform an action on each file
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r') as f:
            data = json.load(f)
            # Perform the desired action on the data in the file
            # For example, print the data to the console
            print(data)
        
        try:
            pass
        except snowflake.connector.errors.ProgrammingError as e:
            print(f'\t {e}')
        finally:
            pass
"""

main()

