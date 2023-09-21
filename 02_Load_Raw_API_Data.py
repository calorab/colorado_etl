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
    get_parks_rec()
    # Set up connection to Snowflake
    conn = snowflake.connector.connect(
        user='ETLPROG2023',
        password='NHqLFb2X6#CgoLtn',
        account='omb53008.us-east-1'
    )
#  https://omb53008.us-east-1.snowflakecomputing.com
    
    # Create a cursor object
    cur = conn.cursor()

    try:
        # Upload the JSON data file to the Snowflake stage                  !!!CALEB - before first run create the stage and format!!!
        cur.execute("PUT 'file:///Users/AllHeart/Desktop/Projects_2023/coloradoproject_snowflake/parksrec.json' @api_stage AUTO_COMPRESS=TRUE;")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')

    #  create raw JSON formatted data table
    try:
        pass
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')
    

     #  create flattened JSON formatted data table
    try:
        pass
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')
    finally:
        # drop raw data table (NOT FLATTENED DATA!!!!!!!!!!!)
        pass


     #  create parks data table from flattened data
    try:
        pass
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')

    raw_script = """
        SELECT VALUE FROM """
    
    mapped_script = """


    """


def get_parks_rec():
    print("inside parks")
    apiKey = {"X-Api-Key": "mLeZrzDzwF8fhMmxnhTg4RvgTSclubh8vt4DAFRg"}

    data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=apiKey)
    
    results = data.json()

    if not os.path.exists('parksrec.json'):
        with open('parksrec.json', 'w') as f:
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






main()

