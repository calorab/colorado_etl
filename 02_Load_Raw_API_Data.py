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

    # Stage the parks JSON data
    try:
        # !!!CALEB - before first run create the stage and format!!!
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
        # remove raw data table (NOT FLATTENED DATA!!!!!!!!!!!)
        pass


     #  create parks data table from flattened data
    try:
        pass
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')

    raw_script = """
        COPY INTO raw_parks_api_data
        FROM @api_stage
        FILE_FORMAT = (TYPE = JSON); """
    
    flattened_script = """
        CREATE TABLE flattened_parks_api_data as
        SELECT VALUE
        FROM raw_parks_api_data,
        LATERAL FLATTEN(INPUT => SRC:data)
    """

    parks_script = """
        CREATE OR REPLACE TABLE parks_api_data as
            SELECT 
                value:id::string                as park_id,
                value:fullName::string          as name,
                value:description::string       as description,
                value:latitude::number          as latitude,
                value:longitude::number         as longitude,
                value:states::string            as states,
                value:images[0].url::string     as image_1,
                value:images[0].title::string   as title_image_1,
                value:images[1].url::string     as image_2,
                value:images[1].title::string   as title_image_2,
                value:weatherInfo::string       as weather_climate,
                value:designation::string       as designation
            FROM
                flattened_parks_api_data,
                LATERAL FLATTEN(INPUT => src:data);
    """


    address_script = """
        CREATE OR REPLACE TABLE parks_api_data as
            SELECT 
                value:id::string                            as park_id,
                value:addresses[0].postalCode::string       as zipcode
                value:addresses[0].city::string             as city
                value:addresses[0].stateCode::string        as state
                value:addresses[0].line1::string            as address_line1
                value:addresses[0].type::string             as address_type
                value:addresses[0].line3::string            as address_line3
                value:addresses[0].line2::string            as address_line2
            FROM
                flattened_parks_api_data,
                LATERAL FLATTEN(INPUT => src:data);
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

