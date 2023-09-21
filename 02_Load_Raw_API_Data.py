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

    try:
        pass
    except snowflake.connector.errors.ProgrammingError as e:
        print(f'\t {e}')



def get_parks_rec():
    print("inside parks")
    apiKey = {"X-Api-Key": "mLeZrzDzwF8fhMmxnhTg4RvgTSclubh8vt4DAFRg"}

    data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=apiKey)
    
    results = data.json()
    if not os.path.exists('response.json'):
        with open('parksrec.json', 'w') as f:
            json.dump(results, f, indent=4)

    
"""
Example for copying staged jaon data:account

{
   "location": {
      "state_city": "MA-Lexington",
      "zip": "40503"
   },
   "sale_date": "2017-3-5",
   "price": "275836"
}

COPY INTO home_sales(city, state, zip, sale_date, price)
   FROM (SELECT SUBSTR($1:location.state_city,4),
                SUBSTR($1:location.state_city,1,2),
                $1:location.zip,
                to_timestamp_ntz($1:sale_date),
                $1:price
         FROM @sf_tut_stage/sales.json.gz t)
   ON_ERROR = 'continue';

   
$1 in the SELECT query refers to the single column where the JSON is stored. The query also uses the following functions:


The SUBSTR , SUBSTRING function to extract city and state values from state_city JSON key.
The TO_TIMESTAMP / TO_TIMESTAMP_* to cast the sale_date JSON key value to a timestamp.


Execute the following query to verify data is copied.

SELECT * from home_sales;




"""






main()

