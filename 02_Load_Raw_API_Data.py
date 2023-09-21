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
        # Upload the JSON data file to the Snowflake stage
        cur.execute("PUT 'file:///Users/AllHeart/Desktop/Projects_2023/coloradoproject_snowflake/response.json' @api_stage;")
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)





def get_parks_rec():
    print("inside parks")
    apiKey = {"X-Api-Key": "mLeZrzDzwF8fhMmxnhTg4RvgTSclubh8vt4DAFRg"}

    data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=apiKey)
    
    results = data.json()
    if not os.path.exists('response.json'):
        with open('parksrec.json', 'w') as f:
            json.dump(results, f, indent=4)

    







get_parks_rec()