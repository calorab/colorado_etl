#  first line

# need snowpark, pandas,
import os
from snowflake.snowpark import Session
import requests
import json
import pandas as pd
# Keep the URL's here - maybe in a tuple and loop over it


#  Main function
def main(session):
    # first list any variables needed and open Snowpark session
    conn_params: dict = {
        "account": "omb53008.us-east-1",
        "user": "ETLPROG2023",
        "password": "NHqLFb2X6#CgoLtn"
    }
#  https://omb53008.us-east-1.snowflakecomputing.com
    
    new_session = Session.builder.configs(conn_params).create()
    #  some code here
    new_session.close()
    # make the API call on "url" - include testing for err's in try/catch/exception block

    # assign the returned data to var

    #   if data type JSON - upload to raw table (or whatever SF recommends - UPDATE HERE WHEN YOU KNOW)
    #       Need to use Snowpark for session object, and writing sql with python
    #   if data type CSV use pandas to upload to table

    #  close session


def get_parks_rec():
    print("inside parks")
    apiKey = {"X-Api-Key": "mLeZrzDzwF8fhMmxnhTg4RvgTSclubh8vt4DAFRg"}

    data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=apiKey)
    
    results = data.json()
    # The below for shits and giggles bc it won't work in a SF worksheet:
    # if not os.path.exists('response.json'):
    #     with open('response.json', 'w') as f:
    #         json.dump(results, f, indent=4)

    # df = pd.read_json('response.json')
    """
    CALEB - here is the plan:
        since this is going to run in a worksheet we cannot write it to a file
        1 - get data as above
        2 - 
        2a - Create a task (?) that runs another SQL worksheet that contains the steps from here: https://docs.snowflake.com/en/user-guide/script-data-load-transform-json
        2b - Follow these instructions in order to  
    
    """

get_parks_rec()