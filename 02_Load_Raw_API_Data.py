#  first line

import os
import snowflake.connector 
import requests
import json
from dotenv import load_dotenv
import logging

load_dotenv()

# setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_ROLE = 'COL_ADMIN'
SNOWFLAKE_DATABASE = 'COLORADO'
SNOWFLAKE_SCHEMA = 'EXTERNAL'
SNOWFLAKE_WAREHOUSE = 'COL_WH'
PARKS_API_KEY = os.getenv('PR_API_KEY')
POI_API_KEY = os.getenv('POI_API_KEY')
COMM_API_KEY = os.getenv('COMM_API_KEY')
comm_url_base = 'https://api.gateway.attomdata.com/v4/neighborhood/community?geoIdV4='
comm_header = {'APIKey': COMM_API_KEY}
parks_header = {"X-Api-Key": PARKS_API_KEY}
parks_target_location = 'JSON_Docs/parksrec.json'
geo_ids = {
    "Adams": "5c3a6ba1bdd969d35b89bbc7944f281d", 
    "Arapahoe": "df0f229947fc78b15e21005d5ba933a9", 
    "Boulder": "4c746922ad37c5e0e9c397a1d56cbd50", 
    "Denver": "1291dc1937525d78f89cebb6a43a50de", 
    "Douglas": "3ba5f4b1a7a6fec2eb0c023ca9e19e91", 
    "El Paso": "ca5cf064d0a92eb05d781c257c087ce5", 
    "Jefferson": "2251fecccd50ac4b7d79f31dc864b8f4", 
    "Larimer": "3eccdb33ca70f1537c5498e2e205ce0b"
}
poi = (
    'commercial.pet',
    'pet.veterinary',
    'pet.dog_park',
    'leisure.park',
    'populated_place.neighbourhood',
    'natural.mountain,natural.forest',
    'catering.restaurant.sushi'
)
purl_map = {
    'jefferson': '5170cd374708505ac0596bf2479314cb4340f00101f901ff88150000000000c002099203104a6566666572736f6e20436f756e7479',
    'denver': '513795b35c0a385ac059fc7fdca68de14340f00101f9010b89150000000000c0020992030644656e766572',
    'boulder': '5114becfbee3565ac0597ff9b363da0b4440f00101f9010989150000000000c0020992030e426f756c64657220436f756e7479',
    'douglas': '51aa9a54167e3b5ac05959e5f0e533aa4340f00101f9010a89150000000000c0020992030e446f75676c617320436f756e7479',
    'adams': '51a148d03d9e155ac059ea97b4acd3ef4340f00101f9011289150000000000c0020992030c4164616d7320436f756e7479',
    'arapahoe': '51def06ba5b7155ac059a7b5c85f27d34340f00101f9011789150000000000c0020992030f4172617061686f6520436f756e7479',
    'larimer': '51ca1d3fe4815d5ac05930cf56d952554440f00101f901fb88150000000000c0020992030e4c6172696d657220436f756e7479',
    'elpaso': '517fd481c5a1215ac059a907cc27836a4340f00101f9010789150000000000c0020992030e456c205061736f20436f756e7479',
    'forsyth': '5111e5be7d000855c0591172f5bbdd1c4140f00101f901e4a20f0000000000c0020992030e466f727379746820436f756e7479'
}

#  Main function
def main():
    # Make the API calls and create files
    get_community_data()
    get_parks_rec()

    # Set up connection to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT
    )
    
    # Create a the snowflake cursor object
    cur = conn.cursor()
    cur.execute('USE ROLE COL_ADMIN;')
    cur.execute('USE DATABASE COLORADO;')
    cur.execute('USE SCHEMA EXTERNAL;')
    cur.execute('USE WAREHOUSE COL_WH;')

    # Call POI API endpoint, build DB's and insert rows
    get_poi_data(conn)

    # Get a list of all the files in the JSON_Docs folder
    folder_path = 'JSON_Docs'
    file_list = os.listdir(folder_path)

    # Loop through the list of files and perform a PUT on each file and create table from JSON data
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
            raw_script = f"CREATE OR REPLACE TABLE {unique_raw_name} AS SELECT $1 FROM @api_stage (FILE_FORMAT => 'api_json_format');"
            cur.execute(raw_script)
        except snowflake.connector.errors.ProgrammingError as e:
            print(f'\t {e}')
        else:
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
    
    # CALEB - Need to clean up json files after they are loaded


    # end the Snowflake session
    conn.close()


def get_parks_rec():
    #  Get parks data
    if not os.path.exists(parks_target_location):
        data = requests.get('https://developer.nps.gov/api/v1/parks?stateCode=CO&fields=addresses', headers=parks_header)
        
        results = data.json()

        with open(parks_target_location, 'w') as f:
            json.dump(results, f, indent=4)

    
def get_poi_data(conn):
    #  Get Point of Interests data
    cur = conn.cursor()
    for c,l in purl_map.items():
        db_name = c + '_poi_data'
        print(f'Running {c} loop...')
        cur.execute(f'DROP TABLE IF EXISTS {db_name}')
        cur.execute(f'CREATE TABLE IF NOT EXISTS {db_name} (county TEXT, topic Text, num_of_features NUMBER)')
        location = str(c)
        for point in poi:
            url = 'https://api.geoapify.com/v2/places?categories=' + point + '&filter=place:' + l + '&limit=100&apiKey=' + POI_API_KEY
            response = requests.get(url)
            data = response.json()
            feature_length = len(data['features'])
            cur.execute(f"INSERT INTO {db_name} VALUES ('{location}','{point}', '{feature_length}')")
    
        print(f'{c} loop done')
    
    print('POI data done')


def get_community_data():
    # getting neighborhood data:

    # Create the folder for the JSON documents if it doesn't exist
    if not os.path.exists('JSON_Docs'):
        os.makedirs('JSON_Docs')
    
    
    for k,v in geo_ids.items():

        # create file name and url string
        if k == "El Paso":
            file_name = 'ElPaso_data.json'
        else:
            file_name = k + '_data.json'
        
        url_string = comm_url_base + v
        data = requests.get(url_string, headers=comm_header)
        results = data.json()

        # Write the JSON data to a file in the JSON_Docs folder
        file_path = os.path.join('JSON_Docs', file_name)
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=4)


if __name__ == '__main__':
    main()

