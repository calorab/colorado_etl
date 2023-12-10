# First line

'''
    This file was built in Snowflake's Worksheet environment and is designed to be run in a task as one step in a monthly
    batch ETL piepline.
'''


# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
'''
    location_id text,
    pet_commercial_index number,
    pet_vet_index number,
    pet_dogpark_index number,
    local_park_index number,
    neighborhood_index number,
    mountain_forest_index number,
    sushi_index number

    
'''


import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    db_list = [
        'ADAMS_POI_DATA', 
        'ARAPAHOE_POI_DATA', 
        'BOULDER_POI_DATA', 
        'DENVER_POI_DATA', 
        'DOUGLAS_POI_DATA', 
        'ELPASO_POI_DATA', 
        'FORSYTH_POI_DATA', 
        'JEFFERSON_POI_DATA', 
        'LARIMER_POI_DATA'
    ]
    
    topic_dict = {
        "commercial.pet":"pet_commercial_index", 
        "pet.veterinary": "pet_vet_index", 
        "pet.dog_park": "pet_dogpark_index", 
        "leisure.park": "local_park_index", 
        "populated_place.neighbourhood": "neighborhood_index", 
        "natural.mountain,natural.forest": "mountain_forest_index", 
        "catering.restaurant.sushi": "sushi_index"
             
    }

    data = {
        "location_id": [],
        "pet_commercial_index": [], 
        "pet_vet_index": [], 
        "pet_dogpark_index": [], 
        "local_park_index": [], 
        "neighborhood_index": [], 
        "mountain_forest_index": [], 
        "sushi_index": []
    }
    
    for db in db_list:
        df_table = session.table(f'COLORADO.EXTERNAL.{db}')
        location = db.split('_')[0]
        data['location_id'].append(location)
        
        for row in df_table.to_local_iterator():
            data[topic_dict[row['TOPIC']]].append(row['NUM_OF_FEATURES'])
    
    # print(data)
    new_df = pd.DataFrame(data)
    print(new_df)
    session.write_pandas(df=new_df,table_name='POI_INDEX_COMP',database='COLORADO', schema='ANALYTICS', auto_create_table=True, overwrite=True)

    return 'Table COLORADO.ANALYTICS.POI_INDEX_COMP Created'    


