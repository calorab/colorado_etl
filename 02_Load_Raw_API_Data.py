#  first line

# need snowpark, pandas,
from snowflake.snowpark import Session
# Keep the URL's here - maybe in a tuple and loop over it


#  Main function
def main():
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


def get_parks_rec(url, session):
    pass


main()