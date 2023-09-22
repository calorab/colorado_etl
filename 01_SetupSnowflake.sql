-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------
-- SELECT THE ROLE THAT WE WILL USE FOR ALL FOLLOWING QUERIES - MUST BE ACCOUNTADMIN SINCE WE ARE CREATING SYSADMIN ROLE(S)
USE ROLE ACCOUNTADMIN;

-- Roles
-- SET THE VARIABLE MY_USER TO THE DEFAULT OR CURRENT SYSTEM USER 
SET MY_USER = CURRENT_USER();

-- CREATE THE MAIN ROLE, GIVE IT SYSADMIN PRIVILEGES AND GRANT THAT ROLE TO MY_USER
CREATE OR REPLACE ROLE COL_ADMIN;
GRANT ROLE COL_ADMIN TO ROLE SYSADMIN;
GRANT ROLE COL_ADMIN TO USER IDENTIFIER($MY_USER);

-- GIVE EXECUTE TASK AND MONITOR EXECUTION PRIVILEGES TO THE MAIN ROLE (FIND OUT WHAT IMPORTED PRIVILEGES ARE)
GRANT EXECUTE TASK ON ACCOUNT TO ROLE COL_ADMIN;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE COL_ADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE COL_ADMIN;

-- Databases CREATE THE DATABASE(S) AND GRANT THE OWNERSHIP TO THE MAIN ROLE SINCE WE ARE IN ACCOUNTADMIN ROLE
CREATE OR REPLACE DATABASE COLORADO;
GRANT OWNERSHIP ON DATABASE COLORADO TO ROLE COL_ADMIN;

-- Warehouses
-- CREATE THE WAREHOUSE(S) AND GRANT THE OWNERSHIP TO THE MAIN ROLE SINCE WE ARE IN ACCOUNTADMIN ROLE
CREATE OR REPLACE WAREHOUSE COL_WH;
GRANT OWNERSHIP ON WAREHOUSE COL_WH TO ROLE COL_ADMIN;

-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE COL_ADMIN;
USE DATABASE COLORADO;
USE WAREHOUSE COL_WH;

-- Schemas
-- USE SCHEMA EXTERNAL;


CREATE OR REPLACE SCHEMA ANALYTICS;
CREATE OR REPLACE SCHEMA WEBSCRAPE;
CREATE OR REPLACE SCHEMA EXTERNAL;

-- tables

CREATE OR REPLACE TABLE raw_parks_api_data (SRC VARIANT);

-- ----------------------------------------------------------------------------
-- Step #4: Create the pipeline objects
-- ----------------------------------------------------------------------------

-- Format
CREATE OR REPLACE FILE FORMAT api_json_format
  TYPE = 'JSON';

CREATE OR REPLACE STAGE api_stage
  FILE_FORMAT = api_json_format;