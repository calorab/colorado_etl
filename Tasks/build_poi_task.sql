-- First Line

/*
    This file was built in Snowflake's Worksheet environment and is designed to be run there. This builds the task in the DAG that will 
    calls 05_Build_POI_Table.py
 */


USE DATABASE COLORADO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COL_WH;

CREATE TASK build_poi_task
    WAREHOUSE='COL_WH'
    COMMENT='This task builds the POI table by Calling the build_poi_sp() Storeed Procedure'
AS 
    CALL build_poi_sp();