-- First line

/*
    This file was built in Snowflake's Worksheet environment and is designed to be run there. This builds the task in the DAG that will 
    refresh the Views
 */

USE DATABASE COLORADO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COL_WH;
    
-- Build Views Task
!set sql_delimiter=/
CREATE OR REPLACE TASK build_views_task
    WAREHOUSE='COL_WH'
    COMMENT='This task builds the zip and county views in the Analytics schema'
AS BEGIN
    USE DATABASE COLORADO;
    USE SCHEMA ANALYTICS;
    
    -- Build County Zip View
    CREATE OR REPLACE VIEW zip_county AS
    select
        zip.ZIP_CODE,
        zip.CITY_NAME,
        wc.COUNTY_NAME
    from
        ZIPCODE_AND_COUNTY.ATERIO_DATASHEET_DEV.US_HOUSING_FORECAST_ZIPCODE zip
        JOIN WEATHER_CLIMATE wc on zip.COUNTY_NAME = wc.COUNTY_NAME
    ORDER BY
        wc.COUNTY_NAME;
    
    -- Build Adams View
    CREATE OR REPLACE VIEW adams_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Adams';
            
    -- Build Arapahoe View
    CREATE OR REPLACE VIEW arapahoe_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Arapahoe';
    
    -- Build Boulder View
    CREATE OR REPLACE VIEW boulder_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Boulder';
    
    -- Build Denver View
    CREATE OR REPLACE VIEW denver_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Denver';
    
    -- Build Douglas View
    CREATE OR REPLACE VIEW douglas_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Douglas';
    
    -- Build ElPaso View
    CREATE OR REPLACE VIEW elpaso_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'ElPaso';
    
    
    -- Build Jefferson View
    CREATE OR REPLACE VIEW jefferson_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Jefferson';
    
    
    -- Build Larimer View
    CREATE OR REPLACE VIEW Larimer_county AS
        SELECT
            wc.ID,
            wc.COUNTY_NAME,
            d.POPULATION_2020,
            d.POPULATION_5YR_PROJ,
            d.POPULATION_DENSITY,
            d.HH_PCT_WO_CHILDREN,
            (100 - d.HH_PCT_WHITE) as "DIVERSITY_PCT",
            d.HH_MEDIAN_INCOME,
            d.HH_AVG_INCOME,
            d.OCCUPATION_WHITE_COLLAR,
            d.OCCUPATION_BLUE_COLLAR,
            d.CONSUMER_PRICE_INDEX,
            d.CPI_HOUSING,
            d.CRIME_INDEX,
            h.HOUSING_UNITS_VACANT_PCT,
            h.HOUSING_SINGLE_UNIT_PCT,
            h.HOUSING_MEDIAN_RENT,
            h.HOUSING_OWNER_MEDIAN_VALUE_2020,
            wc.AIR_POLLUTION_INDEX,
            wc.ANNUAL_AVERAGE_LOW_TEMP,
            wc.ANNUAL_AVERAGE_HIGH_TEMP,
            wc.AVERAGE_NUMBER_RAINY_DAYS,
            wc.AVERAGE_NUMBER_SNOW_DAYS,
            wc.ANNUAL_AVERAGE_SNOWFALL_INCHES,
            wc.WEATHER_INDEX
        FROM DEMOGRAPHICS d
        JOIN HOUSING h ON d.LOCATION_ID = h.LOCATION_ID
        JOIN WEATHER_CLIMATE wc on d.LOCATION_ID=wc.ID
        WHERE d.LOCATION_ID = 'Larimer';
END;/
!set sql_delimiter=";"