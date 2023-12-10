-- First line
/*
    This file was built in Snowflake's Worksheet environment and is designed to be run in a task as one step in a monthly
    batch ETL piepline.
 */

 
USE DATABASE COLORADO;
USE SCHEMA ANALYTICS;

-- Build weather and climate data
create or replace TABLE COLORADO.ANALYTICS.weather_climate (
	ID VARCHAR(9),
	county_name VARCHAR(16),
	air_pollution_index NUMBER(38,0),
	particulate_matter_index NUMBER(38,0),
	carbon_monoxide_index NUMBER(38,0),
	annual_average_low_temp FLOAT,
	annual_average_high_temp FLOAT,
	average_number_clear_days NUMBER(38,0),
	average_number_rainy_days NUMBER(38,0),
	average_number_snow_days NUMBER(38,0),
	annual_average_precipitation_inches FLOAT,
	annual_average_snowfall_inches FLOAT,
	average_jan_low_temp FLOAT,
	average_jan_high_temp FLOAT,
	average_jul_low_temp FLOAT,
	average_jul_high_temp FLOAT,
	weather_index NUMBER(38,0),
	hail_index NUMBER(38,0),
	tornado_index NUMBER(38,0),
	wind_index NUMBER(38,0)
);

INSERT INTO weather_climate  
SELECT
    'Adams',
    'Adams County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_ADAMS_DATA
UNION ALL
SELECT
    'Arapahoe',
    'Arapahoe County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_ARAPAHOE_DATA
UNION ALL
SELECT
    'Boulder',
    'Boulder County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_BOULDER_DATA
UNION ALL
SELECT
    'Denver',
    'Denver County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_DENVER_DATA
UNION ALL
SELECT
    'Douglas',
    'Douglas County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_DOUGLAS_DATA
UNION ALL
SELECT
    'ElPaso',
    'ElPaso County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_ELPASO_DATA
UNION ALL
SELECT
    'Jefferson',
    'Jefferson County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_JEFFERSON_DATA
UNION ALL
SELECT
    'Larimer',
    'Larimer County',
    $1:community['airQuality']['air_Pollution_Index'],
    $1:community['airQuality']['particulate_Matter_Index'],
    $1:community['airQuality']['carbon_Monoxide_Index'],
    $1:community['climate']['annual_Avg_Temp_Min'],
    $1:community['climate']['annual_Avg_Temp_Max'],
    $1:community['climate']['clear_Day_Mean'],
    $1:community['climate']['rainy_Day_Mean'],
    $1:community['climate']['snow_Day_Mean'],
    $1:community['climate']['annual_Precip_In'],
    $1:community['climate']['annual_Snowfall_In'],
    $1:community['climate']['avg_Jan_Low_Temp'],
    $1:community['climate']['avg_Jan_High_Temp'],
    $1:community['climate']['avg_Jul_Low_Temp'],
    $1:community['climate']['avg_Jul_High_Temp'],
    $1:community['naturalDisasters']['weather_Index'],
    $1:community['naturalDisasters']['hail_Index'],
    $1:community['naturalDisasters']['tornado_Index'],
    $1:community['naturalDisasters']['wind_Index']
FROM 
    COLORADO.EXTERNAL.RAW_COMM_LARIMER_DATA;


-- Build point of interest(index) data
CREATE OR REPLACE table COLORADO.ANALYTICS.poi_index(
    location_id text,
    pet_commercial_index number,
    pet_vet_index number,
    pet_dogpark_index number,
    local_park_index number,
    neighborhood_index number,
    mountain_forest_index number,
    sushi_index number
);


-- Build Demographics data
CREATE OR REPLACE table COLORADO.ANALYTICS.demographics(
    location_id text,
    square_miles float,
    population_2020 number,
    population_5yr_proj number,
    population_density float,
    population_urban float,
    populatino_rural float,
    population_poverty float,
    num_households_2020 number,
    avg_household_size float,
    hh_pct_wo_children float,
    hh_pct_w_children float,
    hh_pct_white float,
    median_age float,
    hh_pct_work_from_home float,
    hh_median_income number,
    hh_avg_income number,
    occupation_white_collar float,
    occupation_blue_collar float,
    consumer_price_index float,
    cpi_housing float,
    cpi_home_expenses float,
    cpi_electricity float,
    crime_index number,
    crime_index_murder number,
    crime_index_burglary number
);

INSERT INTO COLORADO.ANALYTICS.DEMOGRAPHICS
SELECT
    'Adams',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_adams_data
UNION ALL
SELECT
    'Arapahoe',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_arapahoe_data
UNION ALL
SELECT
    'Boulder',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_boulder_data
UNION ALL
SELECT
    'Denver',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_denver_data
UNION ALL
SELECT
    'Douglas',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_douglas_data
UNION ALL
SELECT
    'ElPaso',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_elpaso_data
UNION ALL
SELECT
    'Jefferson',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_jefferson_data
UNION ALL
SELECT
    'Larimer',
    $1:community['geography']['area_Square_Mile'],
    $1:community['demographics']['population_2020'],
    $1:community['demographics']['population_5_Yr_Projection'],
    $1:community['demographics']['population_Density_Sq_Mi'],
    $1:community['demographics']['population_Urban_Pct'],
    $1:community['demographics']['population_Rural_Pct'],
    $1:community['demographics']['population_In_Poverty_Pct'],
    $1:community['demographics']['households_2020'],
    $1:community['demographics']['household_Size_Avg'],
    $1:community['demographics']['hh_Fam_Married_Wo_Children_L18_Pct'],
    $1:community['demographics']['hh_Fam_Married_W_Children_L18_Pct'],
    $1:community['demographics']['households_White_Pct'],
    $1:community['demographics']['median_Age'],
    $1:community['demographics']['transportation_Work_From_Home_Pct'],
    $1:community['demographics']['family_Median_Income'],
    $1:community['demographics']['family_Avg_Income'],
    $1:community['demographics']['occupation_White_Collar_Pct'],
    $1:community['demographics']['occupation_Blue_Collar_Pct'],
    $1:community['demographics']['consumerPriceIndex'],
    $1:community['demographics']['costIndex_Housing'],
    $1:community['demographics']['home_Maintenance_Repairs_Insurance_And_Other_Expenses'],
    $1:community['demographics']['costIndex_Electricity'],
    $1:community['crime']['crime_Index'],
    $1:community['crime']['murder_Index'],
    $1:community['crime']['burglary_Index']
FROM COLORADO.EXTERNAL.raw_comm_larimer_data;

-- Build Housing data
CREATE OR REPLACE table COLORADO.ANALYTICS.housing (
    location_id text,
    housing_units_vacant number,
    housing_units_vacant_pct float,
    housing_units_owner_pct float,
    housing_single_unit_pct float,
    housing_median_rent number,
    housing_owner_median_value_2020 number
);

INSERT INTO COLORADO.ANALYTICS.housing
SELECT
    'Adams',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_adams_data
UNION ALL
SELECT
    'Arapahoe',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_arapahoe_data
UNION ALL
SELECT
    'Boulder',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_boulder_data
UNION ALL
SELECT
    'Denver',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_denver_data
UNION ALL
SELECT
    'Douglas',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_douglas_data
UNION ALL
SELECT
    'ElPaso',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_elpaso_data
UNION ALL
SELECT
    'Jefferson',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_jefferson_data
UNION ALL
SELECT
    'Larimer',
    $1:community['demographics']['housing_Units_Vacant'],
    $1:community['demographics']['housing_Units_Vacant_Pct'],
    $1:community['demographics']['housing_Units_Owner_Occupied_Pct'],
    $1:community['demographics']['housing_Occupied_Structure_1_Unit_Detached_Pct'],
    $1:community['demographics']['housing_Median_Rent'],
    $1:community['demographics']['housing_Owner_Households_Median_Value']
FROM COLORADO.EXTERNAL.raw_comm_larimer_data;

-- Build Parks General data from RAW table 
CREATE OR REPLACE TABLE COLORADO.ANALYTICS.PARKS_API_DATA (
    ID string,
    full_name string,
    designation string,
    description string,
    directions_url string,
    website_url string,
    climate_description string
);



INSERT INTO COLORADO.ANALYTICS.PARKS_API_DATA
SELECT
    value:id,
    value:fullName,
    value:designation,
    value:description,
    value:directionsUrl,
    value:url,
    value:weatherInfo
FROM
     COLORADO.EXTERNAL.RAW_PARKS_API_DATA,
    LATERAL FLATTEN(input => SRC:data);



-- Build Parks Address data from RAW table    
CREATE OR REPLACE TABLE COLORADO.ANALYTICS.PARKS_ADDRESS_API_DATA (
    id string,
    city string,
    country_code string,
    address_line1 string,
    address_line2 string,
    address_line3 string,
    zipcode number,
    territory_code string,
    state_code string,
    address_type string
);

INSERT INTO COLORADO.ANALYTICS.PARKS_ADDRESS_API_DATA
SELECT
    value:id,
    value:addresses[0]['city'],
    value:addresses[0]['countryCode'],
    value:addresses[0]['line1'],
    value:addresses[0]['line2'],
    value:addresses[0]['line3'],
    value:addresses[0]['postalCode'],
    value:addresses[0]['provinceTerritoryCode'],
    value:addresses[0]['stateCode'],
    value:addresses[0]['type']
FROM 
    COLORADO.EXTERNAL.RAW_PARKS_API_DATA,
    LATERAL FLATTEN(input => SRC:data);




