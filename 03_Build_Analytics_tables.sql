

-- Build weather and climate data
INSERT INTO weather_climate 
SELECT
    'Adams' as "ID",
    'Adams County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_ADAMS_DATA
UNION ALL
SELECT
    'Arapahoe' as "ID",
    'Arapahoe County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_ARAPAHOE_DATA;
UNION ALL
SELECT
    'Boulder' as "ID",
    'Boulder County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_BOULDER_DATA;
UNION ALL
SELECT
    'Denver' as "ID",
    'Denver County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_DENVER_DATA;
UNION ALL
SELECT
    'Douglas' as "ID",
    'Douglas County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_DOUGLAS_DATA;
UNION ALL
SELECT
    'ElPaso' as "ID",
    'ElPaso County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_ELPASO_DATA;
UNION ALL
SELECT
    'Jefferson' as "ID",
    'Jefferson County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_JEFFERSON_DATA;
UNION ALL
SELECT
    'Larimer' as "ID",
    'Larimer County' as "County Name",
    $1:community['airQuality']['air_Pollution_Index']::number as "Air Pollution Index",
    $1:community['airQuality']['particulate_Matter_Index']::number  as "Particulate Matter Index",
    $1:community['airQuality']['carbon_Monoxide_Index']::number  as "Carbon Monoxide Index",
    $1:community['climate']['annual_Avg_Temp_Min']::float as "Annual Average Low Temp",
    $1:community['climate']['annual_Avg_Temp_Max']::float as "Annual Average High Temp",
    $1:community['climate']['clear_Day_Mean']::number as "Average Number Clear Days",
    $1:community['climate']['rainy_Day_Mean']::number as "Average Number Rainy Days",
    $1:community['climate']['snow_Day_Mean']::number as "Average Number Snow Days",
    $1:community['climate']['annual_Precip_In']::float as "Annual Average Precipitation (inches)",
    $1:community['climate']['annual_Snowfall_In']::float as "Annual Average Snowfall (inches)",
    $1:community['climate']['avg_Jan_Low_Temp']::float as "Average Jan Low Temp",
    $1:community['climate']['avg_Jan_High_Temp']::float as "Average Jan High Temp",
    $1:community['climate']['avg_Jul_Low_Temp']::float as "Average Jul Low Temp",
    $1:community['climate']['avg_Jul_High_Temp']::float as "Average Jul High Temp",
    $1:community['naturalDisasters']['weather_Index']::number as "Weather Index",
    $1:community['naturalDisasters']['hail_Index']::number as "Hail Index",
    $1:community['naturalDisasters']['tornado_Index']::number as "Tornado Index",
    $1:community['naturalDisasters']['wind_Index']::number as "Wind Index"
FROM 
    COLORADO.EXTERNAL.RAW_COMM_LARIMER_DATA;