CREATE TABLE silver.explicit_gdelt_events (
    "GlobalEventID" serial4 NOT NULL, -- Primary key, unique identifier for the event
    "EventDate_Day" int4 NULL, -- Day of the event
    "EventDate_MonthYear" int4 NULL, -- Month and year of the event (YYYYMM format)
    "EventDate_Year" int4 NULL, -- Year of the event
    "EventDate_Fractional" float8 NULL, -- Fractional date of the event (e.g., day of the year in decimal)
    
    "Actor1_Code" varchar NULL, -- Code representing Actor 1 (e.g., country code, organization code)
    "Actor1_Name" varchar NULL, -- Name of Actor 1
    "Actor1_CountryCode" varchar NULL, -- Country code of Actor 1
    "Actor1_KnownGroupCode" varchar NULL, -- Code representing a known group for Actor 1
    "Actor1_EthnicCode" varchar NULL, -- Ethnic code of Actor 1
    "Actor1_ReligionPrimaryCode" varchar NULL, -- Primary religion code of Actor 1
    "Actor1_ReligionSecondaryCode" varchar NULL, -- Secondary religion code of Actor 1
    "Actor1_TypePrimaryCode" varchar NULL, -- Primary type code of Actor 1 (e.g., individual, organization)
    "Actor1_TypeSecondaryCode" varchar NULL, -- Secondary type code of Actor 1
    "Actor1_TypeTertiaryCode" varchar NULL, -- Tertiary type code of Actor 1
    
    "Actor2_Code" varchar NULL, -- Code representing Actor 2
    "Actor2_Name" varchar NULL, -- Name of Actor 2
    "Actor2_CountryCode" varchar NULL, -- Country code of Actor 2
    "Actor2_KnownGroupCode" varchar NULL, -- Code representing a known group for Actor 2
    "Actor2_EthnicCode" varchar NULL, -- Ethnic code of Actor 2
    "Actor2_ReligionPrimaryCode" varchar NULL, -- Primary religion code of Actor 2
    "Actor2_ReligionSecondaryCode" varchar NULL, -- Secondary religion code of Actor 2
    "Actor2_TypePrimaryCode" varchar NULL, -- Primary type code of Actor 2
    "Actor2_TypeSecondaryCode" varchar NULL, -- Secondary type code of Actor 2
    "Actor2_TypeTertiaryCode" varchar NULL, -- Tertiary type code of Actor 2
    
    "IsRootEvent" bool NULL, -- Indicates if this event is a root event
    "Event_MainCode" varchar NULL, -- Main event code
    "Event_BaseCode" varchar NULL, -- Base event code
    "Event_RootCode" varchar NULL, -- Root event code
    "Event_QuadClass" int4 NULL, -- Quadrant classification of the event
    
    "GoldsteinScale_Score" float8 NULL, -- Goldstein scale score representing the tone of the event
    "Mentions_Count" int4 NULL, -- Number of mentions in sources
    "Sources_Count" int4 NULL, -- Number of distinct sources
    "Articles_Count" int4 NULL, -- Number of distinct articles
    "Tone_AverageScore" float8 NULL, -- Average tone score
    
    "Actor1Geo_TypeCode" int4 NULL, -- Geographic type code for Actor 1's location
    "Actor1Geo_FullName" varchar NULL, -- Full name of Actor 1's geographic location
    "Actor1Geo_CountryCode" varchar NULL, -- Country code of Actor 1's geographic location
    "Actor1Geo_Admin1Code" varchar NULL, -- Administrative division code 1 (e.g., state, province) for Actor 1's location
    "Actor1Geo_Latitude" float8 NULL, -- Latitude of Actor 1's location
    "Actor1Geo_Longitude" float8 NULL, -- Longitude of Actor 1's location
    "Actor1Geo_FeatureID" varchar NULL, -- Feature ID for Actor 1's location
    
    "Actor2Geo_TypeCode" int4 NULL, -- Geographic type code for Actor 2's location
    "Actor2Geo_FullName" varchar NULL, -- Full name of Actor 2's geographic location
    "Actor2Geo_CountryCode" varchar NULL, -- Country code of Actor 2's geographic location
    "Actor2Geo_Admin1Code" varchar NULL, -- Administrative division code 1 (e.g., state, province) for Actor 2's location
    "Actor2Geo_Latitude" float8 NULL, -- Latitude of Actor 2's location
    "Actor2Geo_Longitude" float8 NULL, -- Longitude of Actor 2's location
    "Actor2Geo_FeatureID" varchar NULL, -- Feature ID for Actor 2's location
    
    "ActionGeo_TypeCode" int4 NULL, -- Geographic type code for the location of the action/event
    "ActionGeo_FullName" varchar NULL, -- Full name of the action/event's geographic location
    "ActionGeo_CountryCode" varchar NULL, -- Country code of the action/event's geographic location
    "ActionGeo_Admin1Code" varchar NULL, -- Administrative division code 1 (e.g., state, province) for the action/event's location
    "ActionGeo_Latitude" float8 NULL, -- Latitude of the action/event's location
    "ActionGeo_Longitude" float8 NULL, -- Longitude of the action/event's location
    "ActionGeo_FeatureID" varchar NULL, -- Feature ID for the action/event's location
    
    "Event_DateAdded" int4 NULL, -- Date when the event was added (YYYYMMDD format)
    "Source_URL" varchar NULL -- URL of the source document
);

INSERT INTO silver.explicit_gdelt_events (
    "GlobalEventID",
    "EventDate_Day",
    "EventDate_MonthYear",
    "EventDate_Year",
    "EventDate_Fractional",
    "Actor1_Code",
    "Actor1_Name",
    "Actor1_CountryCode",
    "Actor1_KnownGroupCode",
    "Actor1_EthnicCode",
    "Actor1_ReligionPrimaryCode",
    "Actor1_ReligionSecondaryCode",
    "Actor1_TypePrimaryCode",
    "Actor1_TypeSecondaryCode",
    "Actor1_TypeTertiaryCode",
    "Actor2_Code",
    "Actor2_Name",
    "Actor2_CountryCode",
    "Actor2_KnownGroupCode",
    "Actor2_EthnicCode",
    "Actor2_ReligionPrimaryCode",
    "Actor2_ReligionSecondaryCode",
    "Actor2_TypePrimaryCode",
    "Actor2_TypeSecondaryCode",
    "Actor2_TypeTertiaryCode",
    "IsRootEvent",
    "Event_MainCode",
    "Event_BaseCode",
    "Event_RootCode",
    "Event_QuadClass",
    "GoldsteinScale_Score",
    "Mentions_Count",
    "Sources_Count",
    "Articles_Count",
    "Tone_AverageScore",
    "Actor1Geo_TypeCode",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_Admin1Code",
    "Actor1Geo_Latitude",
    "Actor1Geo_Longitude",
    "Actor1Geo_FeatureID",
    "Actor2Geo_TypeCode",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_Admin1Code",
    "Actor2Geo_Latitude",
    "Actor2Geo_Longitude",
    "Actor2Geo_FeatureID",
    "ActionGeo_TypeCode",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_Admin1Code",
    "ActionGeo_Latitude",
    "ActionGeo_Longitude",
    "ActionGeo_FeatureID",
    "Event_DateAdded",
    "Source_URL"
)
SELECT
    "GlobalEventID",
    "Day" AS "EventDate_Day",
    "MonthYear" AS "EventDate_MonthYear",
    "Year" AS "EventDate_Year",
    "FractionDate" AS "EventDate_Fractional",
    "Actor1Code" AS "Actor1_Code",
    "Actor1Name" AS "Actor1_Name",
    "Actor1CountryCode" AS "Actor1_CountryCode",
    "Actor1KnownGroupCode" AS "Actor1_KnownGroupCode",
    "Actor1EthnicCode" AS "Actor1_EthnicCode",
    "Actor1Religion1Code" AS "Actor1_ReligionPrimaryCode",
    "Actor1Religion2Code" AS "Actor1_ReligionSecondaryCode",
    "Actor1Type1Code" AS "Actor1_TypePrimaryCode",
    "Actor1Type2Code" AS "Actor1_TypeSecondaryCode",
    "Actor1Type3Code" AS "Actor1_TypeTertiaryCode",
    "Actor2Code" AS "Actor2_Code",
    "Actor2Name" AS "Actor2_Name",
    "Actor2CountryCode" AS "Actor2_CountryCode",
    "Actor2KnownGroupCode" AS "Actor2_KnownGroupCode",
    "Actor2EthnicCode" AS "Actor2_EthnicCode",
    "Actor2Religion1Code" AS "Actor2_ReligionPrimaryCode",
    "Actor2Religion2Code" AS "Actor2_ReligionSecondaryCode",
    "Actor2Type1Code" AS "Actor2_TypePrimaryCode",
    "Actor2Type2Code" AS "Actor2_TypeSecondaryCode",
    "Actor2Type3Code" AS "Actor2_TypeTertiaryCode",
    "IsRootEvent",
    "EventCode" AS "Event_MainCode",
    "EventBaseCode" AS "Event_BaseCode",
    "EventRootCode" AS "Event_RootCode",
    "QuadClass" AS "Event_QuadClass",
    "GoldsteinScale" AS "GoldsteinScale_Score",
    "NumMentions" AS "Mentions_Count",
    "NumSources" AS "Sources_Count",
    "NumArticles" AS "Articles_Count",
    "AvgTone" AS "Tone_AverageScore",
    "Actor1Geo_Type" AS "Actor1Geo_TypeCode",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code" AS "Actor1Geo_Admin1Code",
    "Actor1Geo_Lat" AS "Actor1Geo_Latitude",
    "Actor1Geo_Long" AS "Actor1Geo_Longitude",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type" AS "Actor2Geo_TypeCode",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code" AS "Actor2Geo_Admin1Code",
    "Actor2Geo_Lat" AS "Actor2Geo_Latitude",
    "Actor2Geo_Long" AS "Actor2Geo_Longitude",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type" AS "ActionGeo_TypeCode",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code" AS "ActionGeo_Admin1Code",
    "ActionGeo_Lat" AS "ActionGeo_Latitude",
    "ActionGeo_Long" AS "ActionGeo_Longitude",
    "ActionGeo_FeatureID",
    "DATEADDED" AS "Event_DateAdded",
    "SOURCEURL" AS "Source_URL"
FROM bronze.events;


