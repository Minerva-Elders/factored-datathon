
INSERT INTO silver.explicit_gkg (
    "GlobalKnowledgeGraphUUID",
    "RecordDate",
    "ArticleCount",
    "EventCounts",
    "Themes",
    "Locations",
    "Persons",
    "Organizations",
    "ToneAnalysis",
    "CAMEOEventIDs",
    "SourceIdentifiers",
    "SourceURLs"
)
SELECT
    "UUID" AS "GlobalKnowledgeGraphUUID",
    "DATE" AS "RecordDate",
    "NUMARTS" AS "ArticleCount",
    "COUNTS" AS "EventCounts",
    "THEMES" AS "Themes",
    "LOCATIONS" AS "Locations",
    "PERSONS" AS "Persons",
    "ORGANIZATIONS" AS "Organizations",
    "TONE" AS "ToneAnalysis",
    "CAMEOEVENTIDS" AS "CAMEOEventIDs",
    "SOURCES" AS "SourceIdentifiers",
    "SOURCEURLS" AS "SourceURLs"
FROM bronze.gkg;