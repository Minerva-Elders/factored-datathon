-- Join using default gkg tables
WITH exploded_events AS (
	SELECT 
		CAST(UNNEST(STRING_TO_ARRAY("CAMEOEVENTIDS", ',')) AS INT) AS "ExplodedEvents",
		*
	FROM bronze.gkg
)

SELECT 
	ee."DATE" AS "NewsPublicationDate",
	
FROM bronze.events
LEFT JOIN exploded_events AS ee
	ON "GlobalEventID" = ee."ExplodedEvents"


-- exploding CAMEOEVENTIDS
select * 
from bronze.events as e
where "GlobalEventID" in (
	select cast(unnest(string_to_array("CAMEOEVENTIDS", ',')) as INT) AS parts
	from bronze.gkg
	where "UUID" = 'cbdef974-b299-4f9a-8fad-47f02fe18f0e'
)