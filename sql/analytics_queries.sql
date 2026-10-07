-- Number of earthquakes recorded in each region.
SELECT region, COUNT(*) AS total_events
FROM enriched_earthquake_events
GROUP BY region
ORDER BY total_events DESC, region;

-- Mean magnitude by region; values are kept in the source magnitude scale.
SELECT region, AVG(magnitude) AS avg_magnitude
FROM enriched_earthquake_events
GROUP BY region
ORDER BY avg_magnitude DESC, region;

-- Events classified as HIGH by the enrichment stage.
SELECT event_id, event_time_utc, region, magnitude, depth_km, severity_level
FROM enriched_earthquake_events
WHERE severity_level = 'HIGH'
ORDER BY event_time_utc DESC, event_id;

-- Mean time between the event and its ingestion, in seconds.
SELECT region, AVG(ingestion_latency_seconds) AS avg_latency_seconds
FROM enriched_earthquake_events
GROUP BY region
ORDER BY avg_latency_seconds DESC, region;
