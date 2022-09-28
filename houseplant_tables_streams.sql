// STEP 1.A Create houseplant reference table

CREATE TABLE houseplant_metadata (
  id STRING PRIMARY KEY
) WITH (
  kafka_topic='houseplant-metadata', 
  value_format='AVRO',
  partitions=6
);


// STEP 1.B Create readings stream

CREATE STREAM houseplant_readings (
  id STRING KEY
) WITH (
  kafka_topic='houseplant-readings',
  value_format='AVRO',
  partitions=6
);


// STEP 2. Enrich stream with houseplant table data

CREATE STREAM houseplant_readings_enriched WITH (
  kafka_topic='houseplant-readings-enriched',
  value_format='AVRO'
) AS 
SELECT 
    houseplant_readings.id               AS plant_id,
    houseplant_readings.ROWTIME          AS ts,
    houseplant_readings.moisture         AS moisture,
    houseplant_readings.temperature      AS temperature,
    houseplant_metadata.scientific_name  AS scientific_name,
    houseplant_metadata.common_name      AS common_name,
    houseplant_metadata.given_name       AS given_name,
    houseplant_metadata.temperature_low  AS temperature_low,
    houseplant_metadata.temperature_high AS temperature_high,
    houseplant_metadata.moisture_low     AS moisture_low,
    houseplant_metadata.moisture_high    AS moisture_high
FROM houseplant_readings
JOIN houseplant_metadata
ON houseplant_readings.id = houseplant_metadata.id
EMIT CHANGES;


// STEP 3. Create low readings table with messages

CREATE TABLE houseplant_low_readings WITH (   
    kafka_topic='houseplant-low-readings',
    format='AVRO'
) AS SELECT
    plant_id, 
    scientific_name,
    common_name,
    given_name,
    moisture_low,
    CONCAT(given_name, ' the ', common_name, ' (', scientific_name, ') is looking pretty dry...') AS message,
    COUNT(*) AS low_reading_count
FROM houseplant_readings_enriched
WINDOW TUMBLING (SIZE 6 HOURS, RETENTION 7 DAYS, GRACE PERIOD 10 MINUTES)
WHERE moisture < moisture_low
GROUP BY plant_id, scientific_name, common_name, given_name, moisture_low
HAVING COUNT(*) > 100
EMIT FINAL;