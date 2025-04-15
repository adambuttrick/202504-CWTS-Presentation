-- Drop relationship tables
DROP TABLE IF EXISTS process_record_relationships;
DROP TABLE IF EXISTS process_value_relationships;
DROP TABLE IF EXISTS record_value_relationships;
DROP TABLE IF EXISTS value_value_relationships;

-- Drop main data tables
DROP TABLE IF EXISTS records;
DROP TABLE IF EXISTS values;

-- Drop optional metadata tables
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS processes;
DROP TABLE IF EXISTS source_process_relationships;