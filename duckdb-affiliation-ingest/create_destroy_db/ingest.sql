CREATE TABLE records (
    record_id       VARCHAR PRIMARY KEY,
    doi             VARCHAR NOT NULL
);

CREATE TABLE values (
    value_id        VARCHAR PRIMARY KEY,
    value_type      VARCHAR NOT NULL,
    value_content   VARCHAR NOT NULL
);


CREATE TABLE process_record_relationships (
    process_record_id VARCHAR PRIMARY KEY,
    process_id        VARCHAR NOT NULL,
    record_id         VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,
    timestamp         TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE process_value_relationships (
    process_value_id  VARCHAR PRIMARY KEY,
    process_id        VARCHAR NOT NULL,
    value_id          VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,
    confidence_score  FLOAT,
    timestamp         TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE record_value_relationships (
    record_value_id   VARCHAR PRIMARY KEY,
    record_id         VARCHAR NOT NULL,
    value_id          VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,
    ordinal           INTEGER NOT NULL,
    process_id        VARCHAR NOT NULL,
    timestamp         TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE value_value_relationships (
    value_value_id    VARCHAR PRIMARY KEY,
    source_value_id   VARCHAR NOT NULL,
    target_value_id   VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,
    ordinal           INTEGER,
    process_id        VARCHAR NOT NULL,
    confidence_score  FLOAT,
    timestamp         TIMESTAMP WITH TIME ZONE NOT NULL
);


CREATE TABLE sources (
    source_id           VARCHAR PRIMARY KEY,
    source_name         VARCHAR,
    source_description  VARCHAR
);

CREATE TABLE processes (
    process_id           VARCHAR PRIMARY KEY,
    process_name         VARCHAR,
    process_description  VARCHAR
);

CREATE TABLE source_process_relationships (
    source_process_id VARCHAR PRIMARY KEY,
    source_id         VARCHAR NOT NULL,
    process_id        VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL,
    start_date        DATE NOT NULL,
    end_date          DATE
);


COPY records FROM './sample_ingest_files/records.csv' (FORMAT CSV, HEADER, QUOTE '"', ESCAPE '"');
COPY values FROM './sample_ingest_files/values.csv' (FORMAT CSV, HEADER, QUOTE '"', ESCAPE '"');
COPY process_record_relationships FROM './sample_ingest_files/process_record_relationships.csv' (FORMAT CSV, HEADER);
COPY process_value_relationships FROM './sample_ingest_files/process_value_relationships.csv' (FORMAT CSV, HEADER);
COPY record_value_relationships FROM './sample_ingest_files/record_value_relationships.csv' (FORMAT CSV, HEADER);
COPY value_value_relationships FROM './sample_ingest_files/value_value_relationships.csv' (FORMAT CSV, HEADER);


COPY sources FROM './sample_ingest_files/sources.csv' (FORMAT CSV, HEADER, QUOTE '"', ESCAPE '"');
COPY processes FROM './sample_ingest_files/processes.csv' (FORMAT CSV, HEADER, QUOTE '"', ESCAPE '"');
COPY source_process_relationships FROM './sample_ingest_files/source_process_relationships.csv' (FORMAT CSV, HEADER, DATEFORMAT '%Y-%m-%d');


SELECT 'records', COUNT(*) FROM records
UNION ALL
SELECT 'values', COUNT(*) FROM values
UNION ALL
SELECT 'process_record_relationships', COUNT(*) FROM process_record_relationships
UNION ALL
SELECT 'process_value_relationships', COUNT(*) FROM process_value_relationships
UNION ALL
SELECT 'record_value_relationships', COUNT(*) FROM record_value_relationships
UNION ALL
SELECT 'value_value_relationships', COUNT(*) FROM value_value_relationships;


SELECT 'sources', COUNT(*) FROM sources
UNION ALL
SELECT 'processes', COUNT(*) FROM processes
UNION ALL
SELECT 'source_process_relationships', COUNT(*) FROM source_process_relationships;


DESCRIBE records;
DESCRIBE values;
DESCRIBE process_record_relationships;
DESCRIBE process_value_relationships;
DESCRIBE record_value_relationships;
DESCRIBE value_value_relationships;
DESCRIBE sources;
DESCRIBE processes;
DESCRIBE source_process_relationships;
