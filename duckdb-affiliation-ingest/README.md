# Affiliation Ingest DuckDB

This repo provides a demonstration example of value-centered data model and tools for enriching scholarly metadata, focusing on author affiliations and institutional identifiers.

## Setup Instructions

### Prerequisites
- DuckDB installed (https://duckdb.org/docs/installation/)
- Sample data files organized in the `./sample_ingest_files/` directory (provided)

### Getting Started

1. Clone this repository

2. Ensure your sample data files are in the correct location or update `ingest.sql` to the path for your data:
   ```
   ./sample_ingest_files/
   ├── records.csv
   ├── values.csv
   ├── process_record_relationships.csv
   ├── process_value_relationships.csv
   ├── record_value_relationships.csv
   ├── value_value_relationships.csv
   ├── sources.csv
   ├── processes.csv
   └── source_process_relationships.csv
   ```

3. Run the database initialization script:
   ```
   duckdb metadata.db < ingest.sql
   ```

4. To reset the database, run:
   ```
   duckdb metadata.db < drop_tables.sql
   ```

## Database Schema

### Core Tables
- **records**: Works identified by record_id and DOI
- **values**: Metadata elements (author names, affiliations, ROR IDs, etc.)

### Relationship Tables
- **process_record_relationships**: Links between processes and records
- **process_value_relationships**: Links between processes and values
- **record_value_relationships**: Links between records and values
- **value_value_relationships**: Links between different values

### Metadata Tables
- **sources**: Data sources (e.g., Crossref, OpenAlex)
- **processes**: Data processing events/pipelines
- **source_process_relationships**: Links between sources and processes

## Usage Example

To identify author affiliations that appear in OpenAlex but not in Crossref:

```
duckdb metadata.db < in_openalex_not_in_crossref.sql
```

This will generate a CSV file (`in_openalex_not_in_crossref.csv`) containing DOIs, author names, affiliations, and ROR IDs for works where OpenAlex provides affiliation data that Crossref doesn't have (indicated by a null assertion).
