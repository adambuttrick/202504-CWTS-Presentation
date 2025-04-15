# DOI Metadata Enrichment

Example code for presentation on DOI metadata enrichment to CWTS.

## Components

- **Affiliation Parser**: Rust utility that transforms source records into a value-centered graph
- **DuckDB AffiliationIngest**: Pipeline for loading, analyzing and generating enrichment data


## Usage

```bash
# Extract values from source files
affiliation-extractor --run-config config.yaml --output ./output

# Load into database
duckdb metadata.db < ingest.sql

# Generate enrichments
duckdb metadata.db < in_openalex_not_in_crossref.sql
```
