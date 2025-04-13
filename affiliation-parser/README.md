# Affiliation Extractor

Rust utility that extracts authors, affiliations, and institutional identifiers from the JSONL.gz files, and outputs CSVs describing structured relationship data between publications, authors, affiliations, and their identifiers (ROR IDs).


## Installation

```bash
cargo install affiliation-extractor
```

## Usage

```
affiliation-extractor --run-config <CONFIG_FILE> --output <OUTPUT_DIR> [OPTIONS]
```

### Options

- `--run-config`: Path to YAML configuration file (required)
- `--output, -o`: Output directory for CSV files (required)
- `--log-level, -l`: Logging level (DEBUG, INFO, WARN, ERROR; default: INFO)
- `--threads, -t`: Number of processing threads (0 for auto; default: 0)
- `--batch-size, -b`: Size of batches sent to writer thread (default: 10000)
- `--create-metadata-files`: Enable creation of source/process metadata files

## Configuration

### Run Configuration (YAML)

Defines a sequence of extraction tasks with their respective profiles and input directories:

```yaml
description: "Run description"
tasks:
  - description: "Task description"
    profile: "path/to/profile.json"
    input_dir: "path/to/input/files"
    filters:
      filter_key: "filter_value"
```

### Extraction Profiles (JSON)

Extraction profiles define how we should map data from source files to the extracted entity graph. They provide detailed instructions for navigating input documents, identifying entities, and establishing the relationships between them.

#### Core Components

```json
{
  "profile_description": "Human-readable description",
  "source_info": { ... },
  "process_info": { ... },
  "record_identifier": { ... },
  "deterministic_ids": { ... },
  "null_values": { ... },
  "filters": [ ... ],
  "entities": [ ... ]
}
```

#### Source and Process Metadata

```json
"source_info": {
  "source_id": "src_crossref",  // Unique identifier for the data source
  "source_name": "Crossref",    // Human-readable name
  "source_description": "Crossref metadata repository" // Description
},
"process_info": {
  "process_id": "proc_crossref_ingest", // Unique identifier for this process
  "process_name": "Crossref Ingest",    // Human-readable name
  "process_description": "Extracts authors, affiliations, RORs" // Description
}
```

#### Record Identification

```json
"record_identifier": {
  "path": "/DOI",    // JSON path to the primary identifier in each record
  "required": true   // Whether records without this field should be skipped
}
```

#### ID Generation

```json
"deterministic_ids": {
  "record_prefix": "rec",      // Prefix for record IDs
  "value_prefix": "val",       // Prefix for value IDs
  "value_format": "{value_type}:{value_content}" // Format for value hash inputs
}
```

#### Null Value Handling

```json
"null_values": {
  "null_author": {                              // Reference key
    "value_type": "author_name",                // Type of the null value
    "content": "<NULL_AUTHOR_NAME_CONTENT>"     // Content to use when null
  }
}
```

#### Filtering

```json
"filters": [
  {
    "cli_arg": "member",        // CLI argument name for this filter
    "path": "/member"           // Path to match against in the record
  },
  {
    "cli_arg": "doi_prefix",    // Another filter option
    "path": "/prefix",          // Primary path to check
    "fallback_from": "/DOI"     // Fallback path if primary doesn't exist
  }
]
```

#### Entity Extraction

The `entities` array defines what to extract from each record and how to organize the relationships:

```json
"entities": [
  {
    "name": "Author",                 // Descriptive name
    "path": "/author",                // JSON path to find entities
    "is_array": true,                 // Whether the path points to an array
    "relationship_to_record": "has_author",  // How this entity relates to the record
    
    "value_extraction": {             // How to extract the entity's value
      "type": "combine_fields",       // Extraction method
      "fields": ["given", "family"],  // Fields to combine (for combine_fields type)
      "separator": " ",               // Separator when combining
      "target_value_type": "author_name", // Type assigned to extracted value
      "use_null": "null_author"       // Null value key to use if extraction fails
    },
    
    "nested_entities": [ ... ],       // Child entities (similar structure)
    "related_values": [ ... ]         // Related values to extract
  }
]
```

##### Value Extraction Methods

1. **Field extraction**:
```json
"value_extraction": {
  "type": "field",
  "field": "name",                  // Field to extract
  "target_value_type": "affiliation", // Type assignment
  "use_null": "null_affiliation"    // Null fallback
}
```

2. **Field combination**:
```json
"value_extraction": {
  "type": "combine_fields",
  "fields": ["given", "family"],    // Fields to combine
  "separator": " ",                 // Separator between fields
  "target_value_type": "author_name",
  "use_null": "null_author"
}
```

##### Nested Entities

Child entities follow the same structure as top-level entities but are found within their parent:

```json
"nested_entities": [
  {
    "name": "Affiliation",
    "path": "/affiliation",         // Path relative to parent
    "is_array": true,
    "relationship_to_parent": "has_affiliation", // Relation to parent entity
    "value_extraction": { ... }
  }
]
```

##### Related Values

Values related to an entity but requiring additional filtering:

```json
"related_values": [
  {
    "name": "ROR_ID",
    "path": "/id",                  // Path to search for related values
    "is_array": true,
    "filter_condition": {           // Only process values matching this condition
      "field": "id-type",
      "equals": "ROR",
      "case_insensitive": true
    },
    "extract_value": {              // How to extract the value
      "type": "field",
      "field": "id",
      "target_value_type": "ror_id",
      "use_null": "null_ror_id"
    },
    "relationship_to_parent": "identified_by", // Relation to parent
    "take_first_match": true        // Only use first matching value
  }
]
```

## Output

Generates CSV files representing a graph database structure:
- `records.csv`: Publication records
- `values.csv`: Extracted values (authors, affiliations, identifiers)
- `process_record_relationships.csv`: Links between processes and records
- `process_value_relationships.csv`: Links between processes and values
- `record_value_relationships.csv`: Links between records and values
- `value_value_relationships.csv`: Links between values (e.g., author → affiliation)

## Path Notation

Two path formats are supported:
- JSON Pointer format (starts with `/`): `/path/to/field`
- Simple key format (for top-level access): `field_name`