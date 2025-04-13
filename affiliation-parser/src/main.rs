use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::Parser;
use csv::Writer;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::{DashMap, DashSet};
use flate2::read::GzDecoder;
use glob::glob;
use hex;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn, LevelFilter};
use rayon::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use serde_yaml;
use sha2::{Digest, Sha256};
use simple_logger::SimpleLogger;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use time::macros::format_description;
use uuid::Uuid;

#[cfg(target_os = "linux")]
use std::fs::read_to_string;
#[cfg(target_os = "windows")]
use std::process::Command as WinCommand;

mod memory_usage {
    use log::info;
    #[cfg(target_os = "linux")]
    use std::fs::read_to_string;
    #[cfg(target_os = "windows")]
    use std::process::Command as WinCommand;
    #[derive(Debug)]
    pub struct MemoryStats { pub rss_mb: f64, pub vm_size_mb: f64, pub percent: Option<f64> }
    #[cfg(target_os = "linux")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        let pid = std::process::id(); let status_file = format!("/proc/{}/status", pid); let content = read_to_string(status_file).ok()?;
        let mut vm_rss_kb = None; let mut vm_size_kb = None;
        for line in content.lines() { if line.starts_with("VmRSS:") { vm_rss_kb = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok()); } else if line.starts_with("VmSize:") { vm_size_kb = line.split_whitespace().nth(1).and_then(|s| s.parse::<f64>().ok()); } if vm_rss_kb.is_some() && vm_size_kb.is_some() { break; } }
        let rss_mb = vm_rss_kb? / 1024.0; let vm_size_mb = vm_size_kb? / 1024.0; let mut percent = None;
        if let Ok(meminfo) = read_to_string("/proc/meminfo") { if let Some(mem_total_kb) = meminfo.lines().find(|line| line.starts_with("MemTotal:")).and_then(|line| line.split_whitespace().nth(1)).and_then(|s| s.parse::<f64>().ok()) { if mem_total_kb > 0.0 { percent = Some((vm_rss_kb? / mem_total_kb) * 100.0); } } }
        Some(MemoryStats { rss_mb, vm_size_mb, percent })
    }
    #[cfg(target_os = "macos")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command; let pid = std::process::id();
        let ps_output = Command::new("ps").args(&["-o", "rss=", "-p", &pid.to_string()]).output().ok()?; let rss_kb = String::from_utf8_lossy(&ps_output.stdout).trim().parse::<f64>().ok()?;
        let vsz_output = Command::new("ps").args(&["-o", "vsz=", "-p", &pid.to_string()]).output().ok()?; let vsz_kb = String::from_utf8_lossy(&vsz_output.stdout).trim().parse::<f64>().ok()?;
        let rss_mb = rss_kb / 1024.0; let vm_size_mb = vsz_kb / 1024.0; let mut percent = None;
        if let Ok(hw_mem_output) = Command::new("sysctl").args(&["-n", "hw.memsize"]).output() { if let Ok(total_bytes_str) = String::from_utf8(hw_mem_output.stdout) { if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() { let total_kb = total_bytes / 1024.0; if total_kb > 0.0 { percent = Some((rss_kb / total_kb) * 100.0); } } } }
        Some(MemoryStats { rss_mb, vm_size_mb, percent })
    }
    #[cfg(target_os = "windows")]
    pub fn get_memory_usage() -> Option<MemoryStats> {
        use std::process::Command; let pid = std::process::id();
        let wmic_output = Command::new("wmic").args(&["process", "where", &format!("ProcessId={}", pid), "get", "WorkingSetSize,", "PageFileUsage", "/value"]).output().ok()?;
        let output_str = String::from_utf8_lossy(&wmic_output.stdout); let mut rss_bytes = None; let mut vm_kb = None;
        for line in output_str.lines() { if line.starts_with("PageFileUsage=") { vm_kb = line.split('=').nth(1).and_then(|s| s.trim().parse::<f64>().ok()); } else if line.starts_with("WorkingSetSize=") { rss_bytes = line.split('=').nth(1).and_then(|s| s.trim().parse::<f64>().ok()); } }
        let rss_mb = rss_bytes? / (1024.0 * 1024.0); let vm_size_mb = vm_kb? / 1024.0; let mut percent = None;
        if let Ok(mem_output) = Command::new("wmic").args(&["ComputerSystem", "get", "TotalPhysicalMemory", "/value"]).output() {
            let mem_str = String::from_utf8_lossy(&mem_output.stdout); if let Some(total_bytes_str) = mem_str.lines().find(|line| line.starts_with("TotalPhysicalMemory=")).and_then(|line| line.split('=').nth(1)) { if let Ok(total_bytes) = total_bytes_str.trim().parse::<f64>() { if total_bytes > 0.0 { percent = Some((rss_bytes? / total_bytes) * 100.0); } } }
        }
        Some(MemoryStats { rss_mb, vm_size_mb, percent })
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    pub fn get_memory_usage() -> Option<MemoryStats> { None }
    pub fn log_memory_usage(note: &str) { if let Some(stats) = get_memory_usage() { let percent_str = stats.percent.map_or_else(|| "N/A".to_string(), |p| format!("{:.1}%", p)); info!("Memory usage ({}): {:.1} MB physical (RSS), {:.1} MB virtual/commit, {} of system memory", note, stats.rss_mb, stats.vm_size_mb, percent_str); } else { info!("Memory usage tracking not available or failed on this platform ({})", std::env::consts::OS); } }
}


#[derive(Deserialize, Debug, Clone)]
struct RunConfig {
    description: Option<String>,
    tasks: Vec<TaskConfig>,
}

#[derive(Deserialize, Debug, Clone)]
struct TaskConfig {
    description: Option<String>,
    profile: PathBuf,
    input_dir: PathBuf,
    #[serde(default)]
    filters: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct Profile {
    profile_description: String,
    source_info: SourceInfo,
    process_info: ProcessInfo,
    record_identifier: RecordIdentifierConfig,
    deterministic_ids: DeterministicIdConfig,
    null_values: HashMap<String, NullValueConfig>,
    filters: Option<Vec<FilterConfig>>,
    entities: Vec<EntityConfig>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct SourceInfo {
    source_id: String,
    source_name: Option<String>,
    source_description: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct ProcessInfo {
    process_id: String,
    process_name: Option<String>,
    process_description: Option<String>,
}


#[derive(Deserialize, Debug, Clone, PartialEq)]
struct RecordIdentifierConfig {
    path: String,
    required: bool,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct DeterministicIdConfig {
    record_prefix: String,
    value_prefix: String,
    value_format: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct NullValueConfig {
    value_type: String,
    content: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct FilterConfig {
    cli_arg: String,
    path: String,
    fallback_from: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct EntityConfig {
    name: String,
    path: String,
    is_array: bool,
    relationship_to_record: Option<String>,
    relationship_to_parent: Option<String>,
    value_extraction: Option<ValueExtractionConfig>,
    nested_entities: Option<Vec<EntityConfig>>,
    related_values: Option<Vec<RelatedValueConfig>>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct RelatedValueConfig {
    name: String,
    path: String,
    is_array: bool,
    filter_condition: Option<FilterConditionConfig>,
    extract_value: ValueExtractionConfig,
    relationship_to_parent: String,
    take_first_match: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(tag = "type")]
enum ValueExtractionConfig {
    #[serde(rename = "field")]
    Field { field: String, target_value_type: String, use_null: Option<String> },
    #[serde(rename = "combine_fields")]
    CombineFields { fields: Vec<String>, separator: String, target_value_type: String, use_null: Option<String> },
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
struct FilterConditionConfig {
    field: String,
    equals: String,
    case_insensitive: Option<bool>,
}


impl ValueExtractionConfig {
     fn get_null_ref(&self) -> Option<&String> {
         match self {
             ValueExtractionConfig::Field { use_null, .. } => use_null.as_ref(),
             ValueExtractionConfig::CombineFields { use_null, .. } => use_null.as_ref(),
         }
     }
}


fn generate_deterministic_id(prefix: &str, content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let result = hasher.finalize();
    format!("{}-sha256-{}", prefix, hex::encode(result))
}


#[derive(Parser, Clone)]
#[command(name = "Affiliation Extractor - Multi Profile Runner")]
#[command(about = "Extracts affiliation data from JSONL.gz files based on multiple profiles defined in a run configuration.")]
#[command(version = "1.0.0")]
struct Cli {
    #[arg(long, help = "Path to the run configuration YAML file", required = true)]
    run_config: PathBuf,
    #[arg(short, long, help = "Output directory for CSV files", required = true)]
    output: String,
    #[arg(short, long, default_value = "INFO", help = "Logging level (DEBUG, INFO, WARN, ERROR)")]
    log_level: String,
    #[arg(short, long, default_value = "0", help = "Number of threads to use (0 for auto)")]
    threads: usize,
    #[arg(short, long, default_value = "10000", help = "Size of batches sent to writer thread")]
    batch_size: usize,
    #[arg(long, help = "Flag to enable creation of source/process metadata files")]
    create_metadata_files: bool,
}

#[derive(Debug, Clone)] struct RecordRow { record_id: String, doi: String }
#[derive(Debug, Clone)] struct ValueRow { value_id: String, value_type: String, value_content: String }
#[derive(Debug, Clone)] struct ProcessRecordRow { process_record_id: String, process_id: String, record_id: String, relationship_type: String, timestamp: String }
#[derive(Debug, Clone)] struct ProcessValueRow { process_value_id: String, process_id: String, value_id: String, relationship_type: String, confidence_score: f32, timestamp: String }
#[derive(Debug, Clone)] struct RecordValueRow { record_value_id: String, record_id: String, value_id: String, relationship_type: String, ordinal: i32, process_id: String, timestamp: String }
#[derive(Debug, Clone)] struct ValueValueRow { value_value_id: String, source_value_id: String, target_value_id: String, relationship_type: String, ordinal: Option<i32>, process_id: String, confidence_score: f32, timestamp: String }

#[derive(Debug, Default)]
struct OutputBatch {
    records: Vec<RecordRow>,
    values: Vec<ValueRow>,
    process_record_relationships: Vec<ProcessRecordRow>,
    process_value_relationships: Vec<ProcessValueRow>,
    record_value_relationships: Vec<RecordValueRow>,
    value_value_relationships: Vec<ValueValueRow>,
}
impl OutputBatch {
    fn is_empty(&self) -> bool { self.records.is_empty() && self.values.is_empty() && self.process_record_relationships.is_empty() && self.process_value_relationships.is_empty() && self.record_value_relationships.is_empty() && self.value_value_relationships.is_empty() }
    fn count_rows(&self) -> usize { self.records.len() + self.values.len() + self.process_record_relationships.len() + self.process_value_relationships.len() + self.record_value_relationships.len() + self.value_value_relationships.len() }
}

type RecordIdMap = Arc<DashMap<String, String>>;
type ValueIdMap = Arc<DashMap<(String, String), String>>;
type WrittenValueIdSet = Arc<DashSet<String>>;
type NullValueIdMap = Arc<HashMap<String, String>>;

struct JsonlProcessor {
    profile: Arc<Profile>,
    null_value_ids: NullValueIdMap,
    record_id_map: RecordIdMap,
    value_id_map: ValueIdMap,
    timestamp_str: Arc<String>,
    active_filters: HashMap<String, String>,
}

fn generate_relationship_uuid() -> String { Uuid::new_v4().to_string() }

impl JsonlProcessor {
    fn new(
        profile: Arc<Profile>,
        null_value_ids: NullValueIdMap,
        record_id_map: RecordIdMap,
        value_id_map: ValueIdMap,
        timestamp_str: Arc<String>,
        active_filters: HashMap<String, String>,
    ) -> Self {
        Self {
            profile,
            null_value_ids,
            record_id_map,
            value_id_map,
            timestamp_str,
            active_filters,
        }
    }

    fn process(&self, filepath: &Path) -> Result<OutputBatch, (PathBuf, anyhow::Error)> {
        let file = File::open(filepath).map_err(|e| (filepath.to_path_buf(), anyhow::Error::new(e).context(format!("Failed to open file: {}", filepath.display()))))?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        let mut batch = OutputBatch::default();
        let mut lines_processed = 0;
        let mut records_processed = 0;
        let mut records_missing_id = 0;
        let mut records_filtered_out = 0;
        let mut json_parsing_errors = 0;

        for (line_num, line_result) in reader.lines().enumerate() {
            lines_processed += 1;
            let line_str = match line_result {
                Ok(s) => s,
                Err(e) => { warn!("Error reading line {} from {}: {}", line_num + 1, filepath.display(), e); continue; }
            };
            if line_str.trim().is_empty() { continue; }

            match serde_json::from_str::<Value>(&line_str) {
                Ok(record_json) => {
                    records_processed += 1;

                    if self.should_filter_out(&record_json).unwrap_or(false) {
                        records_filtered_out += 1;
                        continue;
                    }

                    let primary_id_value = match self.get_value_at_path(&record_json, &self.profile.record_identifier.path)
                            .and_then(|v| v.as_str())
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                    {
                        Some(id_val) => id_val.to_string(),
                        None => {
                            if self.profile.record_identifier.required {
                                records_missing_id += 1;
                                debug!("Skipping record in {} line {} due to missing required identifier at path '{}'", filepath.display(), line_num + 1, self.profile.record_identifier.path);
                                continue;
                            } else {
                                records_missing_id += 1;
                                debug!("Skipping record in {} line {} with missing optional identifier at path '{}'", filepath.display(), line_num + 1, self.profile.record_identifier.path);
                                continue;
                            }
                        }
                    };

                    let record_id = self.record_id_map.entry(primary_id_value.clone())
                        .or_insert_with(|| self.generate_record_id(&primary_id_value))
                        .value()
                        .clone();

                    batch.records.push(RecordRow { record_id: record_id.clone(), doi: primary_id_value.clone() });
                    batch.process_record_relationships.push(ProcessRecordRow {
                        process_record_id: generate_relationship_uuid(),
                        process_id: self.profile.process_info.process_id.clone(),
                        record_id: record_id.clone(),
                        relationship_type: "ingested".to_string(),
                        timestamp: self.timestamp_str.to_string(),
                    });

                    if let Err(e) = self.process_json_node(
                        &record_json,
                        &record_id,
                        None,
                        &self.profile.entities,
                        &mut batch,
                    ) {
                         warn!("Error processing entities for record {} in {}: {}", record_id, filepath.display(), e);
                    }

                },
                Err(e) => {
                    json_parsing_errors += 1;
                    warn!("Error parsing JSON from {}:{}: {}", filepath.display(), line_num + 1, e);
                }
            }
        }
        debug!("Finished {}: Lines={}, Records={}, Skipped(NoID)={}, Filtered={}, JsonErrors={}",
            filepath.display(), lines_processed, records_processed, records_missing_id, records_filtered_out, json_parsing_errors);

        Ok(batch)
    }

    fn process_json_node(
        &self,
        current_node: &Value,
        record_id: &str,
        parent_value_id: Option<&str>,
        entity_configs: &[EntityConfig],
        batch: &mut OutputBatch,
    ) -> Result<()> {
        for config in entity_configs {
            if let Some(entity_data) = self.get_value_at_path(current_node, &config.path) {
                let items_to_process = if config.is_array {
                    entity_data.as_array().map(|a| a.iter().cloned().collect()).unwrap_or_default()
                } else {
                    vec![entity_data.clone()]
                };

                for (ordinal, item_node) in items_to_process.into_iter().enumerate() {
                    let current_ordinal = (ordinal + 1) as i32;
                    let mut current_entity_value_id: Option<String> = None;

                    if let Some(val_config) = &config.value_extraction {
                         let (extracted_content, value_type) = self.extract_value(&item_node, val_config)?;
                         match self.get_or_create_value_id(&extracted_content, &value_type, val_config.get_null_ref()) {
                             Ok((final_content, value_id)) => {
                                 self.add_value_rows(&value_id, &value_type, &final_content, batch)?;
                                 current_entity_value_id = Some(value_id.clone());

                                 if let Some(parent_id) = parent_value_id {
                                     if let Some(rel_type) = &config.relationship_to_parent {
                                         self.add_value_value_relationship(parent_id, &value_id, rel_type, Some(current_ordinal), batch)?;
                                     }
                                 } else {
                                     if let Some(rel_type) = &config.relationship_to_record {
                                          self.add_record_value_relationship(record_id, &value_id, rel_type, current_ordinal, batch)?;
                                     }
                                 }
                             },
                             Err(e) => {
                                 warn!("Failed to get/create value ID for entity '{}' in record {}: {}", config.name, record_id, e);
                                 continue;
                             }
                         }
                    }

                    let parent_id_for_children = current_entity_value_id.as_deref().or(parent_value_id);

                    if let Some(pid) = parent_id_for_children {
                        if let Some(nested_configs) = &config.nested_entities {
                            if let Err(e) = self.process_json_node(&item_node, record_id, Some(pid), nested_configs, batch) {
                                 warn!("Error processing nested entities for {} under parent {}: {}", config.name, pid, e);
                            }
                        }
                        if let Some(related_configs) = &config.related_values {
                            if let Err(e) = self.process_related_values(&item_node, pid, related_configs, batch) {
                                 warn!("Error processing related values for {} under parent {}: {}", config.name, pid, e);
                            }
                        }
                    } else if config.nested_entities.is_some() || config.related_values.is_some() {
                         warn!("Cannot process nested/related entities for '{}' in record {} because no parent value ID was established or inherited.", config.name, record_id);
                    }
                }
            }
        }
        Ok(())
    }

   fn process_related_values(
        &self,
        current_node: &Value,
        parent_value_id: &str,
        related_configs: &[RelatedValueConfig],
        batch: &mut OutputBatch,
    ) -> Result<()> {
        for config in related_configs {
            if let Some(related_data_node) = self.get_value_at_path(current_node, &config.path) {
                let items_to_check = if config.is_array {
                    related_data_node.as_array().map(|a| a.iter().cloned().collect()).unwrap_or_default()
                } else {
                    vec![related_data_node.clone()]
                };

                let mut found_match_for_config = false;

                for item in items_to_check {
                    let mut condition_met = true;
                    if let Some(condition) = &config.filter_condition {
                        match self.check_filter_condition(&item, condition) {
                            Ok(met) => condition_met = met,
                            Err(e) => {
                                warn!("Error checking filter condition for related value '{}' (path '{}') under parent {}: {}. Skipping item.", config.name, config.path, parent_value_id, e);
                                continue;
                            }
                        }
                    }

                    if condition_met {
                        match self.extract_value(&item, &config.extract_value) {
                            Ok((extracted_content, value_type)) => {
                                match self.get_or_create_value_id(&extracted_content, &value_type, config.extract_value.get_null_ref()) {
                                    Ok((final_content, value_id)) => {
                                        self.add_value_rows(&value_id, &value_type, &final_content, batch)?;
                                        self.add_value_value_relationship(parent_value_id, &value_id, &config.relationship_to_parent, None, batch)?;
                                        found_match_for_config = true;

                                        if config.take_first_match.unwrap_or(false) {
                                            break;
                                        }
                                    },
                                    Err(e) => {
                                         warn!("Failed to get/create value ID for related value '{}' (path '{}', field '{}') under parent {}: {}", config.name, config.path, "", parent_value_id, e);
                                    }
                                }
                            },
                            Err(e) => {
                                warn!("Failed to extract related value '{}' (path '{}') from item under parent {}: {}", config.name, config.path, parent_value_id, e);
                            }
                        }
                    }
                }
                 if !found_match_for_config && config.filter_condition.is_some() {
                    if let Some(null_key) = config.extract_value.get_null_ref() {
                        debug!("Path '{}' existed for parent {}, but no item met filter condition for related value '{}'. Applying null default '{}'.", config.path, parent_value_id, config.name, null_key);
                        if let Some(null_config) = self.profile.null_values.get(null_key) {
                             if let Some(null_id) = self.null_value_ids.get(null_key) {
                                 self.add_value_rows(null_id, &null_config.value_type, &null_config.content, batch)?;
                                 self.add_value_value_relationship(parent_value_id, null_id, &config.relationship_to_parent, None, batch)?;
                             } else { warn!("(Post-filter) Precomputed null ID not found for key: {}", null_key); }
                        } else { warn!("(Post-filter) Null value config not found for key: {}", null_key); }
                    }
                 }

            } else {
                if let Some(null_key) = config.extract_value.get_null_ref() {
                     debug!("Path '{}' missing for parent {}, applying null default '{}' for related value '{}'.", config.path, parent_value_id, null_key, config.name);
                     if let Some(null_config) = self.profile.null_values.get(null_key) {
                         if let Some(null_id) = self.null_value_ids.get(null_key) {
                             self.add_value_rows(null_id, &null_config.value_type, &null_config.content, batch)?;
                             self.add_value_value_relationship(parent_value_id, null_id, &config.relationship_to_parent, None, batch)?;
                         } else {
                              warn!("Could not find precomputed null ID for key '{}' when handling missing path '{}' for parent {}", null_key, config.path, parent_value_id);
                         }
                    } else {
                         warn!("Could not find null value config for key '{}' when handling missing path '{}' for parent {}", null_key, config.path, parent_value_id);
                    }
                }
            }
        }
        Ok(())
    }


    fn should_filter_out(&self, record: &Value) -> Result<bool> {
        if self.active_filters.is_empty() { return Ok(false); }

        for (key, required_value) in &self.active_filters {
            if let Some(profile_filter_config) = self.profile.filters.as_ref().and_then(|filters| filters.iter().find(|f| f.cli_arg == *key)) {
                let mut current_value: Option<String> = self.get_value_at_path(record, &profile_filter_config.path)
                    .and_then(|v| v.as_str().map(|s| s.to_string()).or_else(|| Some(v.to_string())));

                if current_value.is_none() {
                    if let Some(fallback_path) = &profile_filter_config.fallback_from {
                        if let Some(primary_id) = self.get_value_at_path(record, fallback_path).and_then(|v| v.as_str()) {
                             if (fallback_path == "/DOI" || fallback_path == "DOI") && key == "doi_prefix" {
                                 current_value = primary_id.split_once('/').map(|(pfx, _)| pfx.to_string());
                             }
                        }
                    }
                }

                if current_value.as_ref().map_or(true, |cv| cv != required_value) {
                    return Ok(true);
                }
            } else {
                 warn!("Active filter key '{}' not found in profile filter definitions.", key);
            }
        }
        Ok(false)
    }

    fn get_value_at_path<'a>(&self, node: &'a Value, path: &str) -> Option<&'a Value> {
        if path.starts_with('/') {
            node.pointer(path)
        } else {
            node.get(path)
        }
    }

    fn extract_value(&self, node: &Value, config: &ValueExtractionConfig) -> Result<(Option<String>, String)> {
        match config {
            ValueExtractionConfig::Field { field, target_value_type, .. } => {
                let val = self.get_value_at_path(node, &format!("/{}", field))
                    .and_then(|v| v.as_str().map(|s| s.trim().to_string()).or_else(|| if v.is_number() || v.is_boolean() { Some(v.to_string()) } else {None}))
                    .filter(|s| !s.is_empty());
                Ok((val, target_value_type.clone()))
            },
            ValueExtractionConfig::CombineFields { fields, separator, target_value_type, .. } => {
                let parts: Vec<String> = fields.iter().filter_map(|f|
                    self.get_value_at_path(node, &format!("/{}", f))
                        .and_then(|v| v.as_str().map(|s| s.trim().to_string()))
                        .filter(|s| !s.is_empty())
                ).collect();
                let combined = if parts.is_empty() { None } else { Some(parts.join(separator)) };
                Ok((combined, target_value_type.clone()))
           },
        }
    }

     fn get_or_create_value_id(
        &self,
        extracted_content: &Option<String>,
        value_type: &str,
        null_ref: Option<&String>,
    ) -> Result<(String, String)> {
        if let Some(content) = extracted_content {
            let value_id = self.value_id_map.entry((value_type.to_string(), content.clone()))
                .or_insert_with(|| self.generate_value_id(value_type, content))
                .value()
                .clone();
            Ok((content.clone(), value_id))
        } else if let Some(null_key) = null_ref {
            if let Some(null_config) = self.profile.null_values.get(null_key) {
                if let Some(null_id) = self.null_value_ids.get(null_key) {
                    Ok((null_config.content.clone(), null_id.clone()))
                } else {
                    Err(anyhow::anyhow!("Precomputed null ID not found for key: {}", null_key))
                }
            } else {
                Err(anyhow::anyhow!("Null value configuration not found for key: {}", null_key))
            }
        } else {
            Err(anyhow::anyhow!("Value extraction failed for type '{}' and no null default specified", value_type))
        }
    }

    fn generate_record_id(&self, primary_id_value: &str) -> String {
        generate_deterministic_id(&self.profile.deterministic_ids.record_prefix, primary_id_value)
    }

    fn generate_value_id(&self, value_type: &str, content: &str) -> String {
        let id_hashing_content = format!("{}:{}", value_type, content);
        generate_deterministic_id(&self.profile.deterministic_ids.value_prefix, &id_hashing_content)
    }

     fn add_value_rows(&self, value_id: &str, value_type: &str, value_content: &str, batch: &mut OutputBatch) -> Result<()> {
        batch.values.push(ValueRow {
            value_id: value_id.to_string(),
            value_type: value_type.to_string(),
            value_content: value_content.to_string(),
        });
        batch.process_value_relationships.push(ProcessValueRow {
            process_value_id: generate_relationship_uuid(),
            process_id: self.profile.process_info.process_id.clone(),
            value_id: value_id.to_string(),
            relationship_type: "created".to_string(),
            confidence_score: 1.0,
            timestamp: self.timestamp_str.to_string(),
        });
        Ok(())
     }

    fn add_value_value_relationship(&self, source_id: &str, target_id: &str, rel_type: &str, ordinal: Option<i32>, batch: &mut OutputBatch) -> Result<()> {
         batch.value_value_relationships.push(ValueValueRow {
             value_value_id: generate_relationship_uuid(),
             source_value_id: source_id.to_string(),
             target_value_id: target_id.to_string(),
             relationship_type: rel_type.to_string(),
             ordinal,
             process_id: self.profile.process_info.process_id.clone(),
             confidence_score: 1.0,
             timestamp: self.timestamp_str.to_string(),
         });
         Ok(())
    }
    fn add_record_value_relationship(&self, record_id: &str, value_id: &str, rel_type: &str, ordinal: i32, batch: &mut OutputBatch) -> Result<()> {
        batch.record_value_relationships.push(RecordValueRow {
            record_value_id: generate_relationship_uuid(),
            record_id: record_id.to_string(),
            value_id: value_id.to_string(),
            relationship_type: rel_type.to_string(),
            ordinal,
            process_id: self.profile.process_info.process_id.clone(),
            timestamp: self.timestamp_str.to_string(),
        });
        Ok(())
    }

    fn check_filter_condition(&self, node: &Value, condition: &FilterConditionConfig) -> Result<bool> {
        if let Some(field_value) = self.get_value_at_path(node, &format!("/{}", condition.field)) {
             if let Some(field_str) = field_value.as_str() {
                 let target_str = &condition.equals;
                 let case_insensitive = condition.case_insensitive.unwrap_or(false);
                 if case_insensitive {
                     return Ok(field_str.eq_ignore_ascii_case(target_str));
                 } else {
                     return Ok(field_str == target_str);
                 }
             } else if field_value.is_number() || field_value.is_boolean() {
                 return Ok(field_value.to_string().eq_ignore_ascii_case(&condition.equals));
             }
        }
        Ok(false)
    }
}

trait OutputWriter: Send {
    fn write_batch(&mut self, batch: OutputBatch) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn report_files_created(&self) -> usize;
    fn report_rows_written(&self) -> HashMap<String, usize>;
    fn finalize(&mut self) -> Result<()>;
}

const DATA_TABLE_NAMES: [&str; 6] = [
    "records", "values", "process_record_relationships", "process_value_relationships", "record_value_relationships", "value_value_relationships",
];
const METADATA_TABLE_NAMES: [&str; 3] = ["sources", "processes", "source_process_relationships"];


type ProcessValueRelKey = (String, String, String);
type ValueValueRelKey = (String, String, String, Option<i32>);

struct MultiTableCsvOutput {
    data_writers: HashMap<String, Writer<File>>,
    output_dir: PathBuf,
    rows_written: Arc<DashMap<String, AtomicUsize>>,
    files_created: usize,
    written_value_ids: WrittenValueIdSet,
    written_process_value_rels: Arc<DashSet<ProcessValueRelKey>>,
    written_value_value_rels: Arc<DashSet<ValueValueRelKey>>,
    all_profiles_in_run: Vec<Arc<Profile>>,
    null_value_ids: NullValueIdMap,
    create_metadata_files: bool,
}

impl MultiTableCsvOutput {
    fn new(
        output_dir: PathBuf,
        written_value_ids: WrittenValueIdSet,
        written_process_value_rels: Arc<DashSet<ProcessValueRelKey>>,
        written_value_value_rels: Arc<DashSet<ValueValueRelKey>>,
        all_profiles_in_run: Vec<Arc<Profile>>,
        null_value_ids: NullValueIdMap,
        create_metadata_files: bool,
    ) -> Result<Self> {
        fs::create_dir_all(&output_dir)?;
        let mut data_writers = HashMap::new();
        let rows_written = Arc::new(DashMap::new());
        let mut files_created = 0;

        let data_headers: HashMap<&str, Vec<&str>> = [
            ("records", vec!["record_id", "doi"]),
            ("values", vec!["value_id", "value_type", "value_content"]),
            ("process_record_relationships", vec!["process_record_id", "process_id", "record_id", "relationship_type", "timestamp"]),
            ("process_value_relationships", vec!["process_value_id", "process_id", "value_id", "relationship_type", "confidence_score", "timestamp"]),
            ("record_value_relationships", vec!["record_value_id", "record_id", "value_id", "relationship_type", "ordinal", "process_id", "timestamp"]),
            ("value_value_relationships", vec!["value_value_id", "source_value_id", "target_value_id", "relationship_type", "ordinal", "process_id", "confidence_score", "timestamp"]),
        ].iter().cloned().collect();

        for &table_name in DATA_TABLE_NAMES.iter() {
            let file_path = output_dir.join(format!("{}.csv", table_name));
            let file = File::create(&file_path)?;
            files_created += 1;
            let mut writer = Writer::from_writer(file);
            if let Some(header_vec) = data_headers.get(table_name) {
                writer.write_record(header_vec)?;
            } else {
                warn!("No headers defined for data table: {}", table_name);
            }
            writer.flush()?;
            data_writers.insert(table_name.to_string(), writer);
            rows_written.insert(table_name.to_string(), AtomicUsize::new(0));
        }

        if create_metadata_files {
            info!("Creating metadata files based on profiles used in the run...");
            let mut seen_source_ids = HashSet::new();
            let mut seen_process_ids = HashSet::new();

             let metadata_headers: HashMap<&str, Vec<&str>> = [
                 ("sources", vec!["source_id", "source_name", "source_description"]),
                 ("processes", vec!["process_id", "process_name", "process_description"]),
                 ("source_process_relationships", vec!["source_process_id", "source_id", "process_id", "relationship_type", "start_date", "end_date"]),
             ].iter().cloned().collect();

             let mut metadata_writers = HashMap::new();
             for &table_name in METADATA_TABLE_NAMES.iter() {
                 let file_path = output_dir.join(format!("{}.csv", table_name));
                 let file = File::create(&file_path)?;
                 files_created += 1;
                 let mut writer = Writer::from_writer(file);
                  if let Some(header_vec) = metadata_headers.get(table_name) { writer.write_record(header_vec)?; } else { warn!("No headers defined for metadata table: {}", table_name); }
                 metadata_writers.insert(table_name.to_string(), writer);
                 rows_written.insert(table_name.to_string(), AtomicUsize::new(0));
             }

             let current_date = Utc::now().format("%Y-%m-%d").to_string();
             for profile in &all_profiles_in_run {
                 let source_id = &profile.source_info.source_id;
                 let process_id = &profile.process_info.process_id;

                 if seen_source_ids.insert(source_id.clone()) {
                     if let Some(writer) = metadata_writers.get_mut("sources") {
                         let _count = rows_written.entry("sources".to_string()).or_insert(AtomicUsize::new(0)).value().fetch_add(1, Ordering::Relaxed);
                         writer.write_record(&[
                             source_id,
                             profile.source_info.source_name.as_deref().unwrap_or(""),
                             profile.source_info.source_description.as_deref().unwrap_or(""),
                         ])?;
                     }
                 }
                 if seen_process_ids.insert(process_id.clone()) {
                     if let Some(writer) = metadata_writers.get_mut("processes") {
                         let _count = rows_written.entry("processes".to_string()).or_insert(AtomicUsize::new(0)).value().fetch_add(1, Ordering::Relaxed);
                         writer.write_record(&[
                             process_id,
                             profile.process_info.process_name.as_deref().unwrap_or(""),
                             profile.process_info.process_description.as_deref().unwrap_or(""),
                         ])?;
                     }
                 }

                 if let Some(writer) = metadata_writers.get_mut("source_process_relationships") {
                      let sp_id = generate_relationship_uuid();
                      let _count = rows_written.entry("source_process_relationships".to_string()).or_insert(AtomicUsize::new(0)).value().fetch_add(1, Ordering::Relaxed);
                      writer.write_record(&[
                          &sp_id,
                          source_id,
                          process_id,
                          "defined_by",
                          &current_date,
                          "",
                      ])?;
                 }
             }

             for (_name, writer) in metadata_writers.iter_mut() {
                 writer.flush()?;
             }
             info!("Metadata files created and populated.");
        } else {
            info!("Skipping creation of metadata files.");
        }

        Ok(Self {
            data_writers,
            output_dir,
            rows_written,
            files_created,
            written_value_ids,
            written_process_value_rels,
            written_value_value_rels,
            all_profiles_in_run,
            null_value_ids,
            create_metadata_files,
        })
    }

    fn get_writer(&mut self, table_name: &str) -> Result<&mut Writer<File>> {
        self.data_writers.get_mut(table_name)
            .ok_or_else(|| anyhow::anyhow!("Writer for table '{}' not found", table_name))
    }

    fn increment_row_count(&self, table_name: &str, count: usize) {
        if let Some(counter) = self.rows_written.get(table_name) {
            counter.fetch_add(count, Ordering::Relaxed);
        } else {
             if self.create_metadata_files && METADATA_TABLE_NAMES.contains(&table_name) {
             } else {
                 warn!("Attempted to increment row count for unknown or non-initialized table: {}", table_name);
             }
        }
    }
}

impl OutputWriter for MultiTableCsvOutput {
    fn write_batch(&mut self, batch: OutputBatch) -> Result<()> {
        if !batch.records.is_empty() {
            let writer = self.get_writer("records")?;
            let count = batch.records.len();
            for row in batch.records { writer.write_record(&[row.record_id, row.doi])?; }
            self.increment_row_count("records", count);
        }

        if !batch.process_record_relationships.is_empty() {
            let writer = self.get_writer("process_record_relationships")?;
            let count = batch.process_record_relationships.len();
            for row in batch.process_record_relationships { writer.write_record(&[row.process_record_id, row.process_id, row.record_id, row.relationship_type, row.timestamp])?; }
            self.increment_row_count("process_record_relationships", count);
        }

        if !batch.process_value_relationships.is_empty() {
            let mut new_rels_to_write = Vec::new();
            for row in batch.process_value_relationships {
                let key: ProcessValueRelKey = (
                    row.process_id.clone(),
                    row.value_id.clone(),
                    row.relationship_type.clone()
                );
                if self.written_process_value_rels.insert(key) {
                    new_rels_to_write.push(row);
                }
            }

            if !new_rels_to_write.is_empty() {
                let writer = self.get_writer("process_value_relationships")?;
                let count = new_rels_to_write.len();
                for row in new_rels_to_write {
                    writer.write_record(&[
                        row.process_value_id,
                        row.process_id,
                        row.value_id, // Corrected from original code which had record_id
                        row.relationship_type,
                        row.confidence_score.to_string(),
                        row.timestamp
                    ])?;
                }
                self.increment_row_count("process_value_relationships", count);
            }
        }

        if !batch.record_value_relationships.is_empty() {
            let writer = self.get_writer("record_value_relationships")?;
            let count = batch.record_value_relationships.len();
            for row in batch.record_value_relationships { writer.write_record(&[row.record_value_id, row.record_id, row.value_id, row.relationship_type, row.ordinal.to_string(), row.process_id, row.timestamp])?; }
            self.increment_row_count("record_value_relationships", count);
        }

        if !batch.value_value_relationships.is_empty() {
            let mut new_rels_to_write = Vec::new();
            for row in batch.value_value_relationships {
                 let key: ValueValueRelKey = (
                    row.source_value_id.clone(),
                    row.target_value_id.clone(),
                    row.relationship_type.clone(),
                    row.ordinal
                 );
                 if self.written_value_value_rels.insert(key) {
                    new_rels_to_write.push(row);
                 }
            }

            if !new_rels_to_write.is_empty() {
                let writer = self.get_writer("value_value_relationships")?;
                let count = new_rels_to_write.len();
                for row in new_rels_to_write {
                    writer.write_record(&[
                        row.value_value_id,
                        row.source_value_id,
                        row.target_value_id,
                        row.relationship_type,
                        row.ordinal.map_or("".to_string(), |o| o.to_string()),
                        row.process_id,
                        row.confidence_score.to_string(),
                        row.timestamp
                    ])?;
                }
                self.increment_row_count("value_value_relationships", count);
            }
        }

        if !batch.values.is_empty() {
            let mut new_values_to_write = Vec::new();
            for row in batch.values {
                if self.written_value_ids.insert(row.value_id.clone()) {
                    new_values_to_write.push(row);
                }
            }

            if !new_values_to_write.is_empty() {
                let writer = self.get_writer("values")?;
                for row in &new_values_to_write {
                    writer.write_record(&[&row.value_id, &row.value_type, &row.value_content])?;
                }
                self.increment_row_count("values", new_values_to_write.len());
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        info!("Flushing {} data CSV files in directory {}...", self.data_writers.len(), self.output_dir.display());
        let mut flush_errors = Vec::new();
        for (name, writer) in self.data_writers.iter_mut() {
            if let Err(e) = writer.flush() {
                flush_errors.push(format!("Failed to flush file {}.csv: {}", name, e));
            }
        }
        if !flush_errors.is_empty() {
            Err(anyhow::anyhow!("Errors occurred during final flush:\n - {}", flush_errors.join("\n - ")))
        } else {
            info!("All data CSV writers flushed successfully.");
            Ok(())
        }
    }

    fn report_files_created(&self) -> usize { self.files_created }

    fn report_rows_written(&self) -> HashMap<String, usize> {
        self.rows_written.iter()
            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
            .collect()
    }

    fn finalize(&mut self) -> Result<()> {
        info!("Finalizing output: ensuring all defined null value entries exist...");
        let values_file_path = self.output_dir.join("values.csv");
        let file = OpenOptions::new().append(true).create(true).open(&values_file_path)?;
        let mut writer = Writer::from_writer(file);
        let mut nulls_added = 0;

        for (null_key, value_id) in self.null_value_ids.iter() {
             if self.written_value_ids.insert(value_id.clone()) {
                 let null_config = self.all_profiles_in_run.iter()
                     .find_map(|p| p.null_values.get(null_key));

                 if let Some(config) = null_config {
                     writer.write_record(&[value_id, &config.value_type, &config.content])?;
                     self.increment_row_count("values", 1);
                     nulls_added += 1;
                 } else {
                      warn!("Could not find configuration details for precomputed null key '{}' during finalization.", null_key);
                 }
             }
        }

        writer.flush()?;
        info!("Null value entry check complete. Added {} null values.", nulls_added);
        Ok(())
    }
}


struct CsvWriterManager {
    writer_impl: Box<dyn OutputWriter>,
}

impl CsvWriterManager {
    fn new(
        output_dir: PathBuf,
        written_value_ids: WrittenValueIdSet,
        all_profiles_in_run: Vec<Arc<Profile>>,
        null_value_ids: NullValueIdMap,
        create_metadata_files: bool,
    ) -> Result<Self> {
        let written_process_value_rels = Arc::new(DashSet::new());
        let written_value_value_rels: Arc<DashSet<ValueValueRelKey>> = Arc::new(DashSet::new());

        let strategy = MultiTableCsvOutput::new(
            output_dir,
            written_value_ids,
            written_process_value_rels,
            written_value_value_rels,
            all_profiles_in_run,
            null_value_ids,
            create_metadata_files
        )?;
        Ok(Self { writer_impl: Box::new(strategy) })
    }
    fn write_batch(&mut self, batch: OutputBatch) -> Result<()> { self.writer_impl.write_batch(batch).context("Error writing batch via CsvWriterManager") }
    fn flush_all(&mut self) -> Result<()> { self.writer_impl.flush().context("Error flushing all files via CsvWriterManager") }
    fn report_files_created(&self) -> usize { self.writer_impl.report_files_created() }
    fn report_rows_written(&self) -> HashMap<String, usize> { self.writer_impl.report_rows_written() }
    fn finalize_output(&mut self) -> Result<()> { self.writer_impl.finalize().context("Error finalizing output via CsvWriterManager") }
}

impl Drop for CsvWriterManager {
    fn drop(&mut self) {
        info!("CsvWriterManager dropping. Attempting final flush...");
        if let Err(e) = self.flush_all() {
            error!("Error flushing CSV writers during cleanup: {}", e);
        }
    }
}

fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    let pattern_str = pattern.to_string_lossy();
    info!("Searching for files matching pattern: {}", pattern_str);
    let paths: Vec<PathBuf> = glob(&pattern_str)?.filter_map(Result::ok).collect();
    if paths.is_empty() {
        warn!("No files found matching the pattern: {}", pattern_str);
    }
    Ok(paths)
}

fn format_elapsed(elapsed: Duration) -> String {
    let total_secs = elapsed.as_secs();
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let millis = elapsed.subsec_millis();
    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}.{:03}s", seconds, millis)
    }
}

fn get_current_timestamp_str() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn precompute_null_value_ids(
    profiles: &[Arc<Profile>]
) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    let mut seen_configs: HashMap<String, NullValueConfig> = HashMap::new();

    for profile in profiles {
        let id_config = &profile.deterministic_ids;
        for (key, config) in &profile.null_values {
            if let Some(existing_config) = seen_configs.get(key) {
                if *config != *existing_config {
                    return Err(anyhow::anyhow!("Inconsistent null value configuration found for key '{}' (type: {}, content: '{}' vs type: {}, content: '{}') across profiles.",
                        key, config.value_type, config.content, existing_config.value_type, existing_config.content));
                }
                continue;
            }
            let id_content = format!("{}:{}", config.value_type, config.content);
            let value_id = generate_deterministic_id(&id_config.value_prefix, &id_content);
            map.insert(key.clone(), value_id);
            seen_configs.insert(key.clone(), config.clone());
        }
    }
    Ok(map)
}

fn resolve_task_filters(
    profile_filters: &Option<Vec<FilterConfig>>,
    task_filters: &HashMap<String, String>
) -> HashMap<String, String> {
    let mut resolved = HashMap::new();
    for (key, value) in task_filters {
        if profile_filters.as_ref().map_or(false, |pf| pf.iter().any(|f| f.cli_arg == *key)) {
             resolved.insert(key.clone(), value.clone());
        } else {
             warn!("Task filter specified for key '{}', but no corresponding filter definition found in the profile. Ignoring this task filter.", key);
        }
    }
    resolved
}


fn main() -> Result<()> {
    let start_time = Instant::now();
    let cli = Cli::parse();

    let log_level = match cli.log_level.to_uppercase().as_str() {
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" | "WARNING" => LevelFilter::Warn,
        "ERROR" => LevelFilter::Error,
        _ => { eprintln!("Invalid log level '{}', defaulting to INFO.", cli.log_level); LevelFilter::Info }
    };
    SimpleLogger::new()
        .with_level(log_level)
        .with_timestamp_format(format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"))
        .init()?;

    info!("Starting Affiliation Extractor - Multi Profile Runner");
    memory_usage::log_memory_usage("initial");

    let output_dir = PathBuf::from(&cli.output);
    fs::create_dir_all(&output_dir).with_context(|| format!("Failed to create output directory: {}", output_dir.display()))?;
    info!("Output directory: {}", output_dir.display());

    let timestamp_str = Arc::new(get_current_timestamp_str());
    info!("Run Timestamp: {}", *timestamp_str);

    let run_config_path = &cli.run_config;
    info!("Loading run configuration from: {}", run_config_path.display());
    let run_config_file = File::open(run_config_path)
        .with_context(|| format!("Failed to open run configuration file: {}", run_config_path.display()))?;
    let run_config: RunConfig = serde_yaml::from_reader(run_config_file)
        .with_context(|| format!("Failed to parse run configuration YAML from {}", run_config_path.display()))?;
    info!("Run config loaded: {} tasks.", run_config.tasks.len());

    let record_id_map: RecordIdMap = Arc::new(DashMap::new());
    let value_id_map: ValueIdMap = Arc::new(DashMap::new());
    let written_value_ids: WrittenValueIdSet = Arc::new(DashSet::new());

    let mut loaded_profiles: HashMap<PathBuf, Arc<Profile>> = HashMap::new();
    let mut files_to_process_with_filters: Vec<(PathBuf, Arc<Profile>, HashMap<String, String>)> = Vec::new();
    let mut all_profiles_in_run_set: HashSet<PathBuf> = HashSet::new();
    let mut all_profiles_in_run_vec: Vec<Arc<Profile>> = Vec::new();

    info!("Scanning tasks and input files...");
    for (i, task) in run_config.tasks.iter().enumerate() {
        info!("Processing Task {} ({})", i + 1, task.description.as_deref().unwrap_or("No description"));
        info!("  Profile: {}", task.profile.display());
        info!("  Input Dir: {}", task.input_dir.display());

        let profile = match loaded_profiles.entry(task.profile.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => Arc::clone(entry.get()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let profile_content = fs::read_to_string(&task.profile)
                   .with_context(|| format!("Task {}: Failed to read profile file: {}", i+1, task.profile.display()))?;
                let parsed_profile: Profile = serde_json::from_str(&profile_content)
                       .with_context(|| format!("Task {}: Failed to parse profile JSON from {}", i+1, task.profile.display()))?;
                let arc_profile = Arc::new(parsed_profile);
                entry.insert(Arc::clone(&arc_profile));
                arc_profile
            }
        };

         if all_profiles_in_run_set.insert(task.profile.clone()) {
            all_profiles_in_run_vec.push(Arc::clone(&profile));
         }

        let resolved_filters = resolve_task_filters(&profile.filters, &task.filters);
         if !resolved_filters.is_empty() {
              info!("  Applying task filters: {:?}", resolved_filters);
         }

        match find_jsonl_gz_files(&task.input_dir) {
            Ok(files) => {
                 info!("  Found {} *.jsonl.gz files for this task.", files.len());
                 for file in files {
                     files_to_process_with_filters.push((file, Arc::clone(&profile), resolved_filters.clone()));
                 }
            },
            Err(e) => {
                 error!("Task {}: Failed to find input files in {}: {}", i+1, task.input_dir.display(), e);
                 return Err(e).context(format!("Error finding files for task {}", i+1));
            }
        }
    }


    if files_to_process_with_filters.is_empty() {
        warn!("No .jsonl.gz files found across all tasks. Exiting.");
        return Ok(());
    }
    info!("Total files to process across all tasks: {}", files_to_process_with_filters.len());

    let null_value_ids = Arc::new(precompute_null_value_ids(&all_profiles_in_run_vec)?);
    info!("Precomputed {} unique null value IDs.", null_value_ids.len());

    let num_threads = if cli.threads == 0 {
        let cores = num_cpus::get();
        info!("Auto-detected {} CPU cores. Using {} threads.", cores, cores);
        cores
    } else {
        info!("Using specified {} threads.", cli.threads);
        cli.threads
    };
    if let Err(e) = rayon::ThreadPoolBuilder::new().num_threads(num_threads).build_global() {
        error!("Failed to build global thread pool: {}. Proceeding with default.", e);
    }

    let progress_bar = ProgressBar::new(files_to_process_with_filters.len() as u64);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta} @ {per_sec}) {msg}")
        .expect("Failed to create progress bar template")
        .progress_chars("=> "));
    progress_bar.set_message("Starting processing...");

    let channel_capacity = (num_threads * 2).max(16);
    let (batch_sender, batch_receiver): (Sender<OutputBatch>, Receiver<OutputBatch>) = bounded(channel_capacity);
    info!("Using writer channel with capacity: {}", channel_capacity);

    let output_dir_clone = output_dir.clone();
    let written_value_ids_clone = Arc::clone(&written_value_ids);
    let all_profiles_clone = all_profiles_in_run_vec.clone();
    let null_ids_clone = Arc::clone(&null_value_ids);
    let create_meta_files = cli.create_metadata_files;

    let writer_thread = thread::spawn(move || -> Result<CsvWriterManager> {
        info!("Writer thread started.");
        let mut csv_writer_manager = CsvWriterManager::new(
            output_dir_clone,
            written_value_ids_clone,
            all_profiles_clone,
            null_ids_clone,
            create_meta_files,
        )?;
        let mut total_batches_processed = 0;
        for batch in batch_receiver {
             if !batch.is_empty() {
                 let num_rows = batch.count_rows();
                 if let Err(e) = csv_writer_manager.write_batch(batch) {
                     error!("Writer thread error writing batch: {}", e);
                 } else {
                     total_batches_processed += 1;
                     debug!("Writer thread processed batch {}, {} rows", total_batches_processed, num_rows);
                 }
             }
        }
        info!("Writer thread finished receiving. Processed {} batches.", total_batches_processed);
        if let Err(e) = csv_writer_manager.flush_all() { error!("Writer thread error during final flush: {}", e); }
        if let Err(e) = csv_writer_manager.finalize_output() { error!("Writer thread error during finalize (adding null rows): {}", e); }

        Ok(csv_writer_manager)
    });


    info!("Starting parallel file processing...");

     let processing_results: Vec<Result<(), (PathBuf, anyhow::Error)>> = files_to_process_with_filters.par_iter()
         .map(|(filepath, profile, task_filters_resolved)| {
             let record_id_map_clone = Arc::clone(&record_id_map);
             let value_id_map_clone = Arc::clone(&value_id_map);
             let null_ids_local_clone = Arc::clone(&null_value_ids);
             let timestamp_clone = Arc::clone(&timestamp_str);
             let sender_clone = batch_sender.clone();
             let pb_clone = progress_bar.clone();
             let process_start_time = Instant::now();

             let processor = JsonlProcessor::new(
                 Arc::clone(profile),
                 null_ids_local_clone,
                 record_id_map_clone,
                 value_id_map_clone,
                 timestamp_clone,
                 task_filters_resolved.clone(),
             );

             match processor.process(filepath) {
                 Ok(output_batch) => {
                     let duration = process_start_time.elapsed();
                     let file_name_msg = filepath.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| filepath.display().to_string());
                     let rows_in_batch = output_batch.count_rows();
                     pb_clone.set_message(format!("OK: {} ({} rows, {})", file_name_msg, rows_in_batch, format_elapsed(duration)));

                     if !output_batch.is_empty() {
                         if let Err(e) = sender_clone.send(output_batch) {
                             error!("Failed to send batch from {} to writer thread: {}. Writer likely panicked.", filepath.display(), e);
                              return Err((filepath.to_path_buf(), anyhow::anyhow!("Writer channel closed unexpectedly")));
                         }
                     }
                     pb_clone.inc(1);
                     Ok(())
                 },
                 Err((path, e)) => {
                     let file_name_msg = path.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| path.display().to_string());
                     error!("Error processing file {}: {}", path.display(), e);
                     pb_clone.set_message(format!("ERR: {}", file_name_msg));
                     pb_clone.inc(1);
                     Err((path, e))
                 }
             }
         }).collect();


    info!("File processing complete. Aggregating results...");
    progress_bar.set_message("Aggregating results...");
    drop(batch_sender);

    let mut files_with_errors = Vec::new();
    let mut successful_files_count = 0;
    for result in processing_results {
         match result {
             Ok(_) => successful_files_count += 1,
             Err((path, _e)) => {
                 files_with_errors.push(path);
             }
         }
     }
    progress_bar.finish_with_message(format!("Processing finished. {} files OK, {} errors.", successful_files_count, files_with_errors.len()));

    info!("Waiting for writer thread to finish writing, flushing, and finalizing...");
    let writer_manager_result = writer_thread.join();

    let final_row_counts = match writer_manager_result {
         Ok(Ok(manager)) => {
             info!("Writer thread finished successfully.");
             Some(manager.report_rows_written())
         },
         Ok(Err(e)) => {
             error!("Writer thread returned an error: {}", e);
             None
         },
         Err(e) => {
             error!("Writer thread panicked: {:?}", e);
             None
         }
     };

    info!("-------------------- FINAL SUMMARY --------------------");
    let total_runtime = start_time.elapsed();
    info!("Total execution time: {}", format_elapsed(total_runtime));
    info!("Total input files found: {}", files_to_process_with_filters.len());
    info!("Files processed successfully: {}", successful_files_count);
    if !files_with_errors.is_empty() {
        warn!("Files with processing errors: {}", files_with_errors.len());
        for err_file in files_with_errors.iter().take(10) {
            warn!("  - {}", err_file.display());
        }
        if files_with_errors.len() > 10 {
            warn!("  ... (and {} more)", files_with_errors.len() - 10);
        }
    }

    info!("Unique Primary IDs processed (Records): {}", record_id_map.len());
    info!("Unique Values generated (Authors, Affs, RORs, etc.): {}", value_id_map.len());

    if let Some(counts) = final_row_counts {
         info!("Total rows written per table (includes added null value rows):");
         let mut sorted_counts: Vec<_> = counts.into_iter().collect();
         sorted_counts.sort_by_key(|(name, _)| name.clone());
         for (table_name, count) in sorted_counts {
             info!("  - {}.csv: {}", table_name, count);
         }
     } else {
         error!("Could not retrieve final row counts from writer thread.");
     }

    memory_usage::log_memory_usage("final");
    info!("Extraction process finished.");
    info!("-------------------------------------------------------");

    if !files_with_errors.is_empty() {
         std::process::exit(1);
     }

    Ok(())
}