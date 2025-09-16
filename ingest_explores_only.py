#!/usr/bin/env python3
"""
Ingest only explores from the retail banking model into Dataplex
Split from the main ingest_looker_metadata.py script for selective ingestion
"""

import os
import json
import subprocess
import re
import tempfile
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

# Dataplex configuration
from config import PROJECT_ID, LOCATION, ENTRY_GROUP, LOOKER_INSTANCE_ID

@dataclass
class FieldLineage:
    """Represents field-level lineage information"""
    field_name: str
    field_type: str
    source_table: Optional[str] = None
    source_column: Optional[str] = None
    sql_definition: Optional[str] = None

@dataclass
class ViewMetadata:
    """Enhanced view metadata with field-level details"""
    name: str
    sql_table_name: Optional[str]
    dimensions: List[FieldLineage]
    measures: List[FieldLineage]
    description: Optional[str] = None

@dataclass
class ExploreMetadata:
    """Enhanced explore metadata"""
    name: str
    base_view: str
    joins: List[Dict[str, str]]
    fields: List[str]
    description: Optional[str] = None

def run_gcloud_command(command_args):
    """Runs a gcloud command and returns the output."""
    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {' '.join(command_args)}")
        print(f"Stderr: {e.stderr}")
        return None

def entry_exists(entry_id):
    """Check if a Dataplex entry already exists."""
    command_args = [
        "gcloud", "dataplex", "entries", "describe", entry_id,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--entry-group={ENTRY_GROUP}",
        "--format=json"
    ]
    
    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            check=True
        )
        return True
    except subprocess.CalledProcessError:
        return False

def create_dataplex_entry_with_aspects(entry_fqn, entry_type, aspect_data):
    """Creates a Dataplex entry with proper FQN and aspect types."""
    
    # Extract entry ID from FQN for gcloud command
    if ":" in entry_fqn:
        entry_id = entry_fqn.split(":")[-1].replace(".", "-")
    else:
        entry_id = entry_fqn.replace(".", "-")
    
    # Check if entry already exists
    if entry_exists(entry_id):
        print(f"  ⚠️  Entry {entry_id} already exists, skipping...")
        return True
    
    print(f"  ➕ Creating entry: {entry_id} (FQN: {entry_fqn})")
    
    formatted_aspects = {}
    
    for aspect_type_id, aspect_payload in aspect_data.items():
        aspect_key = f"{PROJECT_ID}.{LOCATION}.{aspect_type_id}"
        formatted_aspects[aspect_key] = {
            "data": aspect_payload
        }

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".json") as tmp:
        json.dump(formatted_aspects, tmp, indent=2)
        tmp_file_name = tmp.name

    command_args = [
        "gcloud", "dataplex", "entries", "create", entry_id,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--entry-group={ENTRY_GROUP}",
        f"--entry-type={entry_type}",
        f"--entry-type-project={PROJECT_ID}",
        f"--entry-type-location={LOCATION}",
        f"--aspects={tmp_file_name}",
        f"--fully-qualified-name={entry_fqn}"
    ]
    
    try:
        result = run_gcloud_command(command_args)
        success = result is not None
        if success:
            print(f"  ✅ Created {entry_id}")
        return success
    except Exception as e:
        print(f"  ❌ Error creating entry {entry_id}: {e}")
        return False
    finally:
        os.remove(tmp_file_name)

def generate_looker_fqn(entity_type, folder_id, entity_id):
    """Generate Looker FQN following Dataplex predefined patterns."""
    if entity_type == "dashboard":
        return f"looker:dashboard:{LOOKER_INSTANCE_ID}.{folder_id}.{entity_id}"
    elif entity_type == "explore":
        return f"looker:explore:{LOOKER_INSTANCE_ID}.{folder_id}.{entity_id}"
    elif entity_type == "look":
        return f"looker:look:{LOOKER_INSTANCE_ID}.{folder_id}.{entity_id}"
    else:
        return f"custom:looker.{entity_type}:{LOOKER_INSTANCE_ID}.{entity_id}"

def generate_custom_fqn(entity_type, *args):
    """Generate custom FQN for entities without predefined formats."""
    if entity_type == "tile":
        instance_id, dashboard_id, element_id = args
        return f"custom:looker.tile:{instance_id}.{dashboard_id}.{element_id}"
    elif entity_type == "view":
        instance_id, model_name, view_name = args
        return f"custom:looker.view:{instance_id}.{model_name}.{view_name}"
    elif entity_type == "field":
        instance_id, view_name, field_name = args
        return f"custom:looker.field:{instance_id}.{view_name}.{field_name}"
    return ""

def generate_bigquery_fqn(project, dataset, table):
    """Generate BigQuery FQN following Dataplex patterns."""
    return f"bigquery:{project}.{dataset}.{table}"

def parse_explores(file_path: str) -> Dict[str, ExploreMetadata]:
    """Enhanced parsing of explores from a LookML file with better nested structure handling."""
    with open(file_path, 'r') as f:
        content = f.read()

    explores = {}
    
    explore_sections = re.split(r'\nexplore:\s*(\w+)', content)
    
    for i in range(1, len(explore_sections), 2):
        if i + 1 < len(explore_sections):
            explore_name = explore_sections[i]
            explore_content = explore_sections[i + 1]
            
            joins = []
            join_pattern = r'join:\s*(\w+)\s*\{([^}]*(?:\{[^}]*\}[^}]*)*)\}'
            join_matches = re.findall(join_pattern, explore_content, re.DOTALL)
            
            for join_name, join_content in join_matches:
                relationship_match = re.search(r'relationship:\s*(\w+)', join_content)
                sql_on_match = re.search(r'sql_on:\s*([^;]+);', join_content, re.DOTALL)
                
                join_info = {
                    "name": join_name,
                    "relationship": relationship_match.group(1) if relationship_match else "many_to_one",
                    "sql_on": sql_on_match.group(1).strip() if sql_on_match else ""
                }
                joins.append(join_info)
            
            fields = []
            fields_match = re.search(r'fields:\s*\[([^\]]+)\]', explore_content)
            if fields_match:
                fields_content = fields_match.group(1)
                field_items = re.findall(r'[^,\s]+(?:\.[^,\s]+)*', fields_content)
                fields = [f.strip().strip('"\'') for f in field_items]
            
            description_match = re.search(r'description:\s*"([^"]+)"', explore_content)
            label_match = re.search(r'label:\s*"([^"]+)"', explore_content)
            description = description_match.group(1) if description_match else (label_match.group(1) if label_match else None)
            
            explores[explore_name] = ExploreMetadata(
                name=explore_name,
                base_view=explore_name,
                joins=joins,
                fields=fields,
                description=description
            )
    
    return explores

def parse_field_definition(field_block: str, field_name: str, field_type: str) -> FieldLineage:
    """Parse a field definition block to extract lineage information."""
    sql_match = re.search(r'sql:\s*([^;]+);', field_block, re.DOTALL)
    sql_definition = sql_match.group(1).strip() if sql_match else None
    
    type_match = re.search(r'type:\s*(\w+)', field_block)
    detected_type = type_match.group(1) if type_match else field_type
    
    source_column = None
    if sql_definition:
        table_column_match = re.search(r'\$\{TABLE\}\.(\w+)', sql_definition)
        if table_column_match:
            source_column = table_column_match.group(1)
    
    return FieldLineage(
        field_name=field_name,
        field_type=detected_type,
        source_column=source_column,
        sql_definition=sql_definition
    )

def parse_view_file(file_path: str) -> ViewMetadata:
    """Enhanced parsing of LookML view file with detailed field information."""
    with open(file_path, 'r') as f:
        content = f.read()

    view_name_match = re.search(r'view:\s*(\w+)', content)
    view_name = view_name_match.group(1) if view_name_match else Path(file_path).stem
    
    sql_table_name_match = re.search(r'sql_table_name:\s*`([^`]+)`', content)
    sql_table_name = sql_table_name_match.group(1) if sql_table_name_match else None
    
    description_match = re.search(r'description:\s*"([^"]+)"', content)
    description = description_match.group(1) if description_match else None
    
    dimensions = []
    dimension_blocks = re.findall(r'dimension:\s*(\w+)\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\}', content, re.DOTALL)
    for dim_name, dim_block in dimension_blocks:
        field_lineage = parse_field_definition(dim_block, dim_name, "dimension")
        field_lineage.source_table = sql_table_name
        dimensions.append(field_lineage)
    
    measures = []
    measure_blocks = re.findall(r'measure:\s*(\w+)\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\}', content, re.DOTALL)
    for measure_name, measure_block in measure_blocks:
        field_lineage = parse_field_definition(measure_block, measure_name, "measure")
        field_lineage.source_table = sql_table_name
        measures.append(field_lineage)
    
    return ViewMetadata(
        name=view_name,
        sql_table_name=sql_table_name,
        dimensions=dimensions,
        measures=measures,
        description=description
    )

def load_view_metadata_cache():
    """Load view metadata from existing view files for explore dependencies."""
    view_metadata_cache = {}
    
    for view_dir in ["banking_and_card_views", "geo_views"]:
        if not os.path.exists(view_dir):
            continue
            
        for filename in os.listdir(view_dir):
            if filename.endswith(".view.lkml"):
                view_name = filename.replace(".view.lkml", "")
                view_metadata = parse_view_file(os.path.join(view_dir, filename))
                view_metadata_cache[view_name] = view_metadata
    
    return view_metadata_cache

def main():
    """Main function to ingest only explores into Dataplex."""
    print("🔍 INGESTING EXPLORES ONLY INTO DATAPLEX")
    print("=" * 60)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print(f"📁 Entry Group: {ENTRY_GROUP}")
    print(f"🔍 Looker Instance: {LOOKER_INSTANCE_ID}")
    print("=" * 60)

    # Load view metadata cache for BigQuery dependencies
    print("📊 Loading view metadata cache...")
    view_metadata_cache = load_view_metadata_cache()
    print(f"  ✅ Loaded {len(view_metadata_cache)} view metadata entries")

    explore_fqns = {}
    
    # Ingest explores with new structure
    print(f"\n🔍 INGESTING EXPLORES")
    print("-" * 40)
    
    # Check both model files for explores
    explore_files = []
    if os.path.exists('models/retail_banking_explores.lkml'):
        explore_files.append('models/retail_banking_explores.lkml')
    if os.path.exists('models/retail_banking.model.lkml'):
        explore_files.append('models/retail_banking.model.lkml')
    
    all_explores = {}
    for explore_file in explore_files:
        explores = parse_explores(explore_file)
        all_explores.update(explores)
        print(f"  📋 Found {len(explores)} explores in {explore_file}")
    
    print(f"  📋 Total explores to process: {len(all_explores)}")
    
    success_count = 0
    
    for explore_name, explore_metadata in all_explores.items():
        # Generate proper FQN for explore
        explore_fqn = generate_looker_fqn("explore", "retail_banking", explore_name)
        
        # Create views list and joins array
        views = [explore_metadata.base_view]
        joins = []
        
        for join in explore_metadata.joins:
            views.append(join["name"])
            joins.append(f"{join['name']}:{join['relationship']}")
        
        # Create aspect data using new structure
        aspect_data = {
            "looker-core": {
                "id": explore_name,
                "title": explore_metadata.description or explore_name,
                "url": f"https://{LOOKER_INSTANCE_ID}.looker.com/explore/retail_banking/{explore_name}",
                "folderId": "retail_banking",
                "owner": "system",
                "tags": ["explore", "semantic_layer"]
            },
            "looker-explore-graph": {
                "model": "retail_banking",
                "explore": explore_name,
                "views": views,
                "joins": joins
            }
        }
        
        # Add BigQuery dependencies from all views
        bq_tables = []
        for view_name in views:
            if view_name in view_metadata_cache:
                view_meta = view_metadata_cache[view_name]
                if view_meta.sql_table_name:
                    parts = view_meta.sql_table_name.split('.')
                    if len(parts) == 3:
                        bq_table_fqn = generate_bigquery_fqn(parts[0], parts[1], parts[2])
                        bq_tables.append(bq_table_fqn)
        
        if bq_tables:
            aspect_data["bq-dependencies"] = {
                "tables": bq_tables,
                "columns": []
            }

        if create_dataplex_entry_with_aspects(explore_fqn, "looker-explore", aspect_data):
            explore_fqns[explore_name] = explore_fqn
            success_count += 1
            print(f"  ✅ Created explore {explore_name} ({len(joins)} joins, {len(views)} views)")

    # Print summary
    print(f"\n" + "=" * 60)
    print(f"🎉 EXPLORE INGESTION COMPLETE!")
    print(f"=" * 60)
    print(f"🔍 Explores processed: {success_count}/{len(all_explores)}")
    print(f"")
    print(f"🎯 NEXT STEPS:")
    print(f"1. 🌐 Open Dataplex console: https://console.cloud.google.com/dataplex/catalog")
    print(f"2. 📁 Navigate to entry group: '{ENTRY_GROUP}'")
    print(f"3. 🔍 Click on any explore to see structured metadata")
    print(f"4. 🔗 Run create_structural_links.py to create relationships")

if __name__ == "__main__":
    main()
