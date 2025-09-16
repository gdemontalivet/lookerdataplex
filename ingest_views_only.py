#!/usr/bin/env python3
"""
Ingest only views from the retail banking model into Dataplex
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

def generate_custom_fqn(entity_type, *args):
    """Generate custom FQN for entities without predefined formats."""
    if entity_type == "view":
        instance_id, model_name, view_name = args
        return f"custom:looker.view:{instance_id}.{model_name}.{view_name}"
    return ""

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

def main():
    """Main function to ingest only views into Dataplex."""
    print("📊 INGESTING VIEWS ONLY INTO DATAPLEX")
    print("=" * 60)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print(f"📁 Entry Group: {ENTRY_GROUP}")
    print(f"🔍 Looker Instance: {LOOKER_INSTANCE_ID}")
    print("=" * 60)

    view_fqns = {}
    success_count = 0
    total_count = 0

    # Ingest views with new structure
    print("\n📊 INGESTING VIEWS")
    print("-" * 40)
    
    for view_dir in ["banking_and_card_views", "geo_views"]:
        if not os.path.exists(view_dir):
            continue
            
        for filename in os.listdir(view_dir):
            if filename.endswith(".view.lkml"):
                total_count += 1
                view_name = filename.replace(".view.lkml", "")
                view_metadata = parse_view_file(os.path.join(view_dir, filename))
                
                # Generate proper FQN for view
                view_fqn = generate_custom_fqn("view", LOOKER_INSTANCE_ID, "retail_banking", view_name)
                
                # Create fields array for looker.viewSchema aspect
                fields = []
                
                # Add dimensions
                for dim in view_metadata.dimensions:
                    field_data = f"{dim.field_name}:dimension:{dim.field_type}"
                    fields.append(field_data)
                
                # Add measures
                for measure in view_metadata.measures:
                    field_data = f"{measure.field_name}:measure:{measure.field_type}"
                    fields.append(field_data)
                
                # Create aspect data using new structure
                aspect_data = {
                    "looker-core": {
                        "id": view_name,
                        "title": view_metadata.description or view_name,
                        "url": f"https://{LOOKER_INSTANCE_ID}.looker.com/projects/retail_banking/files/{view_name}.view.lkml",
                        "folderId": "retail_banking",
                        "owner": "system",
                        "tags": ["view", "lookml"]
                    }
                }
                
                # Add view schema if we have fields
                if fields:
                    aspect_data["looker-view-schema"] = {
                        "model": "retail_banking",
                        "view": view_name,
                        "sql_table_name": view_metadata.sql_table_name or "",
                        "derived_table_sql": "",
                        "fields": fields
                    }

                if create_dataplex_entry_with_aspects(view_fqn, "looker-view", aspect_data):
                    view_fqns[view_name] = view_fqn
                    success_count += 1
                    print(f"  ✅ Created view {view_name} ({len(view_metadata.dimensions)} dims, {len(view_metadata.measures)} measures)")

    # Print summary
    print(f"\n" + "=" * 60)
    print(f"🎉 VIEW INGESTION COMPLETE!")
    print(f"=" * 60)
    print(f"📊 Views processed: {success_count}/{total_count}")
    print(f"")
    print(f"🎯 NEXT STEPS:")
    print(f"1. 🌐 Open Dataplex console: https://console.cloud.google.com/dataplex/catalog")
    print(f"2. 📁 Navigate to entry group: '{ENTRY_GROUP}'")
    print(f"3. 🔍 Click on any view to see structured metadata")
    print(f"4. 🔗 Run ingest_explores_only.py next")

if __name__ == "__main__":
    main()
