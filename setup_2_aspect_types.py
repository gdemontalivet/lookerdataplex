#!/usr/bin/env python3
"""
Step 2: Create Dataplex aspect types for Looker metadata
"""

import subprocess
import json
import tempfile
import os
from config import PROJECT_ID, LOCATION

def run_gcloud_command(command_args, description="", ignore_errors=False):
    """Runs a gcloud command and returns the output."""
    if description:
        print(f"🔄 {description}...")
    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            check=True
        )
        if description:
            print(f"✅ {description}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        if ignore_errors:
            print(f"⚠️  {description} (already exists)")
            return None
        else:
            print(f"❌ Error: {description}")
            print(f"Command: {' '.join(command_args)}")
            print(f"Error: {e.stderr}")
            return None

def create_aspect_type(aspect_type_id, description, metadata_template):
    """Creates a Dataplex aspect type with the given metadata template."""
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".json") as tmp:
        json.dump(metadata_template, tmp, indent=2)
        tmp_file_name = tmp.name

    command_args = [
        "gcloud", "dataplex", "aspect-types", "create", aspect_type_id,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--description={description}",
        f"--metadata-template-file-name={tmp_file_name}"
    ]
    
    try:
        result = run_gcloud_command(command_args, f"Creating aspect type: {aspect_type_id}", ignore_errors=True)
        return result is not None
    finally:
        os.remove(tmp_file_name)

def create_looker_core_aspect():
    """Create looker.core aspect type."""
    looker_core_template = {
        "type": "record",
        "name": "LookerCore",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "title", "type": "string"},
            {"name": "url", "type": "string"},
            {"name": "folderId", "type": "string"},
            {"name": "owner", "type": "string"},
            {"name": "tags", "type": {"type": "array", "items": "string"}}
        ]
    }
    
    return create_aspect_type(
        "looker.core",
        "Core metadata for all Looker entries",
        looker_core_template
    )

def create_explore_graph_aspect():
    """Create looker.exploreGraph aspect type."""
    explore_graph_template = {
        "type": "record",
        "name": "LookerExploreGraph",
        "fields": [
            {"name": "model", "type": "string"},
            {"name": "explore", "type": "string"},
            {"name": "views", "type": {"type": "array", "items": "string"}},
            {"name": "joins", "type": {"type": "array", "items": {
                "type": "record",
                "name": "Join",
                "fields": [
                    {"name": "view", "type": "string"},
                    {"name": "sql_on", "type": "string"}
                ]
            }}}
        ]
    }
    
    return create_aspect_type(
        "looker.exploreGraph",
        "Explore graph metadata with views and joins",
        explore_graph_template
    )

def create_view_schema_aspect():
    """Create looker.viewSchema aspect type."""
    view_schema_template = {
        "type": "record",
        "name": "LookerViewSchema",
        "fields": [
            {"name": "model", "type": "string"},
            {"name": "view", "type": "string"},
            {"name": "sql_table_name", "type": "string"},
            {"name": "derived_table_sql", "type": ["null", "string"]},
            {"name": "fields", "type": {"type": "array", "items": {
                "type": "record",
                "name": "Field",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "kind", "type": "string"},
                    {"name": "type", "type": "string"},
                    {"name": "sql", "type": ["null", "string"]},
                    {"name": "depends_on_columns", "type": {"type": "array", "items": "string"}}
                ]
            }}}
        ]
    }
    
    return create_aspect_type(
        "looker.viewSchema",
        "View schema with field definitions and dependencies",
        view_schema_template
    )

def create_bq_dependencies_aspect():
    """Create bqDependencies aspect type."""
    bq_dependencies_template = {
        "type": "record",
        "name": "BqDependencies",
        "fields": [
            {"name": "tables", "type": {"type": "array", "items": "string"}},
            {"name": "columns", "type": {"type": "array", "items": {
                "type": "record",
                "name": "ColumnDependency",
                "fields": [
                    {"name": "table", "type": "string"},
                    {"name": "columns", "type": {"type": "array", "items": "string"}}
                ]
            }}}
        ]
    }
    
    return create_aspect_type(
        "bqDependencies",
        "BigQuery table and column dependencies",
        bq_dependencies_template
    )

def main():
    """Main function to create all aspect types."""
    print("🚀 STEP 2: Creating Dataplex Aspect Types")
    print("=" * 50)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print("=" * 50)

    print("\n📋 Creating aspect types...")
    
    success_count = 0
    
    if create_looker_core_aspect():
        success_count += 1
    
    if create_explore_graph_aspect():
        success_count += 1
    
    if create_view_schema_aspect():
        success_count += 1
    
    if create_bq_dependencies_aspect():
        success_count += 1

    print(f"\n✅ ASPECT TYPES SETUP COMPLETE!")
    print(f"📊 Created {success_count}/4 aspect types")
    print("🎯 Next step: Run setup_3_entry_types.py")

if __name__ == "__main__":
    main()
