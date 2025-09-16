#!/usr/bin/env python3
"""
Ingest only dashboards from the retail banking model into Dataplex
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
from config import PROJECT_ID, LOCATION, ENTRY_GROUP
LOOKER_INSTANCE_ID = "mylooker"

@dataclass
class DashboardElement:
    """Dashboard element with detailed field usage"""
    name: str
    type: str
    model: str
    explore: str
    fields: List[str]
    filters: Dict[str, str]
    visualization_config: Dict

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

def parse_dashboard_file(file_path: str) -> Dict[str, any]:
    """Enhanced parsing of LookML dashboard file with detailed element information."""
    with open(file_path, 'r') as f:
        content = f.read()

    dashboard_name_match = re.search(r'dashboard:\s*(\w+)', content)
    dashboard_name = dashboard_name_match.group(1) if dashboard_name_match else Path(file_path).stem
    
    title_match = re.search(r'title:\s*([^\n]+)', content)
    title = title_match.group(1).strip() if title_match else dashboard_name
    
    elements = []
    element_pattern = r'- title:\s*([^\n]+)\s+name:\s*([^\n]+)\s+model:\s*([^\n]+)\s+explore:\s*([^\n]+)\s+type:\s*([^\n]+)\s+fields:\s*\[([^\]]+)\]'
    element_matches = re.findall(element_pattern, content, re.MULTILINE)
    
    for match in element_matches:
        element_title, element_name, model, explore, viz_type, fields_str = match
        fields = [f.strip() for f in fields_str.split(',')]
        
        filters = {}
        filter_section_match = re.search(rf'name:\s*{re.escape(element_name.strip())}.*?filters:\s*\{{([^}}]+)\}}', content, re.DOTALL)
        if filter_section_match:
            filter_content = filter_section_match.group(1)
            filter_matches = re.findall(r'(\w+[.\w]*)\s*:\s*([^\n]+)', filter_content)
            filters = {k.strip(): v.strip().strip("'\"") for k, v in filter_matches}
        
        element = DashboardElement(
            name=element_name.strip(),
            type=viz_type.strip(),
            model=model.strip(),
            explore=explore.strip(),
            fields=fields,
            filters=filters,
            visualization_config={}
        )
        elements.append(element)
    
    explores = set()
    for element in elements:
        explores.add(element.explore)
    
    direct_explores = re.findall(r'explore:\s*(\w+)', content)
    explores.update(direct_explores)
    
    return {
        "name": dashboard_name,
        "title": title,
        "elements": elements,
        "explores": list(explores)
    }

def main():
    """Main function to ingest only dashboards into Dataplex."""
    print("📈 INGESTING DASHBOARDS ONLY INTO DATAPLEX")
    print("=" * 60)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print(f"📁 Entry Group: {ENTRY_GROUP}")
    print(f"🔍 Looker Instance: {LOOKER_INSTANCE_ID}")
    print("=" * 60)

    success_count = 0
    total_count = 0

    # Ingest dashboards with new structure
    print(f"\n📈 INGESTING DASHBOARDS")
    print("-" * 40)
    
    if os.path.exists("dashboards"):
        for filename in os.listdir("dashboards"):
            if filename.endswith(".dashboard.lookml"):
                total_count += 1
                dashboard_name = filename.replace(".dashboard.lookml", "")
                dashboard_metadata = parse_dashboard_file(os.path.join("dashboards", filename))
                
                # Generate proper FQN for dashboard
                dashboard_fqn = generate_looker_fqn("dashboard", "retail_banking", dashboard_name)
                
                # Create aspect data using new structure
                aspect_data = {
                    "looker-core": {
                        "id": dashboard_name,
                        "title": dashboard_metadata["title"],
                        "url": f"https://{LOOKER_INSTANCE_ID}.looker.com/dashboards/{dashboard_name}",
                        "folderId": "retail_banking",
                        "owner": "system",
                        "tags": ["dashboard", "analytics"]
                    }
                }
                
                # Add dashboard structure information
                if dashboard_metadata["elements"]:
                    aspect_data["looker-dashboard-structure"] = {
                        "elements": len(dashboard_metadata["elements"]),
                        "explores_used": dashboard_metadata["explores"],
                        "element_types": list(set([elem.type for elem in dashboard_metadata["elements"]]))
                    }

                if create_dataplex_entry_with_aspects(dashboard_fqn, "looker-dashboard", aspect_data):
                    success_count += 1
                    print(f"  ✅ Created dashboard {dashboard_name} ({len(dashboard_metadata['elements'])} elements)")

    # Print summary
    print(f"\n" + "=" * 60)
    print(f"🎉 DASHBOARD INGESTION COMPLETE!")
    print(f"=" * 60)
    print(f"📈 Dashboards processed: {success_count}/{total_count}")
    print(f"")
    print(f"🎯 NEXT STEPS:")
    print(f"1. 🌐 Open Dataplex console: https://console.cloud.google.com/dataplex/catalog")
    print(f"2. 📁 Navigate to entry group: '{ENTRY_GROUP}'")
    print(f"3. 🔍 Click on any dashboard to see structured metadata")
    print(f"4. 🔗 Run create_structural_links.py to create relationships")

if __name__ == "__main__":
    main()
