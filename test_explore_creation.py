#!/usr/bin/env python3
"""
Test script to create a single explore entry with correct aspect structure
"""

import subprocess
import json
import tempfile
import os

# Configuration
from config import PROJECT_ID, LOCATION, ENTRY_GROUP, LOOKER_INSTANCE_ID

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

def test_explore_creation():
    """Test creating a single explore entry"""
    
    # Test data for explore
    explore_fqn = "looker:explore:mylooker.retail_banking.test_explore"
    entry_id = "mylooker-retail_banking-test_explore"
    
    # Create aspect data with correct structure
    aspect_data = {
        "looker-core": {
            "id": "test_explore",
            "title": "Test Explore",
            "url": f"https://{LOOKER_INSTANCE_ID}.looker.com/explore/retail_banking/test_explore",
            "folderId": "retail_banking",
            "owner": "system",
            "tags": ["explore", "semantic_layer"]
        },
        "looker-explore-graph": {
            "model": "retail_banking",
            "explore": "test_explore",
            "views": ["card_transactions", "card", "client"],
            "joins": ["card:many_to_one", "client:many_to_one"]
        }
    }
    
    # Format aspects for gcloud
    formatted_aspects = {}
    for aspect_type_id, aspect_payload in aspect_data.items():
        aspect_key = f"{PROJECT_ID}.{LOCATION}.{aspect_type_id}"
        formatted_aspects[aspect_key] = {
            "data": aspect_payload
        }

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".json") as tmp:
        json.dump(formatted_aspects, tmp, indent=2)
        tmp_file_name = tmp.name

    print("📋 Aspect data being sent:")
    print(json.dumps(formatted_aspects, indent=2))
    print()

    command_args = [
        "gcloud", "dataplex", "entries", "create", entry_id,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--entry-group={ENTRY_GROUP}",
        f"--entry-type=looker-explore",
        f"--entry-type-project={PROJECT_ID}",
        f"--entry-type-location={LOCATION}",
        f"--aspects={tmp_file_name}",
        f"--fully-qualified-name={explore_fqn}"
    ]
    
    print(f"🔄 Creating test explore entry: {entry_id}")
    print(f"Command: {' '.join(command_args)}")
    
    try:
        result = subprocess.run(
            command_args,
            capture_output=True,
            text=True,
            check=True
        )
        print(f"✅ Successfully created test explore!")
        print("Now cleaning up...")
        
        # Clean up the test entry
        cleanup_args = [
            "gcloud", "dataplex", "entries", "delete", entry_id,
            f"--project={PROJECT_ID}",
            f"--location={LOCATION}",
            f"--entry-group={ENTRY_GROUP}",
            "--quiet"
        ]
        run_gcloud_command(cleanup_args)
        print("🧹 Cleaned up test entry")
    except subprocess.CalledProcessError as e:
        print("❌ Failed to create test explore")
        print(f"Return code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
    
    # Clean up temp file
    os.remove(tmp_file_name)

if __name__ == "__main__":
    test_explore_creation()
