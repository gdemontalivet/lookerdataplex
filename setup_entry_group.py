#!/usr/bin/env python3
"""
Create the Looker entry group in Dataplex
"""

import subprocess

# Configuration
from config import PROJECT_ID, LOCATION
ENTRY_GROUP = "looker"

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
            print(f"⚠️  {description} (already exists or not found)")
            return None
        else:
            print(f"❌ Error: {description}")
            print(f"Command: {' '.join(command_args)}")
            print(f"Error: {e.stderr}")
            return None

def main():
    """Main function to create the entry group"""
    print("📁 CREATING ENTRY GROUP")
    print("-" * 40)
    
    command_args = [
        "gcloud", "dataplex", "entry-groups", "create", ENTRY_GROUP,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        "--description=Looker metadata entries following Dataplex FQN specifications"
    ]
    
    run_gcloud_command(command_args, f"Creating entry group: {ENTRY_GROUP}", ignore_errors=True)

if __name__ == "__main__":
    main()
