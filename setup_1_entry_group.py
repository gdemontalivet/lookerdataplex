#!/usr/bin/env python3
"""
Step 1: Create Dataplex entry group for Looker metadata
"""

import subprocess
from config import PROJECT_ID, LOCATION, ENTRY_GROUP

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

def create_entry_group():
    """Create the custom entry group for Looker metadata."""
    command_args = [
        "gcloud", "dataplex", "entry-groups", "create", ENTRY_GROUP,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        "--description=Looker metadata entries following Dataplex FQN specifications"
    ]
    
    return run_gcloud_command(command_args, f"Creating entry group: {ENTRY_GROUP}", ignore_errors=True)

def main():
    """Main function to create entry group."""
    print("🚀 STEP 1: Creating Dataplex Entry Group")
    print("=" * 50)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print(f"📁 Entry Group: {ENTRY_GROUP}")
    print("=" * 50)

    create_entry_group()

    print("\n✅ ENTRY GROUP SETUP COMPLETE!")
    print("🎯 Next step: Run setup_2_aspect_types.py")

if __name__ == "__main__":
    main()
