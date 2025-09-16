#!/usr/bin/env python3
"""
Create all required entry types in Dataplex
"""

import subprocess

# Configuration
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
            print(f"⚠️  {description} (already exists or not found)")
            return None
        else:
            print(f"❌ Error: {description}")
            print(f"Command: {' '.join(command_args)}")
            print(f"Error: {e.stderr}")
            return None

def main():
    """Main function to create all entry types"""
    print("📦 CREATING ENTRY TYPES")
    print("-" * 40)
    
    # 1. Create looker-view entry type
    command_args = [
        "gcloud", "dataplex", "entry-types", "create", "looker-view",
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        "--description=LookML view mapping to warehouse table or derived SQL",
        f"--required-aspects=type='projects/{PROJECT_ID}/locations/{LOCATION}/aspectTypes/looker-core'"
    ]
    
    run_gcloud_command(command_args, "Creating entry type: looker-view", ignore_errors=True)
    
    # 2. Create looker-explore entry type
    command_args = [
        "gcloud", "dataplex", "entry-types", "create", "looker-explore",
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        "--description=Looker explore semantic query surface",
        f"--required-aspects=type='projects/{PROJECT_ID}/locations/{LOCATION}/aspectTypes/looker-core'"
    ]
    
    run_gcloud_command(command_args, "Creating entry type: looker-explore", ignore_errors=True)
    
    # 3. Create looker-dashboard entry type
    command_args = [
        "gcloud", "dataplex", "entry-types", "create", "looker-dashboard",
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        "--description=Looker dashboard with tiles and explore dependencies",
        f"--required-aspects=type='projects/{PROJECT_ID}/locations/{LOCATION}/aspectTypes/looker-core'"
    ]
    
    run_gcloud_command(command_args, "Creating entry type: looker-dashboard", ignore_errors=True)
    
    # 4. Create looker-look entry type
    command_args = [
        "gcloud", "dataplex", "entry-types", "create", "looker-look",
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        "--description=Looker saved look with query definition",
        f"--required-aspects=type='projects/{PROJECT_ID}/locations/{LOCATION}/aspectTypes/looker-core'"
    ]
    
    run_gcloud_command(command_args, "Creating entry type: looker-look", ignore_errors=True)

if __name__ == "__main__":
    main()
