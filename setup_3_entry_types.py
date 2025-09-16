#!/usr/bin/env python3
"""
Step 3: Create Dataplex entry types for Looker metadata
"""

import subprocess
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

def create_entry_type(entry_type_id, description, required_aspects=None):
    """Creates a Dataplex entry type with optional required aspects."""
    
    command_args = [
        "gcloud", "dataplex", "entry-types", "create", entry_type_id,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--description={description}"
    ]
    
    if required_aspects:
        required_aspects_formatted = []
        for aspect in required_aspects:
            required_aspects_formatted.append(f"projects/{PROJECT_ID}/locations/{LOCATION}/aspectTypes/{aspect}")
        
        command_args.extend([
            f"--required-aspects={','.join(required_aspects_formatted)}"
        ])
    
    return run_gcloud_command(command_args, f"Creating entry type: {entry_type_id}", ignore_errors=True)

def create_dashboard_entry_type():
    """Create looker-dashboard entry type."""
    return create_entry_type(
        "looker-dashboard",
        "Looker dashboard with tiles and explore dependencies",
        required_aspects=["looker.core"]
    )

def create_look_entry_type():
    """Create looker-look entry type."""
    return create_entry_type(
        "looker-look",
        "Looker saved look with query definition",
        required_aspects=["looker.core"]
    )

def create_explore_entry_type():
    """Create looker-explore entry type."""
    return create_entry_type(
        "looker-explore",
        "Looker explore semantic query surface",
        required_aspects=["looker.core", "looker.exploreGraph"]
    )

def create_view_entry_type():
    """Create looker-view entry type."""
    return create_entry_type(
        "looker-view",
        "LookML view mapping to warehouse table or derived SQL",
        required_aspects=["looker.core", "looker.viewSchema"]
    )

def main():
    """Main function to create all entry types."""
    print("🚀 STEP 3: Creating Dataplex Entry Types")
    print("=" * 50)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print("=" * 50)

    print("\n📦 Creating entry types...")
    
    success_count = 0
    
    if create_dashboard_entry_type():
        success_count += 1
    
    if create_look_entry_type():
        success_count += 1
    
    if create_explore_entry_type():
        success_count += 1
    
    if create_view_entry_type():
        success_count += 1

    print(f"\n✅ ENTRY TYPES SETUP COMPLETE!")
    print(f"📊 Created {success_count}/4 entry types")
    print("\n🎯 Setup is now complete! You can now run:")
    print("   • ingest_views_only.py")
    print("   • ingest_explores_only.py") 
    print("   • ingest_dashboards_only.py")
    print("   • create_structural_links.py")

if __name__ == "__main__":
    main()
