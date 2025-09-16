#!/usr/bin/env python3
"""
Batch update script to replace hardcoded project IDs with config imports
"""

import os
import re

# Files to update and their import patterns
files_to_update = [
    'ingest_dashboards_only.py',
    'create_structural_links.py', 
    'dataplex_lineage_api.py',
    'cleanup_dataplex.py',
    'test_explore_creation.py',
    'test_lineage_get.py',
    'setup_entry_group.py',
    'setup_entry_types.py'
]

def update_file(filename):
    """Update a single file to use config imports."""
    if not os.path.exists(filename):
        print(f"⚠️  File {filename} not found, skipping...")
        return False
    
    print(f"🔄 Updating {filename}...")
    
    with open(filename, 'r') as f:
        content = f.read()
    
    # Pattern 1: Replace hardcoded configuration block
    pattern1 = r'# Configuration\nPROJECT_ID = "local-dimension-399810"\nLOCATION = "eu"\nENTRY_GROUP = "looker"\nLOOKER_INSTANCE_ID = "mylooker"'
    replacement1 = '# Configuration\nfrom config import PROJECT_ID, LOCATION, ENTRY_GROUP, LOOKER_INSTANCE_ID'
    
    # Pattern 2: Replace Dataplex configuration block
    pattern2 = r'# Dataplex configuration\nPROJECT_ID = "local-dimension-399810"\nLOCATION = "eu"\nENTRY_GROUP = "looker"'
    replacement2 = '# Dataplex configuration\nfrom config import PROJECT_ID, LOCATION, ENTRY_GROUP'
    
    # Pattern 3: Replace simple configuration block
    pattern3 = r'PROJECT_ID = "local-dimension-399810"\nLOCATION = "eu"'
    replacement3 = 'from config import PROJECT_ID, LOCATION'
    
    # Pattern 4: Replace with LOOKER_INSTANCE_ID
    pattern4 = r'PROJECT_ID = "local-dimension-399810"\nLOCATION = "eu"\nENTRY_GROUP = "looker"\nLOOKER_INSTANCE_ID = "mylooker"'
    replacement4 = 'from config import PROJECT_ID, LOCATION, ENTRY_GROUP, LOOKER_INSTANCE_ID'
    
    # Apply replacements
    original_content = content
    content = re.sub(pattern1, replacement1, content)
    content = re.sub(pattern2, replacement2, content)
    content = re.sub(pattern4, replacement4, content)
    content = re.sub(pattern3, replacement3, content)
    
    # Replace hardcoded project ID in strings
    content = re.sub(r'"local-dimension-399810"', 'PROJECT_ID', content)
    content = re.sub(r"'local-dimension-399810'", 'PROJECT_ID', content)
    
    # Special case for dataplex_lineage_api.py - replace in tuples
    if filename == 'dataplex_lineage_api.py':
        content = re.sub(r'\("local-dimension-399810",', '(PROJECT_ID,', content)
        content = re.sub(r'generate_bigquery_fqn\("local-dimension-399810",', 'generate_bigquery_fqn(PROJECT_ID,', content)
    
    if content != original_content:
        with open(filename, 'w') as f:
            f.write(content)
        print(f"  ✅ Updated {filename}")
        return True
    else:
        print(f"  ⚠️  No changes needed for {filename}")
        return False

def main():
    """Update all files."""
    print("🚀 BATCH UPDATING SCRIPTS TO USE CONFIG MODULE")
    print("=" * 60)
    
    updated_count = 0
    
    for filename in files_to_update:
        if update_file(filename):
            updated_count += 1
    
    print(f"\n✅ BATCH UPDATE COMPLETE!")
    print(f"📊 Updated {updated_count}/{len(files_to_update)} files")
    print("\n🎯 Next steps:")
    print("1. Create your .env file: cp .env.template .env")
    print("2. Edit .env with your actual GCP project ID")
    print("3. Test the configuration: python3 config.py")

if __name__ == "__main__":
    main()
