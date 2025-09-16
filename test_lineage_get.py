#!/usr/bin/env python3
"""
Test script to retrieve existing lineage information using GET methods
"""

import subprocess
import requests
from dataplex_lineage_api import DataLineageAPI, PROJECT_ID, LOCATION

def test_get_methods():
    """Test GET methods to retrieve existing lineage information"""
    print("🔍 TESTING LINEAGE GET METHODS")
    print("=" * 60)
    
    try:
        # Initialize API client
        api = DataLineageAPI(PROJECT_ID, LOCATION)
        print("✅ Data Lineage API client initialized")
        
        # Test 1: Get existing processes
        print("\n1️⃣ Testing GET processes...")
        processes_result = get_existing_processes(api)
        
        # Test 2: Search for existing links
        print("\n2️⃣ Testing search links...")
        search_existing_links(api)
        
        # Test 3: Test basic API connectivity
        print("\n3️⃣ Testing basic API connectivity...")
        test_basic_connectivity(api)
        
    except Exception as e:
        print(f"❌ Error: {e}")

def get_existing_processes(api):
    """Get existing lineage processes"""
    url = f"{api.base_url}/processes"
    result = api._make_request("GET", url)
    
    if result is not None:
        if 'processes' in result:
            processes = result['processes']
            print(f"  📊 Found {len(processes)} existing processes")
            for process in processes[:5]:  # Show first 5
                print(f"    • {process.get('displayName', 'Unknown')}")
        else:
            print("  ℹ️  No processes found (empty result)")
        return result
    else:
        print("  ⚠️  Failed to retrieve processes")
        return None

def search_existing_links(api):
    """Search for existing lineage links"""
    # Test with a simple BigQuery FQN
    test_fqn = "bigquery:projects/local-dimension-399810/datasets/retail_banking/tables/card_transactions"
    
    print(f"  🔍 Searching links for: {test_fqn}")
    
    url = f"{api.base_url}:searchLinks"
    search_data = {
        "source": {"fullyQualifiedName": test_fqn}
    }
    
    result = api._make_request("POST", url, search_data)
    
    if result is not None:
        if 'links' in result:
            links = result['links']
            print(f"    📊 Found {len(links)} links")
            for link in links[:3]:  # Show first 3
                target = link.get('target', {}).get('fullyQualifiedName', 'Unknown')
                print(f"      → {target}")
        else:
            print("    ℹ️  No links found (empty result)")
    else:
        print("    ⚠️  Failed to search links")

def test_basic_connectivity(api):
    """Test basic API connectivity"""
    # Try a simple GET request to the base URL
    url = api.base_url
    result = api._make_request("GET", url)
    
    if result is not None:
        print("  ✅ Basic API connectivity working")
        print(f"  📍 API Base URL: {url}")
    else:
        print("  ❌ Basic API connectivity failed")

if __name__ == "__main__":
    test_get_methods()
