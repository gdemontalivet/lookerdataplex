#!/usr/bin/env python3
"""
Test script to create a single run and debug the issue
"""

from dataplex_lineage_api import DataLineageAPI, PROJECT_ID, LOCATION, create_transformation_run
from datetime import datetime, timezone

def test_single_run():
    """Test creating a single run to debug the issue"""
    print("🔍 TESTING SINGLE RUN CREATION")
    print("=" * 50)
    
    try:
        # Initialize API client
        api = DataLineageAPI(PROJECT_ID, LOCATION)
        print("✅ Data Lineage API client initialized")
        
        # Use one of the existing processes (with actual UUID)
        process_name = "projects/339614015417/locations/eu/processes/1e2a6b50-0a22-4b16-bd04-384ac85302ce"
        
        # Try different approaches
        approaches = [
            ("STARTED", "Test with STARTED state"),
            ("COMPLETED", "Test with COMPLETED state"),
        ]
        
        for state, description in approaches:
            print(f"\n🧪 {description}")
            
            # Create run with different state
            run_id = f"test-run-{int(datetime.now().timestamp())}"
            start_time = datetime.now(timezone.utc)
            end_time = start_time.replace(second=start_time.second + 1) if state == "COMPLETED" else None
            
            run_data = {
                "displayName": f"Test run with {state} state",
                "state": state,
                "startTime": start_time.isoformat(),
                "attributes": {
                    "test": "true",
                    "state_test": state
                }
            }
            
            if end_time:
                run_data["endTime"] = end_time.isoformat()
            
            # Make the request
            process_id = process_name.split('/')[-1]
            url = f"{api.base_url}/processes/{process_id}/runs"
            
            print(f"  📤 Making request to: {url}")
            print(f"  📋 Payload: {run_data}")
            
            result = api._make_request("POST", url, run_data)
            
            if result:
                print(f"  ✅ Success with {state} state!")
                print(f"  📄 Response: {result}")
                return True
            else:
                print(f"  ❌ Failed with {state} state")
        
        return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_single_run()
