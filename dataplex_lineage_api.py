#!/usr/bin/env python3
"""
Google Cloud Dataplex Data Lineage API Implementation
Creates formal lineage relationships using the Data Lineage REST API
"""

import os
import json
import subprocess
import requests
import time
import hashlib
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

# Configuration
from config import PROJECT_ID, LOCATION
ENTRY_GROUP = "looker"

# Data Lineage API Configuration
LINEAGE_API_BASE = "https://datalineage.googleapis.com/v1"
PROCESS_NAME_PREFIX = "looker-transformation"

@dataclass
class LineageLink:
    """Represents a lineage link between source and target"""
    source_fqn: str
    target_fqn: str
    process_name: str
    transformation_type: str
    description: Optional[str] = None

@dataclass
class LineageProcess:
    """Represents a lineage process (transformation)"""
    name: str
    display_name: str
    description: str
    attributes: Dict[str, str]

@dataclass
class LineageRun:
    """Represents a lineage run (execution of a process)"""
    name: str
    display_name: str
    state: str
    start_time: str
    end_time: str
    attributes: Dict[str, str]

class DataLineageAPI:
    """Google Cloud Data Lineage API client"""
    
    def __init__(self, project_id: str, location: str):
        self.project_id = project_id
        self.location = location
        self.base_url = f"{LINEAGE_API_BASE}/projects/{project_id}/locations/{location}"
        self.access_token = self._get_access_token()
        
    def _get_access_token(self) -> str:
        """Get access token using gcloud auth"""
        try:
            result = subprocess.run(
                ["gcloud", "auth", "print-access-token"],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get access token: {e}")
    
    def _make_request(self, method: str, url: str, data: Optional[Dict] = None) -> Dict:
        """Make authenticated request to Data Lineage API"""
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "POST":
                response = requests.post(url, headers=headers, json=data)
            elif method == "PATCH":
                response = requests.patch(url, headers=headers, json=data)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            if response.status_code in [200, 201]:
                return response.json() if response.content else {}
            elif response.status_code == 404:
                return None
            elif response.status_code == 400:
                print(f"⚠️  API Error 400 (Bad Request): {response.text}")
                print("   This might be due to Data Lineage API not being enabled or permissions issues")
                return None
            elif response.status_code == 403:
                print(f"⚠️  API Error 403 (Forbidden): Data Lineage API may not be enabled")
                print("   Enable it at: https://console.cloud.google.com/apis/library/datalineage.googleapis.com")
                return None
            else:
                print(f"API Error {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
    
    def create_process(self, process: LineageProcess) -> Optional[str]:
        """Create a lineage process and return the actual process name from API response"""
        url = f"{self.base_url}/processes"
        
        # Check if process already exists
        process_id = process.name.split('/')[-1]
        existing = self._make_request("GET", f"{url}/{process_id}")
        if existing:
            print(f"  🔄 Process already exists: {process.display_name}")
            return existing.get('name')
        
        # Correct format: Process object directly in request body
        process_data = {
            "displayName": process.display_name,
            "attributes": process.attributes,
            "origin": {
                "sourceType": "CUSTOM",
                "name": "looker-dataplex-integration"
            }
        }
        
        print(f"  📤 Making request to: {url}")
        print(f"  📋 Payload: {process_data}")
        
        result = self._make_request("POST", url, process_data)
        
        if result:
            print(f"  ✅ Created process: {process.display_name}")
            print(f"  📄 Response: {result}")
            return result.get('name')
        else:
            print(f"  ❌ Failed to create process: {process.display_name}")
            return None
    
    def create_run(self, process_name: str, run: LineageRun) -> Optional[str]:
        """Create a lineage run and return the actual run name from API response"""
        process_id = process_name.split('/')[-1]
        url = f"{self.base_url}/processes/{process_id}/runs"
        
        # Check if run already exists
        run_id = run.name.split('/')[-1]
        existing = self._make_request("GET", f"{url}/{run_id}")
        if existing:
            print(f"  🔄 Run already exists: {run.display_name}")
            return existing.get('name')
        
        # Correct format: Run object directly in request body (fixed from test script)
        run_data = {
            "displayName": run.display_name,
            "state": run.state,
            "startTime": run.start_time,
            "attributes": run.attributes
        }
        
        # Only add endTime if the run is COMPLETED
        if run.state == "COMPLETED" and run.end_time:
            run_data["endTime"] = run.end_time
        
        print(f"  📤 Making request to: {url}")
        print(f"  📋 Payload: {run_data}")
        
        result = self._make_request("POST", url, run_data)
        
        if result:
            print(f"  ✅ Created run: {run.display_name}")
            print(f"  📄 Response: {result}")
            return result.get('name')
        else:
            print(f"  ❌ Failed to create run: {run.display_name}")
            return None
    
    def create_lineage_event(self, process_name: str, run_name: str, 
                           source_fqn: str, target_fqn: str, 
                           event_type: str = "COMPLETE") -> bool:
        """Create a lineage event linking source to target"""
        process_id = process_name.split('/')[-1]
        run_id = run_name.split('/')[-1]
        url = f"{self.base_url}/processes/{process_id}/runs/{run_id}/lineageEvents"
        
        event_time = datetime.now(timezone.utc).isoformat()
        
        # Correct format: LineageEvent object directly in request body
        event_data = {
            "links": [{
                "source": {"fullyQualifiedName": source_fqn},
                "target": {"fullyQualifiedName": target_fqn}
            }],
            "startTime": event_time,
            "endTime": event_time
        }
        
        print(f"  📤 Making lineage event request to: {url}")
        print(f"  📋 Event payload: {event_data}")
        
        result = self._make_request("POST", url, event_data)
        
        if result:
            print(f"  📊 Created lineage: {source_fqn.split('/')[-1]} → {target_fqn.split('/')[-1]}")
            print(f"  📄 Response: {result}")
            return True
        else:
            print(f"  ❌ Failed to create lineage: {source_fqn.split('/')[-1]} → {target_fqn.split('/')[-1]}")
            return False
    
    def search_links(self, asset_fqn: str) -> Dict:
        """Search for lineage links connected to an asset"""
        url = f"{self.base_url}:searchLinks"
        
        search_data = {
            "source": {"fullyQualifiedName": asset_fqn}
        }
        
        result = self._make_request("POST", url, search_data)
        return result if result else {}
    
    def get_all_processes(self) -> List[Dict]:
        """Get all existing processes"""
        url = f"{self.base_url}/processes"
        result = self._make_request("GET", url)
        
        if result and 'processes' in result:
            return result['processes']
        return []
    
    def delete_process(self, process_name: str) -> bool:
        """Delete a specific process"""
        url = f"{LINEAGE_API_BASE}/{process_name}"
        result = self._make_request("DELETE", url)
        return result is not None
    
    def cleanup_all_processes(self) -> bool:
        """Clean up all existing lineage processes"""
        print("\n🧹 CLEANING UP ALL EXISTING LINEAGE PROCESSES")
        print("-" * 60)
        
        # Get all processes
        print("📋 Fetching all processes...")
        processes = self.get_all_processes()
        
        if not processes:
            print("✅ No existing processes found - nothing to clean up")
            return True
        
        print(f"   Found {len(processes)} processes to delete")
        
        # Delete all processes
        deleted_count = 0
        failed_count = 0
        
        for process in processes:
            process_name = process.get('name', '')
            process_id = process_name.split('/')[-1] if process_name else 'unknown'
            display_name = process.get('displayName', process_id)
            
            print(f"   🗑️  Deleting: {display_name} ({process_id})")
            
            if self.delete_process(process_name):
                deleted_count += 1
                print(f"      ✅ Deleted successfully")
            else:
                failed_count += 1
                print(f"      ❌ Failed to delete")
        
        # Summary
        print(f"\n📊 Cleanup Summary:")
        print(f"   ✅ Successfully deleted: {deleted_count} processes")
        if failed_count > 0:
            print(f"   ❌ Failed to delete: {failed_count} processes")
        
        success = failed_count == 0
        if success:
            print("✅ All processes cleaned up successfully")
        else:
            print("⚠️  Some processes failed to delete - proceeding anyway")
        
        return success

def generate_dataplex_fqn(entry_type: str, entry_name: str) -> str:
    """Generate Dataplex entry FQN based on existing entries"""
    if entry_type == "view":
        return f"custom:looker.view:mylooker.retail_banking.{entry_name}"
    elif entry_type == "explore":
        return f"looker:explore:mylooker.retail_banking.{entry_name}"
    elif entry_type == "dashboard":
        return f"looker:dashboard:mylooker.retail_banking.{entry_name}"
    else:
        # Fallback to original format
        return f"dataplex:projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{entry_type}-{entry_name}"

def generate_bigquery_fqn(project: str, dataset: str, table: str) -> str:
    """Generate BigQuery asset FQN"""
    return f"bigquery:projects/{project}/datasets/{dataset}/tables/{table}"

def generate_deterministic_process_id(display_name: str, transformation_type: str) -> str:
    """Generate a deterministic process ID based on display name and type"""
    # Create a hash of the display name and transformation type for consistency
    content = f"{display_name}:{transformation_type}"
    hash_object = hashlib.md5(content.encode())
    hash_hex = hash_object.hexdigest()
    
    # Use first 8 characters of hash + transformation type prefix
    type_prefix = {
        "view_transformation": "bq-view",
        "explore_transformation": "view-explore", 
        "dashboard_visualization": "explore-dash"
    }.get(transformation_type, "transform")
    
    return f"{type_prefix}-{hash_hex[:8]}"

def create_transformation_process(process_id: str, display_name: str, 
                                description: str, transformation_type: str) -> LineageProcess:
    """Create a transformation process definition with deterministic ID"""
    # Generate deterministic process ID if not provided
    if not process_id:
        process_id = generate_deterministic_process_id(display_name, transformation_type)
    
    process_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/processes/{process_id}"
    
    return LineageProcess(
        name=process_name,
        display_name=display_name,
        description=description,
        attributes={
            "transformation_type": transformation_type,
            "system": "looker",
            "created_by": "dataplex_lineage_api"
        }
    )

def create_transformation_run(process_name: str, run_id: str, 
                            display_name: str) -> LineageRun:
    """Create a transformation run definition"""
    run_name = f"{process_name}/runs/{run_id}"
    start_time = datetime.now(timezone.utc)
    # Add 1 second using timedelta to avoid second overflow
    from datetime import timedelta
    end_time = start_time + timedelta(seconds=1)
    
    return LineageRun(
        name=run_name,
        display_name=display_name,
        state="COMPLETED",
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        attributes={
            "execution_time": start_time.isoformat(),
            "status": "success"
        }
    )

def setup_bigquery_to_view_lineage(api: DataLineageAPI) -> bool:
    """Create lineage from BigQuery tables to Looker views"""
    print("\n🔗 CREATING BIGQUERY → LOOKER VIEW LINEAGE")
    print("-" * 60)
    
    # Define BigQuery to View mappings based on existing views in Dataplex
    bigquery_view_mappings = [
        (PROJECT_ID, "retail_banking", "card", "card"),
        (PROJECT_ID, "retail_banking", "client", "client"),
        (PROJECT_ID, "retail_banking", "district", "district"),
        (PROJECT_ID, "retail_banking", "loan", "loan"),
        (PROJECT_ID, "retail_banking", "order", "order"),
        (PROJECT_ID, "retail_banking", "disp", "disp"),
        (PROJECT_ID, "retail_banking", "card_type", "card_type"),
        (PROJECT_ID, "retail_banking", "merchant_fact", "merchant_fact"),
        (PROJECT_ID, "retail_banking", "card_transactions_with_zip", "card_transactions_with_zip"),
        (PROJECT_ID, "retail_banking", "us_zip_code_boundaries", "us_zip_code_boundaries"),
    ]
    
    success_count = 0
    
    for project, dataset, table, view_name in bigquery_view_mappings:
        # Create process for this transformation using deterministic ID
        display_name = f"BigQuery to Looker View: {view_name}"
        process = create_transformation_process(
            process_id="",  # Will be generated deterministically
            display_name=display_name,
            description=f"Data transformation from BigQuery table {table} to Looker view {view_name}",
            transformation_type="view_transformation"
        )
        
        actual_process_name = api.create_process(process)
        if actual_process_name:
            # Create run for this transformation
            run_id = f"run-{int(time.time())}"
            run = create_transformation_run(
                process_name=actual_process_name,
                run_id=run_id,
                display_name=f"Transform {table} to {view_name}"
            )
            
            actual_run_name = api.create_run(actual_process_name, run)
            if actual_run_name:
                # Create lineage event
                source_fqn = generate_bigquery_fqn(project, dataset, table)
                target_fqn = generate_dataplex_fqn("view", view_name)
                
                if api.create_lineage_event(actual_process_name, actual_run_name, source_fqn, target_fqn):
                    success_count += 1
    
    print(f"  ✅ Created {success_count} BigQuery → View lineage relationships")
    return success_count > 0

def setup_view_to_explore_lineage(api: DataLineageAPI) -> bool:
    """Create lineage from Looker views to explores"""
    print("\n🔗 CREATING LOOKER VIEW → EXPLORE LINEAGE")
    print("-" * 60)
    
    # Define View to Explore mappings based on existing views and explores
    view_explore_mappings = [
        # card_transactions explore (using only existing views)
        (["card", "client", "district", "merchant_fact", 
          "card_transactions_with_zip", "us_zip_code_boundaries"], "card_transactions"),
        # balances_fact explore (using only existing views)
        (["client", "district", "disp", "card"], "balances_fact"),
    ]
    
    success_count = 0
    
    for view_names, explore_name in view_explore_mappings:
        # Create process for this transformation using deterministic ID
        display_name = f"Views to Looker Explore: {explore_name}"
        process = create_transformation_process(
            process_id="",  # Will be generated deterministically
            display_name=display_name,
            description=f"Data aggregation from views {', '.join(view_names)} to explore {explore_name}",
            transformation_type="explore_transformation"
        )
        
        actual_process_name = api.create_process(process)
        if actual_process_name:
            # Create run for this transformation
            run_id = f"run-{int(time.time())}"
            run = create_transformation_run(
                process_name=actual_process_name,
                run_id=run_id,
                display_name=f"Aggregate views to {explore_name}"
            )
            
            actual_run_name = api.create_run(actual_process_name, run)
            if actual_run_name:
                # Create lineage events for each view
                target_fqn = generate_dataplex_fqn("explore", explore_name)
                
                for view_name in view_names:
                    source_fqn = generate_dataplex_fqn("view", view_name)
                    if api.create_lineage_event(actual_process_name, actual_run_name, source_fqn, target_fqn):
                        success_count += 1
    
    print(f"  ✅ Created {success_count} View → Explore lineage relationships")
    return success_count > 0

def setup_explore_to_dashboard_lineage(api: DataLineageAPI) -> bool:
    """Create lineage from explores to dashboards"""
    print("\n🔗 CREATING EXPLORE → DASHBOARD LINEAGE")
    print("-" * 60)
    
    # Define Explore to Dashboard mappings based on your dashboards
    explore_dashboard_mappings = [
        ("card_transactions", ["credit_card_fraud_overview", "customer_account", "merchant_deepdive"]),
        ("balances_fact", ["customer_account", "branch_overview"]),
    ]
    
    success_count = 0
    
    for explore_name, dashboard_names in explore_dashboard_mappings:
        for dashboard_name in dashboard_names:
            # Create process for this transformation using deterministic ID
            display_name = f"Explore to Dashboard: {explore_name} to {dashboard_name}"
            process = create_transformation_process(
                process_id="",  # Will be generated deterministically
                display_name=display_name,
                description=f"Data visualization from explore {explore_name} to dashboard {dashboard_name}",
                transformation_type="dashboard_visualization"
            )
            
            actual_process_name = api.create_process(process)
            if actual_process_name:
                # Create run for this transformation
                run_id = f"run-{int(time.time())}"
                run = create_transformation_run(
                    process_name=actual_process_name,
                    run_id=run_id,
                    display_name=f"Visualize {explore_name} in {dashboard_name}"
                )
                
                actual_run_name = api.create_run(actual_process_name, run)
                if actual_run_name:
                    # Create lineage event
                    source_fqn = generate_dataplex_fqn("explore", explore_name)
                    target_fqn = generate_dataplex_fqn("dashboard", dashboard_name)
                    
                    if api.create_lineage_event(actual_process_name, actual_run_name, source_fqn, target_fqn):
                        success_count += 1
    
    print(f"  ✅ Created {success_count} Explore → Dashboard lineage relationships")
    return success_count > 0

def get_existing_processes(api: DataLineageAPI) -> None:
    """Get existing lineage processes"""
    print("\n🔍 GETTING EXISTING LINEAGE PROCESSES")
    print("-" * 60)
    
    url = f"{api.base_url}/processes"
    result = api._make_request("GET", url)
    
    if result and 'processes' in result:
        processes = result['processes']
        print(f"  📊 Found {len(processes)} existing processes:")
        for process in processes:
            print(f"    • {process.get('displayName', 'Unknown')} ({process.get('name', 'Unknown').split('/')[-1]})")
    else:
        print("  ⚠️  No existing processes found")
    
    return result

def get_existing_runs(api: DataLineageAPI, process_name: str = None) -> None:
    """Get existing lineage runs"""
    print("\n🔍 GETTING EXISTING LINEAGE RUNS")
    print("-" * 60)
    
    if process_name:
        process_id = process_name.split('/')[-1]
        url = f"{api.base_url}/processes/{process_id}/runs"
        result = api._make_request("GET", url)
        
        if result and 'runs' in result:
            runs = result['runs']
            print(f"  📊 Found {len(runs)} runs for process {process_id}:")
            for run in runs:
                print(f"    • {run.get('displayName', 'Unknown')} ({run.get('state', 'Unknown')})")
        else:
            print(f"  ⚠️  No runs found for process {process_id}")
    else:
        print("  ⚠️  No process name provided")

def search_existing_links(api: DataLineageAPI) -> None:
    """Search for existing lineage links"""
    print("\n🔍 SEARCHING EXISTING LINEAGE LINKS")
    print("-" * 60)
    
    # Test a few key assets
    test_assets = [
        generate_bigquery_fqn(PROJECT_ID, "retail_banking", "card_transactions"),
        generate_dataplex_fqn("view", "card_transactions"),
        generate_dataplex_fqn("explore", "card_transactions"),
        generate_dataplex_fqn("dashboard", "credit_card_fraud_overview")
    ]
    
    for asset_fqn in test_assets:
        asset_name = asset_fqn.split('/')[-1]
        print(f"\n  🔍 Searching links for: {asset_name}")
        
        # Search as source
        links = api.search_links(asset_fqn)
        if links and 'links' in links:
            link_count = len(links['links'])
            print(f"    📊 As source: {link_count} downstream connections")
            for link in links['links'][:3]:  # Show first 3
                target = link.get('target', {}).get('fullyQualifiedName', 'Unknown')
                target_name = target.split('/')[-1] if '/' in target else target
                print(f"      → {target_name}")
        else:
            print(f"    ⚠️  As source: No downstream connections found")
        
        # Search as target (reverse search)
        reverse_search_data = {
            "target": {"fullyQualifiedName": asset_fqn}
        }
        url = f"{api.base_url}:searchLinks"
        reverse_links = api._make_request("POST", url, reverse_search_data)
        
        if reverse_links and 'links' in reverse_links:
            link_count = len(reverse_links['links'])
            print(f"    📊 As target: {link_count} upstream connections")
            for link in reverse_links['links'][:3]:  # Show first 3
                source = link.get('source', {}).get('fullyQualifiedName', 'Unknown')
                source_name = source.split('/')[-1] if '/' in source else source
                print(f"      ← {source_name}")
        else:
            print(f"    ⚠️  As target: No upstream connections found")

def verify_lineage(api: DataLineageAPI) -> None:
    """Verify created lineage by searching for links"""
    search_existing_links(api)

def main():
    """Main function to create Data Lineage API relationships"""
    print("🚀 STARTING DATAPLEX DATA LINEAGE API SETUP WITH AUTO-CLEANUP")
    print("=" * 80)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"📍 Location: {LOCATION}")
    print(f"📍 Entry Group: {ENTRY_GROUP}")
    print("=" * 80)
    
    # Initialize Data Lineage API client
    try:
        api = DataLineageAPI(PROJECT_ID, LOCATION)
        print("✅ Data Lineage API client initialized")
    except Exception as e:
        print(f"❌ Failed to initialize API client: {e}")
        return
    
    # STEP 0: Clean up all existing processes first
    print("\n🧹 PHASE 1: CLEANUP EXISTING PROCESSES")
    print("=" * 80)
    cleanup_success = api.cleanup_all_processes()
    
    if not cleanup_success:
        print("⚠️  Cleanup had some issues, but proceeding with recreation...")
    
    # Wait a moment for cleanup to propagate
    print("\n⏳ Waiting 5 seconds for cleanup to propagate...")
    time.sleep(5)
    
    # STEP 1: Create fresh lineage relationships
    print("\n🔗 PHASE 2: CREATING FRESH LINEAGE RELATIONSHIPS")
    print("=" * 80)
    success = True
    
    # 1. BigQuery → Looker Views
    if not setup_bigquery_to_view_lineage(api):
        success = False
    
    # 2. Looker Views → Explores
    if not setup_view_to_explore_lineage(api):
        success = False
    
    # 3. Explores → Dashboards
    if not setup_explore_to_dashboard_lineage(api):
        success = False
    
    # 4. Verify lineage
    print("\n🔍 PHASE 3: VERIFICATION")
    print("=" * 80)
    verify_lineage(api)
    
    # Summary
    print("\n" + "=" * 80)
    if success:
        print("🎉 DATA LINEAGE API SETUP COMPLETE!")
        print("=" * 80)
        print("✅ Created formal lineage relationships using Data Lineage API")
        print("✅ Established processes, runs, and lineage events")
        print("✅ Connected BigQuery → Views → Explores → Dashboards")
        print("")
        print("🔗 LINEAGE CHAIN CREATED:")
        print("   BigQuery Tables → Looker Views → Looker Explores → Looker Dashboards")
        print("")
        print("🎯 NEXT STEPS:")
        print("1. 🌐 Open Dataplex console: https://console.cloud.google.com/dataplex/catalog")
        print("2. 📁 Navigate to any entry in your entry group")
        print("3. 📊 Click the 'Lineage' tab to see the visual graph")
        print("4. 🔍 Explore upstream and downstream relationships")
        print("")
        print("💡 FEATURES ENABLED:")
        print("   • Visual lineage graphs in Dataplex UI")
        print("   • Impact analysis (what's affected by changes)")
        print("   • Data discovery (trace data from source to consumption)")
        print("   • Compliance and governance tracking")
    else:
        print("❌ SOME LINEAGE SETUP STEPS FAILED")
        print("Check the logs above for specific errors")

if __name__ == "__main__":
    main()
