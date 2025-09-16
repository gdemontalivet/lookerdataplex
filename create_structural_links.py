#!/usr/bin/env python3
"""
Create structural links for Looker entity relationships
Following the specifications for Entry Links to represent relationships.
Updated to work with the actual retail banking project structure in Dataplex.
"""

import subprocess
import json
import tempfile
import os
import requests
import uuid
from typing import Dict, List, Optional

# Configuration
from config import PROJECT_ID, LOCATION, ENTRY_GROUP, LOOKER_INSTANCE_ID

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
            print(f"⚠️  {description} (may not exist)")
            return None
        else:
            print(f"❌ Error: {description}")
            print(f"Error: {e.stderr}")
            return None

def get_existing_entries():
    """Get all existing entries from Dataplex."""
    print("🔍 Fetching existing entries from Dataplex...")
    
    command_args = [
        "gcloud", "dataplex", "entries", "list",
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--entry-group={ENTRY_GROUP}",
        "--format=json"
    ]
    
    result = run_gcloud_command(command_args, "Fetching entries")
    if result:
        try:
            entries = json.loads(result)
            return entries
        except json.JSONDecodeError:
            print("❌ Failed to parse entries JSON")
            return []
    return []

def parse_entries_by_type(entries):
    """Parse entries by type for easier processing."""
    dashboards = []
    explores = []
    views = []
    
    for entry in entries:
        entry_type = entry.get("entryType", "")
        entry_name = entry.get("name", "").split("/")[-1]
        fqn = entry.get("fullyQualifiedName", "")
        
        if "looker-dashboard" in entry_type:
            dashboards.append({"id": entry_name, "fqn": fqn})
        elif "looker-explore" in entry_type:
            explores.append({"id": entry_name, "fqn": fqn})
        elif "looker-view" in entry_type:
            views.append({"id": entry_name, "fqn": fqn})
    
    return dashboards, explores, views

def verify_entry_exists(entry_id):
    """Verify that an entry exists in Dataplex."""
    command_args = [
        "gcloud", "dataplex", "entries", "describe", entry_id,
        f"--project={PROJECT_ID}",
        f"--location={LOCATION}",
        f"--entry-group={ENTRY_GROUP}",
        "--format=json"
    ]
    
    result = run_gcloud_command(command_args, "", ignore_errors=True)
    return result is not None

def get_access_token():
    """Get access token for Google Cloud API calls."""
    try:
        result = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to get access token: {e}")
        return None

def create_entry_link(source_entry_id, target_entry_id, link_type, description=""):
    """Create an entry link between two entities using REST API."""
    
    # First verify both entries exist
    if not verify_entry_exists(source_entry_id):
        print(f"  ⚠️  Source entry {source_entry_id} does not exist, skipping link")
        return False
    
    if not verify_entry_exists(target_entry_id):
        print(f"  ⚠️  Target entry {target_entry_id} does not exist, skipping link")
        return False
    
    # Get access token
    access_token = get_access_token()
    if not access_token:
        print(f"  ❌ Failed to get access token")
        return False
    
    # Generate unique entry link ID
    entry_link_id = f"link-{uuid.uuid4().hex[:8]}"
    
    # Prepare the REST API request
    parent = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}"
    url = f"https://dataplex.googleapis.com/v1/{parent}/entryLinks"
    
    # Map link_type to proper entryLinkType
    entry_link_type_mapping = {
        "uses": "projects/dataplex-types/locations/global/entryLinkTypes/related",
        "maps_to": "projects/dataplex-types/locations/global/entryLinkTypes/definition",
        "related": "projects/dataplex-types/locations/global/entryLinkTypes/related",
        "definition": "projects/dataplex-types/locations/global/entryLinkTypes/definition"
    }
    
    entry_link_type = entry_link_type_mapping.get(link_type, "projects/dataplex-types/locations/global/entryLinkTypes/related")
    
    # Prepare request body
    request_body = {
        "entryLinkType": entry_link_type,
        "entryReferences": [
            {
                "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{source_entry_id}",
                "type": "UNSPECIFIED"
            },
            {
                "name": f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroups/{ENTRY_GROUP}/entries/{target_entry_id}",
                "type": "UNSPECIFIED"
            }
        ]
    }
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "entryLinkId": entry_link_id
    }
    
    try:
        print(f"  🔄 Creating {link_type} link: {source_entry_id} → {target_entry_id}")
        response = requests.post(url, json=request_body, headers=headers, params=params)
        
        if response.status_code == 200:
            print(f"  ✅ Successfully created {link_type} link: {source_entry_id} → {target_entry_id}")
            return True
        elif response.status_code == 409:
            print(f"  ℹ️  Link already exists: {source_entry_id} → {target_entry_id} ({link_type})")
            return True
        else:
            print(f"  ❌ Failed to create link: {response.status_code} - {response.text}")
            return False
            
    except requests.RequestException as e:
        print(f"  ❌ Request failed: {e}")
        return False

def create_dashboard_to_explore_links(dashboards, explores):
    """Create 'uses' links from dashboards to explores they use."""
    print("\n📈 CREATING DASHBOARD → EXPLORE LINKS")
    print("-" * 50)
    
    # Map dashboard names to explore names based on actual entries in Dataplex
    dashboard_explore_relationships = {
        "mylooker-retail_banking-credit_card_fraud_overview": ["mylooker-retail_banking-card_transactions"],
        "mylooker-retail_banking-customer_account": ["mylooker-retail_banking-account", "mylooker-retail_banking-card_transactions"],
        "mylooker-retail_banking-merchant_deepdive": ["mylooker-retail_banking-card_transactions"],
        "mylooker-retail_banking-fraud_model_performance": ["mylooker-retail_banking-card_transactions"],
        "mylooker-retail_banking-location_analysis": ["mylooker-retail_banking-account", "mylooker-retail_banking-branches"],
        "mylooker-retail_banking-branch_overview": ["mylooker-retail_banking-account", "mylooker-retail_banking-branches"],
        "mylooker-retail_banking-brick_and_mortar_decision_dashboard": ["mylooker-retail_banking-account", "mylooker-retail_banking-branches"],
        "mylooker-retail_banking-card_type_lookup": ["mylooker-retail_banking-card_payments", "mylooker-retail_banking-card_transactions"],
        "mylooker-retail_banking-cc_customer_marketing": ["mylooker-retail_banking-card_transactions", "mylooker-retail_banking-card_payments"]
    }
    
    # Create lookup maps
    dashboard_map = {d["id"]: d for d in dashboards}
    explore_map = {e["id"]: e for e in explores}
    
    links_created = 0
    
    for dashboard_id, explore_ids in dashboard_explore_relationships.items():
        if dashboard_id not in dashboard_map:
            continue
            
        for explore_id in explore_ids:
            if explore_id not in explore_map:
                continue
            
            if create_entry_link(
                dashboard_id, 
                explore_id, 
                "uses",
                f"Dashboard {dashboard_id.split('-')[-1]} uses explore {explore_id.split('-')[-1]}"
            ):
                links_created += 1
    
    print(f"  📊 Created {links_created} dashboard → explore links")
    return links_created

def create_explore_to_view_links(explores, views):
    """Create 'uses' links from explores to views they depend on."""
    print("\n🔍 CREATING EXPLORE → VIEW LINKS")
    print("-" * 50)
    
    # Map explore IDs to view IDs based on actual entries in Dataplex
    explore_view_relationships = {
        "mylooker-retail_banking-card_transactions": [
            "mylooker-retail_banking-card", "mylooker-retail_banking-disp", 
            "mylooker-retail_banking-client", "mylooker-retail_banking-merchant_fact", 
            "mylooker-retail_banking-card_transactions_with_zip"
        ],
        "mylooker-retail_banking-account": [
            "mylooker-retail_banking-district", "mylooker-retail_banking-loan", 
            "mylooker-retail_banking-disp", "mylooker-retail_banking-card", 
            "mylooker-retail_banking-client", "mylooker-retail_banking-client_fact"
        ],
        "mylooker-retail_banking-trans": [
            "mylooker-retail_banking-disp", "mylooker-retail_banking-card", 
            "mylooker-retail_banking-district", "mylooker-retail_banking-client"
        ],
        "mylooker-retail_banking-card_payments": [
            "mylooker-retail_banking-card_payment_dates", "mylooker-retail_banking-card", 
            "mylooker-retail_banking-card_type", "mylooker-retail_banking-disp", "mylooker-retail_banking-client"
        ],
        "mylooker-retail_banking-balances_fact": [
            "mylooker-retail_banking-account_fact", "mylooker-retail_banking-district", 
            "mylooker-retail_banking-client", "mylooker-retail_banking-card"
        ],
        "mylooker-retail_banking-branches": [
            "mylooker-retail_banking-district"
        ]
    }
    
    # Create lookup maps
    explore_map = {e["id"]: e for e in explores}
    view_map = {v["id"]: v for v in views}
    
    links_created = 0
    
    for explore_id, view_ids in explore_view_relationships.items():
        if explore_id not in explore_map:
            continue
            
        for view_id in view_ids:
            if view_id not in view_map:
                continue
            
            if create_entry_link(
                explore_id, 
                view_id, 
                "uses",
                f"Explore {explore_id.split('-')[-1]} uses view {view_id.split('-')[-1]}"
            ):
                links_created += 1
    
    print(f"  📊 Created {links_created} explore → view links")
    return links_created

def create_view_to_bigquery_links(views):
    """Create 'maps_to' links from views to BigQuery tables."""
    print("\n📊 CREATING VIEW → BIGQUERY LINKS")
    print("-" * 50)
    
    # Map view names to BigQuery table paths based on retail banking setup
    view_table_mappings = {
        "card_transactions": f"{PROJECT_ID}.retail_banking.card_transactions",
        "card": f"{PROJECT_ID}.retail_banking.card",
        "client": f"{PROJECT_ID}.retail_banking.client",
        "account": f"{PROJECT_ID}.retail_banking.account",
        "district": f"{PROJECT_ID}.retail_banking.district",
        "disp": f"{PROJECT_ID}.retail_banking.disp",
        "loan": f"{PROJECT_ID}.retail_banking.loan",
        "trans": f"{PROJECT_ID}.retail_banking.trans",
        "card_payments": f"{PROJECT_ID}.retail_banking.card_payments",
        "merchant_fact": f"{PROJECT_ID}.retail_banking.merchant_fact",
        "balances_fact": f"{PROJECT_ID}.retail_banking.balances_fact",
        "account_fact": f"{PROJECT_ID}.retail_banking.account_fact",
        "client_fact": f"{PROJECT_ID}.retail_banking.client_fact",
        "card_type": f"{PROJECT_ID}.retail_banking.card_type",
        "card_payment_dates": f"{PROJECT_ID}.retail_banking.card_payment_dates",
        "card_order_sequence": f"{PROJECT_ID}.retail_banking.card_order_sequence",
        "card_transactions_with_zip": f"{PROJECT_ID}.retail_banking.card_transactions_with_zip",
        "order": f"{PROJECT_ID}.retail_banking.order"
    }
    
    # Create lookup map
    view_map = {v["id"]: v for v in views}
    
    links_created = 0
    
    for view_name, table_path in view_table_mappings.items():
        view_id = f"{LOOKER_INSTANCE_ID}-retail_banking-{view_name}"
        if view_id not in view_map:
            continue
        
        # For BigQuery links, we need to create the BigQuery entry first or assume it exists
        # For now, we'll create the link assuming the BigQuery table exists in Dataplex
        bq_entry_id = table_path.replace(".", "-")
        
        if create_entry_link(
            view_id, 
            bq_entry_id, 
            "maps_to",
            f"View {view_name} maps to BigQuery table {table_path}"
        ):
            links_created += 1
    
    print(f"  📊 Created {links_created} view → BigQuery links")
    return links_created

def main():
    """Main function to create all structural links."""
    print("🔗 CREATING STRUCTURAL LINKS FOR LOOKER ENTITIES")
    print("=" * 70)
    print(f"📍 Project: {PROJECT_ID}")
    print(f"🌍 Location: {LOCATION}")
    print(f"📁 Entry Group: {ENTRY_GROUP}")
    print(f"🔍 Looker Instance: {LOOKER_INSTANCE_ID}")
    print("=" * 70)
    
    # Get existing entries from Dataplex
    entries = get_existing_entries()
    if not entries:
        print("❌ No entries found in Dataplex. Please run ingestion scripts first.")
        return
    
    # Parse entries by type
    dashboards, explores, views = parse_entries_by_type(entries)
    
    print(f"\n📊 FOUND ENTRIES:")
    print(f"  📈 Dashboards: {len(dashboards)}")
    print(f"  🔍 Explores: {len(explores)}")
    print(f"  📊 Views: {len(views)}")
    
    total_links = 0
    
    # Create different types of structural links
    if dashboards and explores:
        total_links += create_dashboard_to_explore_links(dashboards, explores)
    
    if explores and views:
        total_links += create_explore_to_view_links(explores, views)
    
    if views:
        total_links += create_view_to_bigquery_links(views)
    
    # Summary
    print(f"\n" + "=" * 70)
    print(f"🎉 STRUCTURAL LINKS CREATION COMPLETE!")
    print(f"=" * 70)
    print(f"🔗 Total links created: {total_links}")
    print(f"")
    print(f"📊 LINK TYPES CREATED:")
    print(f"   • Dashboard → Explore (uses)")
    print(f"   • Explore → View (uses)")
    print(f"   • View → BigQuery Table (maps_to)")
    print(f"")
    print(f"🎯 BENEFITS:")
    print(f"   • Improved browsing & search context")
    print(f"   • Better entity relationship understanding")
    print(f"   • Enhanced navigation in Dataplex console")
    print(f"   • Complementary to lineage relationships")
    print(f"")
    print(f"🎯 NEXT STEPS:")
    print(f"1. 🌐 Open Dataplex console: https://console.cloud.google.com/dataplex/catalog")
    print(f"2. 📁 Navigate to entry group: '{ENTRY_GROUP}'")
    print(f"3. 🔍 Click on any entry to see structural relationships")
    print(f"4. 📊 Explore the enhanced browsing experience")
    print(f"")
    print(f"💡 NOTE:")
    print(f"   Structural links represent 'contains/uses/defined-in' relationships")
    print(f"   They are separate from lineage and improve browsing context")

if __name__ == "__main__":
    main()
