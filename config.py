#!/usr/bin/env python3
"""
Configuration module for Dataplex Looker integration
Loads configuration from environment variables
"""

import os
from typing import Optional

# Load .env file if it exists
def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

# Load .env file at module import
load_env_file()

def get_env_var(var_name: str, default: Optional[str] = None, required: bool = True) -> str:
    """Get environment variable with optional default and required validation."""
    value = os.getenv(var_name, default)
    
    if required and not value:
        raise ValueError(f"Required environment variable '{var_name}' is not set. "
                        f"Please set it in your .env file or environment.")
    
    return value

# Core GCP Configuration
PROJECT_ID = get_env_var("GCP_PROJECT_ID")
LOCATION = get_env_var("GCP_LOCATION", default="eu")
ENTRY_GROUP = get_env_var("DATAPLEX_ENTRY_GROUP", default="looker")

# Looker Configuration
LOOKER_INSTANCE_ID = get_env_var("LOOKER_INSTANCE_ID", default="mylooker")

# BigQuery Configuration
BQ_DATASET = get_env_var("BQ_DATASET", default="retail_banking")

# Optional Configuration
DEBUG = get_env_var("DEBUG", default="false", required=False).lower() == "true"

def print_config():
    """Print current configuration (without sensitive values)."""
    print("🔧 Current Configuration:")
    print(f"   GCP Project: {PROJECT_ID}")
    print(f"   GCP Location: {LOCATION}")
    print(f"   Entry Group: {ENTRY_GROUP}")
    print(f"   Looker Instance: {LOOKER_INSTANCE_ID}")
    print(f"   BQ Dataset: {BQ_DATASET}")
    print(f"   Debug Mode: {DEBUG}")

def validate_config():
    """Validate that all required configuration is present."""
    try:
        # Test that all required variables are accessible
        _ = PROJECT_ID
        _ = LOCATION
        _ = ENTRY_GROUP
        _ = LOOKER_INSTANCE_ID
        _ = BQ_DATASET
        
        print("✅ Configuration validation passed")
        return True
    except ValueError as e:
        print(f"❌ Configuration validation failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Dataplex Looker Configuration")
    print("=" * 50)
    
    if validate_config():
        print_config()
    else:
        print("\n💡 Please check your .env file or environment variables")
