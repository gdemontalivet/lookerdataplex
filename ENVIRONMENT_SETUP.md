# Environment Configuration Setup

This document explains how to configure your environment variables for the Dataplex Looker integration project.

## 🔧 Overview

The project now uses environment variables to store sensitive configuration like your GCP Project ID, keeping them out of the git repository for security.

## 📁 Files Created

- **`config.py`** - Configuration module that loads environment variables
- **`.env`** - Your actual configuration values (not committed to git)
- **`.env.template`** - Template file showing required variables
- **`.gitignore`** - Updated to exclude `.env` file from git

## 🚀 Quick Setup

### 1. Copy the template file
```bash
cp .env.template .env
```

### 2. Edit your .env file
```bash
# Edit with your actual values
nano .env
```

### 3. Update your GCP Project ID
```bash
# In .env file, change:
GCP_PROJECT_ID=your-actual-project-id
```

### 4. Test the configuration
```bash
python3 config.py
```

## 📋 Environment Variables

### Required Variables
- **`GCP_PROJECT_ID`** - Your Google Cloud Project ID

### Optional Variables (with defaults)
- **`GCP_LOCATION`** - GCP location (default: "eu")
- **`DATAPLEX_ENTRY_GROUP`** - Entry group name (default: "looker")
- **`LOOKER_INSTANCE_ID`** - Looker instance ID (default: "mylooker")
- **`BQ_DATASET`** - BigQuery dataset (default: "retail_banking")
- **`DEBUG`** - Enable debug mode (default: "false")

## 🔒 Security Benefits

### Before (Insecure)
```python
# Hardcoded in every script
PROJECT_ID = "local-dimension-399810"  # ❌ Exposed in git
```

### After (Secure)
```python
# Imported from config module
from config import PROJECT_ID  # ✅ Loaded from .env
```

## 📊 Updated Scripts

All scripts now use the config module:

### Setup Scripts
- `setup_1_entry_group.py`
- `setup_2_aspect_types.py`
- `setup_3_entry_types.py`

### Ingestion Scripts
- `ingest_views_only.py`
- `ingest_explores_only.py`
- `ingest_dashboards_only.py`

### Utility Scripts
- `create_structural_links.py`
- `dataplex_lineage_api.py`
- `cleanup_dataplex.py`

## 🎯 Usage Examples

### Running Scripts
```bash
# All scripts now automatically load from .env
python3 setup_1_entry_group.py
python3 ingest_views_only.py
```

### Checking Configuration
```bash
# Validate your configuration
python3 config.py
```

### Different Environments
```bash
# Development
cp .env.template .env.dev
# Edit .env.dev with dev project ID

# Production
cp .env.template .env.prod
# Edit .env.prod with prod project ID

# Use specific env file
export $(cat .env.dev | xargs) && python3 setup_1_entry_group.py
```

## 🚨 Important Notes

### Git Security
- **`.env`** is in `.gitignore` - never commit it!
- **`.env.template`** is safe to commit (no actual values)
- All hardcoded project IDs have been removed from scripts

### Sharing the Project
When sharing this project:
1. Others get `.env.template`
2. They copy it to `.env`
3. They add their own project ID
4. Scripts work with their configuration

## 🔧 Troubleshooting

### Configuration Not Found
```bash
# Error: Required environment variable 'GCP_PROJECT_ID' is not set
# Solution: Check your .env file exists and has the right values
ls -la .env
cat .env
```

### Import Errors
```bash
# Error: ModuleNotFoundError: No module named 'config'
# Solution: Run scripts from the project root directory
cd /path/to/lookerdataplex
python3 setup_1_entry_group.py
```

### Testing Configuration
```bash
# Test that config loads correctly
python3 -c "from config import PROJECT_ID; print(f'Project: {PROJECT_ID}')"
```

## 🎉 Benefits Achieved

✅ **Security**: No sensitive data in git repository  
✅ **Flexibility**: Easy to switch between environments  
✅ **Maintainability**: Single place to update configuration  
✅ **Collaboration**: Team members use their own project IDs  
✅ **Best Practices**: Following 12-factor app methodology  

---

**Next Steps**: Your environment is now configured! You can run the modular setup scripts:

```bash
python3 setup_1_entry_group.py
python3 setup_2_aspect_types.py
python3 setup_3_entry_types.py
