# Dataplex Looker Metadata Ingestion - Execution Guide

This guide provides the complete workflow for setting up and ingesting Looker metadata into Google Cloud Dataplex using the latest modular approach with environment variable configuration.

## 📋 Overview

The ingestion process uses modular scripts with environment variable configuration for better security, control, and selective execution. Each script handles a specific aspect of the setup or ingestion process and can be run independently.

## 🏗️ Prerequisites

Before running any scripts, ensure you have:

1. **Google Cloud SDK** installed and authenticated
2. **Project permissions** for Dataplex operations
3. **Environment configured** with your GCP project ID

## 🔧 Environment Setup (Required First Step)

### 1. Configure Environment Variables

```bash
# Copy the template file
cp .env.template .env

# Edit with your actual GCP project ID
nano .env
```

Update your `.env` file:
```bash
# Required: Your Google Cloud Project ID
GCP_PROJECT_ID=your-actual-project-id

# Optional: Other settings (defaults shown)
GCP_LOCATION=eu
DATAPLEX_ENTRY_GROUP=looker
LOOKER_INSTANCE_ID=mylooker
BQ_DATASET=retail_banking
```

### 2. Test Configuration

```bash
# Validate your configuration
python3 config.py
```

Expected output:
```
🚀 Dataplex Looker Configuration
==================================================
✅ Configuration validation passed
🔧 Current Configuration:
   GCP Project: your-actual-project-id
   GCP Location: eu
   Entry Group: looker
   Looker Instance: mylooker
   BQ Dataset: retail_banking
   Debug Mode: False
```

## 📊 Script Organization

### **Phase 1: Setup Scripts (Run Once)**
1. `setup_1_entry_group.py` - Creates the Dataplex entry group
2. `setup_2_aspect_types.py` - Creates aspect types for metadata structure
3. `setup_3_entry_types.py` - Creates entry types with required aspects

### **Phase 2: Ingestion Scripts (Run in Order)**
4. `ingest_views_only.py` - Ingests LookML views with field metadata
5. `ingest_explores_only.py` - Ingests explores with join relationships
6. `ingest_dashboards_only.py` - Ingests dashboards with element information
7. `create_structural_links.py` - Creates relationships between entities

### **Phase 3: Optional Lineage (Advanced)**
8. `dataplex_lineage_api.py` - Creates formal lineage using Data Lineage API

### **Utility Scripts**
- `cleanup_dataplex.py` - Cleans up entries for fresh start
- `config.py` - Configuration validation and testing

## 🚀 Complete Execution Order

### **PHASE 1: Environment & Setup (One-time)**

```bash
# Step 0: Configure environment (REQUIRED)
cp .env.template .env
# Edit .env with your GCP project ID
python3 config.py  # Test configuration

# Step 1: Create entry group
python3 setup_1_entry_group.py

# Step 2: Create aspect types
python3 setup_2_aspect_types.py

# Step 3: Create entry types
python3 setup_3_entry_types.py
```

### **PHASE 2: Metadata Ingestion (Main Process)**

```bash
# Step 4: Ingest Views First (Foundation)
python3 ingest_views_only.py

# Step 5: Ingest Explores (Depends on Views)
python3 ingest_explores_only.py

# Step 6: Ingest Dashboards (References Explores)
python3 ingest_dashboards_only.py

# Step 7: Create Structural Links (Final Step)
python3 create_structural_links.py
```

### **PHASE 3: Advanced Lineage (Optional)**

```bash
# Step 8: Create formal lineage relationships
python3 dataplex_lineage_api.py
```

### **PHASE 4: Verification**

```bash
# Check ingested entries
gcloud dataplex entries list \
  --project=$(python3 -c "from config import PROJECT_ID; print(PROJECT_ID)") \
  --location=$(python3 -c "from config import LOCATION; print(LOCATION)") \
  --entry-group=$(python3 -c "from config import ENTRY_GROUP; print(ENTRY_GROUP)") \
  --format="table(name.basename(),entryType,fullyQualifiedName)"

# Check entry links
gcloud dataplex entry-links list \
  --project=$(python3 -c "from config import PROJECT_ID; print(PROJECT_ID)") \
  --location=$(python3 -c "from config import LOCATION; print(LOCATION)") \
  --entry-group=$(python3 -c "from config import ENTRY_GROUP; print(ENTRY_GROUP)")
```

## 📝 Detailed Script Descriptions

### **Setup Scripts**

#### 1. setup_1_entry_group.py
**Purpose**: Creates the Dataplex entry group for Looker metadata

**What it does**:
- Creates the entry group with proper FQN specifications
- Uses environment variables for project configuration
- Handles existing entry group gracefully

**Expected Output**: Entry group created or already exists message

#### 2. setup_2_aspect_types.py
**Purpose**: Creates all required aspect types

**What it does**:
- Creates `looker.core` - Core metadata for all Looker entries
- Creates `looker.exploreGraph` - Explore graph metadata with views and joins
- Creates `looker.viewSchema` - View schema with field definitions
- Creates `bqDependencies` - BigQuery table and column dependencies

**Expected Output**: 4 aspect types created

#### 3. setup_3_entry_types.py
**Purpose**: Creates entry types with required aspects

**What it does**:
- Creates `looker-dashboard` (requires looker.core)
- Creates `looker-look` (requires looker.core)
- Creates `looker-explore` (requires looker.core + looker.exploreGraph)
- Creates `looker-view` (requires looker.core + looker.viewSchema)

**Expected Output**: 4 entry types created

### **Ingestion Scripts**

#### 4. ingest_views_only.py
**Purpose**: Ingests LookML view files with detailed field metadata

**What it does**:
- Parses `.view.lkml` files from `banking_and_card_views/` and `geo_views/`
- Extracts dimensions and measures with SQL definitions
- Creates entries with `looker-core` and `looker-view-schema` aspects
- Maps to BigQuery tables when `sql_table_name` is defined

**Expected Output**: ~30+ view entries

#### 5. ingest_explores_only.py
**Purpose**: Ingests explore definitions with join relationships

**What it does**:
- Parses explores from `models/retail_banking.model.lkml` and `models/retail_banking_explores.lkml`
- Extracts join relationships and view dependencies
- Creates entries with `looker-core` and `looker-explore-graph` aspects
- Links to BigQuery dependencies through views

**Expected Output**: ~12 explore entries

#### 6. ingest_dashboards_only.py
**Purpose**: Ingests dashboard definitions with element information

**What it does**:
- Parses `.dashboard.lookml` files from `dashboards/`
- Extracts dashboard elements and explore dependencies
- Creates entries with `looker-core` aspect
- Identifies which explores each dashboard uses

**Expected Output**: ~9 dashboard entries

#### 7. create_structural_links.py
**Purpose**: Creates relationships between ingested entities

**What it does**:
- Fetches existing entries from Dataplex
- Creates "uses" links from dashboards to explores
- Creates "uses" links from explores to views
- Creates "maps_to" links from views to BigQuery tables

**Expected Output**: Multiple entry links showing relationships

### **Advanced Lineage Script**

#### 8. dataplex_lineage_api.py
**Purpose**: Creates formal lineage using Data Lineage REST API

**What it does**:
- Creates processes for transformations (BigQuery → Views → Explores → Dashboards)
- Creates runs for execution instances
- Creates lineage events with formal source-to-target relationships
- Enables visual lineage graphs in Dataplex console

**Expected Output**: Formal lineage relationships and visual graphs

## 🔧 Configuration Benefits

### **Secure Configuration**
- No hardcoded project IDs in scripts
- Environment variables loaded from `.env` file
- `.env` file excluded from git repository

### **Easy Environment Switching**
```bash
# Development
cp .env.template .env.dev
# Edit with dev project ID

# Production  
cp .env.template .env.prod
# Edit with prod project ID

# Switch environments
cp .env.dev .env && python3 setup_1_entry_group.py
```

## 🚨 Troubleshooting

### **Configuration Issues**

1. **Environment Variable Not Set**
   ```bash
   # Error: Required environment variable 'GCP_PROJECT_ID' is not set
   # Solution: Check your .env file
   cat .env
   python3 config.py
   ```

2. **Import Errors**
   ```bash
   # Error: ModuleNotFoundError: No module named 'config'
   # Solution: Run from project root directory
   cd /path/to/lookerdataplex
   python3 setup_1_entry_group.py
   ```

### **Common Setup Issues**

1. **Entry Already Exists**
   - Scripts automatically skip existing entries
   - Use `cleanup_dataplex.py` for fresh start if needed

2. **Permission Errors**
   - Ensure proper IAM roles for Dataplex operations
   - Check `gcloud auth list` for active account

3. **Entry Type Not Found**
   - Run setup scripts in order (1, 2, 3)
   - Verify entry types exist: `gcloud dataplex entry-types list`

### **Verification Commands**

```bash
# Check entry types
gcloud dataplex entry-types list \
  --project=$(python3 -c "from config import PROJECT_ID; print(PROJECT_ID)") \
  --location=$(python3 -c "from config import LOCATION; print(LOCATION)")

# Check aspect types
gcloud dataplex aspect-types list \
  --project=$(python3 -c "from config import PROJECT_ID; print(PROJECT_ID)") \
  --location=$(python3 -c "from config import LOCATION; print(LOCATION)")

# Check entries by type
gcloud dataplex entries list \
  --project=$(python3 -c "from config import PROJECT_ID; print(PROJECT_ID)") \
  --location=$(python3 -c "from config import LOCATION; print(LOCATION)") \
  --entry-group=$(python3 -c "from config import ENTRY_GROUP; print(ENTRY_GROUP)") \
  --filter="entryType:looker-view"
```

## 🧹 Cleanup

To start fresh:
```bash
# Clean up all entries (use with caution)
python3 cleanup_dataplex.py

# Or manually delete specific entries
gcloud dataplex entries delete ENTRY_ID \
  --project=$(python3 -c "from config import PROJECT_ID; print(PROJECT_ID)") \
  --location=$(python3 -c "from config import LOCATION; print(LOCATION)") \
  --entry-group=$(python3 -c "from config import ENTRY_GROUP; print(ENTRY_GROUP)")
```

## 📊 Expected Results

After successful execution:
- **Entry Group**: 1 entry group (`looker`)
- **Aspect Types**: 4 aspect types (looker.core, looker.exploreGraph, looker.viewSchema, bqDependencies)
- **Entry Types**: 4 entry types (looker-view, looker-explore, looker-dashboard, looker-look)
- **Views**: ~30 entries with field-level metadata
- **Explores**: ~12 entries with join relationships
- **Dashboards**: ~9 entries with element information
- **Links**: Multiple structural relationships between entities
- **Lineage**: Visual lineage graphs (if Data Lineage API used)

## 🎯 Next Steps

1. **Verify in Dataplex Console**: https://console.cloud.google.com/dataplex/catalog
2. **Explore Relationships**: Click on entries to see structural links
3. **View Lineage**: Check the "Lineage" tab for visual graphs
4. **Set up Automation**: Schedule regular metadata refresh
5. **Integrate with Looker**: Connect Looker to use Dataplex metadata

## 📚 Additional Resources

- `ENVIRONMENT_SETUP.md` - Detailed environment configuration guide
- `README_DATAPLEX_LINEAGE.md` - Detailed lineage documentation
- `NEW_DATAPLEX_SETUP_GUIDE.md` - Comprehensive setup guide
- `DATAPLEX_LINEAGE_COMPLETE_GUIDE.md` - Complete lineage guide

---

**🎉 Success Indicators**

After running all scripts successfully, you should have:
- ✅ Secure environment variable configuration
- ✅ Complete metadata catalog with 50+ entries
- ✅ Visual lineage graphs showing data flow
- ✅ Interactive navigation through relationships
- ✅ Impact analysis capabilities
- ✅ Data discovery features

**Note**: This modular approach with environment variables provides enterprise-grade security and maintainability while allowing selective re-ingestion of specific entity types.
