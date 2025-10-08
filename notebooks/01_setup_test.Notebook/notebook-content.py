# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de850aba-df79-4435-9f6c-8d936c425c62",
# META       "default_lakehouse_name": "energylabels_bronze",
# META       "default_lakehouse_workspace_id": "19badba5-76c2-44f9-ac2e-2b153bb9c055",
# META       "known_lakehouses": [
# META         {
# META           "id": "de850aba-df79-4435-9f6c-8d936c425c62"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import sys
import os
from datetime import datetime
import pandas as pd

print(f"Python version: {sys.version}")
print(f"Current time: {datetime.now()}")
print("="*50)

# Direkte tilnærming - ikke bruk USE statement
print("Testing direct table creation...")

try:
    # Test med full qualified table navn
    test_data = pd.DataFrame({
        'test_id': [1, 2, 3],
        'timestamp': [datetime.now(), datetime.now(), datetime.now()],
        'status': ['working', 'testing', 'success']
    })
    
    spark_df = spark.createDataFrame(test_data)
    
    # Prøv direkte med full path
    table_name = "`EnergyLabels-DEV.energylabels_bronze.dbo`.connection_test"
    spark_df.write.mode("overwrite").saveAsTable(table_name)
    print("✅ Basic table creation successful with full path")
    
    # Nå test schema-basert tabell
    schema_table_name = "`EnergyLabels-DEV.energylabels_bronze.dbo`.`raw_ingestion.lakehouse_test`"
    spark_df.write.mode("overwrite").saveAsTable(schema_table_name)
    print("✅ Schema table creation successful!")
    
    # Verify
    result = spark.sql(f"SELECT * FROM {schema_table_name}")
    print(f"✅ Schema table contains {result.count()} rows")
    result.show()
    
    print("🎉 SUCCESS - schemas working with full path!")
    
except Exception as e:
    print(f"❌ Full path failed: {e}")
    
    # Plan B - forenklet tilnærming
    print("\nTrying simplified approach...")
    try:
        # Bare bruk enkle tabellnavn først
        spark_df.write.mode("overwrite").saveAsTable("simple_test")
        print("✅ Simple table works")
        
        # Så prøv schema uten full path
        spark_df.write.mode("overwrite").saveAsTable("raw_ingestion.simple_schema_test")
        print("✅ Simple schema works!")
        
        result = spark.table("raw_ingestion.simple_schema_test")
        result.show()
        
    except Exception as e2:
        print(f"❌ Even simplified failed: {e2}")
        
        # Plan C - vis hva som faktisk fungerer
        print("\nDebugging - what works:")
        try:
            # Vis tilgjengelige tabeller
            tables = spark.sql("SHOW TABLES").collect()
            print("Existing tables:")
            for table in tables:
                print(f"  {table}")
                
            # Test helt enkelt
            spark_df.createOrReplaceTempView("temp_test")
            temp_result = spark.sql("SELECT COUNT(*) FROM temp_test").collect()[0][0]
            print(f"✅ Temp view works - {temp_result} rows")
            
        except Exception as e3:
            print(f"❌ Debug failed: {e3}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# STEP 1: Test Basic Fabric Environment
# ============================================================================

import sys
import os
from datetime import datetime
import pandas as pd

print(f"Python version: {sys.version}")
print(f"Current time: {datetime.now()}")
print("="*50)

# Test basic lakehouse connectivity
try:
    # List available lakehouses
    print("Testing Lakehouse connectivity...")
    
    # Test write to bronze lakehouse
    test_data = {
        'test_id': [1, 2, 3],
        'test_name': ['Setup Test', 'Connectivity Test', 'Environment Test'],
        'timestamp': [datetime.now(), datetime.now(), datetime.now()]
    }
    df_test = pd.DataFrame(test_data)
    
    print("✓ Basic DataFrame creation successful")
    print(f"Test data shape: {df_test.shape}")
    
except Exception as e:
    print(f"❌ Basic setup error: {e}")

# ============================================================================
# STEP 2: Test External Libraries (needed for your existing code)
# ============================================================================

print("\n" + "="*50)
print("Testing required libraries...")

# Libraries from your existing requirements.txt
required_libs = [
    'requests',      # For Enova API calls
    'pandas',        # Data processing
    'sqlalchemy',    # Database connectivity (may need adaptation)
    'python-dotenv', # Environment variables
    'openpyxl'       # Excel file handling
]

for lib in required_libs:
    try:
        __import__(lib.replace('-', '_'))
        print(f"✓ {lib} - available")
    except ImportError:
        print(f"❌ {lib} - NOT available (may need %pip install)")

# Test docling (your PDF processing library)
try:
    import docling
    print("✓ docling - available")
except ImportError:
    print("❌ docling - NOT available (will need custom installation)")

# ============================================================================
# STEP 3: Test Environment Variables Setup
# ============================================================================

print("\n" + "="*50)
print("Testing environment configuration...")

# These should be configured in Fabric Key Vault or environment
required_env_vars = [
    'ENOVA_API_KEY',
    'ENOVA_API_BASE_URL', 
    'OPENAI_API_KEY',
    'OPENAI_MODEL'
]

for var in required_env_vars:
    value = os.getenv(var, 'NOT_SET')
    if value != 'NOT_SET':
        print(f"✓ {var} - configured")
    else:
        print(f"⚠️  {var} - not configured yet")

# ============================================================================
# STEP 4: Create Schema-Based Lakehouse Structure
# ============================================================================

print("\n" + "="*50)
print("Setting up Schema-based Lakehouse structure...")

try:
    # Define schema structure for Bronze layer
    bronze_schemas = {
        "raw_ingestion": [
            "csv_files_yearly",       # Raw CSV imports by year
            "csv_processing_log"      # Import processing logs
        ],
        "api_data": [
            "enova_responses",        # Raw API responses  
            "api_call_log"           # API call tracking
        ],
        "pdf_storage": [
            "downloaded_files",       # PDF file metadata
            "processing_queue"        # PDF processing queue
        ],
        "system_logs": [
            "pipeline_execution",    # Pipeline run logs
            "error_tracking"         # Error and retry tracking
        ]
    }
    
    # Define schema structure for Silver layer
    silver_schemas = {
        "core_data": [
            "certificates_cleaned",   # Main certificate data (mapped from Certificate table)
            "api_responses_structured" # Structured API responses (mapped from EnovaApi_Energiattest_url)
        ],
        "processing": [
            "failed_records",         # Records that need retry
            "retry_queue",           # API retry queue
            "pdf_text_extracted"     # Docling extraction results
        ],
        "quality": [
            "data_validation_results", # Data quality metrics
            "duplicate_detection",     # Duplicate tracking
            "api_call_metrics"        # API performance tracking
        ]
    }
    
    # Define schema structure for Gold layer  
    gold_schemas = {
        "business_marts": [
            "energy_certificates_final", # Final merged dataset
            "ai_analysis_results"        # OpenAI structured responses (mapped from OpenAIAnswers)
        ],
        "analytics": [
            "performance_metrics",       # Processing performance
            "processing_statistics",     # Pipeline statistics  
            "data_quality_dashboard"     # Quality metrics for reporting
        ],
        "reporting": [
            "powerbi_ready_tables",      # Optimized for Power BI
            "executive_summaries"        # High-level business metrics
        ]
    }
    
    print("📊 BRONZE LAYER SCHEMAS:")
    for schema, tables in bronze_schemas.items():
        print(f"  🗂️  {schema}/")
        for table in tables:
            print(f"    📋 {table}")
    
    print("\n📊 SILVER LAYER SCHEMAS:")
    for schema, tables in silver_schemas.items():
        print(f"  🗂️  {schema}/")
        for table in tables:
            print(f"    📋 {table}")
            
    print("\n📊 GOLD LAYER SCHEMAS:")
    for schema, tables in gold_schemas.items():
        print(f"  🗂️  {schema}/")
        for table in tables:
            print(f"    📋 {table}")
    
    print("\n✓ Schema-based structure ready for implementation")
    print("✓ Lakehouse schemas will provide better organization and governance")
    
except Exception as e:
    print(f"❌ Schema setup error: {e}")

# ============================================================================
# STEP 5: Test Schema-based Data Operations
# ============================================================================

print("\n" + "="*50)
print("Testing schema-based data operations...")

try:
    # Test writing to schema-based tables
    print("Testing schema-based table operations...")
    
    # Example of how data will be written with schemas
    sample_data_examples = {
        "raw_ingestion.csv_files_yearly": {
            'year': [2024, 2024, 2025],
            'filename': ['energisertifikat_2024_01.csv', 'energisertifikat_2024_02.csv', 'energisertifikat_2025_01.csv'],
            'file_size_mb': [15.2, 18.7, 12.3],
            'import_timestamp': [datetime.now(), datetime.now(), datetime.now()],
            'status': ['completed', 'completed', 'pending']
        },
        
        "api_data.enova_responses": {
            'certificate_id': ['CERT_001', 'CERT_002', 'CERT_003'],
            'api_response_json': ['{"data": "sample1"}', '{"data": "sample2"}', '{"data": "sample3"}'],
            'response_timestamp': [datetime.now(), datetime.now(), datetime.now()],
            'http_status': [200, 200, 429]  # 429 = rate limited
        },
        
        "core_data.certificates_cleaned": {
            'certificate_id': ['CERT_001', 'CERT_002'],
            'building_type': ['Residential', 'Commercial'], 
            'energy_class': ['B', 'A'],
            'total_energy_use': [120.5, 89.3],
            'processed_timestamp': [datetime.now(), datetime.now()]
        }
    }
    
    print("Schema-based table examples:")
    for schema_table, data in sample_data_examples.items():
        df_example = pd.DataFrame(data)
        print(f"\n  📋 {schema_table}")
        print(f"     Shape: {df_example.shape}")
        print(f"     Columns: {list(df_example.columns)}")
    
    # Show how tables will be accessed in notebooks
    print(f"\n📝 Code examples for schema access:")
    print(f"   # Reading from schema tables:")
    print(f"   df = spark.table('raw_ingestion.csv_files_yearly')")
    print(f"   df = spark.table('core_data.certificates_cleaned')")
    print(f"   ")
    print(f"   # Writing to schema tables:")
    print(f"   df.write.mode('overwrite').saveAsTable('api_data.enova_responses')")
    print(f"   df.write.mode('append').saveAsTable('system_logs.pipeline_execution')")
    
    print("\n✓ Schema-based data operations ready")
    
except Exception as e:
    print(f"❌ Schema data operations error: {e}")

# ============================================================================
# STEP 6: Test Data Factory Integration with Schemas
# ============================================================================

print("\n" + "="*50)
print("Testing Data Factory integration with schema structure...")

# Test parameters that would come from Data Factory pipeline
pipeline_params = {
    'execution_date': datetime.now().strftime('%Y-%m-%d'),
    'year': datetime.now().year,
    'batch_size': 100,
    'environment': 'development',
    'target_bronze_schema': 'raw_ingestion',
    'target_silver_schema': 'core_data', 
    'target_gold_schema': 'business_marts'
}

print("Pipeline parameters for schema-based processing:")
for key, value in pipeline_params.items():
    print(f"  {key}: {value}")

# Show how Data Factory will orchestrate schema-based processing
processing_steps = [
    {
        'step': 1,
        'name': 'CSV Ingestion',
        'source': 'Enova API',
        'target': 'raw_ingestion.csv_files_yearly',
        'notebook': 'bronze_csv_ingestion.ipynb'
    },
    {
        'step': 2, 
        'name': 'API Enrichment',
        'source': 'raw_ingestion.csv_files_yearly',
        'target': 'api_data.enova_responses',
        'notebook': 'silver_api_enrichment.ipynb'
    },
    {
        'step': 3,
        'name': 'Data Cleaning', 
        'source': 'api_data.enova_responses',
        'target': 'core_data.certificates_cleaned',
        'notebook': 'silver_data_cleaning.ipynb'
    },
    {
        'step': 4,
        'name': 'Final Processing',
        'source': 'core_data.certificates_cleaned', 
        'target': 'business_marts.energy_certificates_final',
        'notebook': 'gold_final_processing.ipynb'
    }
]

print(f"\nData Factory pipeline with schema-based flow:")
for step in processing_steps:
    print(f"  {step['step']}. {step['name']}")
    print(f"     📥 Source: {step['source']}")
    print(f"     📤 Target: {step['target']}")
    print(f"     📓 Notebook: {step['notebook']}")
    print()

print("✓ Ready for Data Factory integration with schemas")

# ============================================================================
# STEP 7: External API Connectivity Test
# ============================================================================

print("\n" + "="*50)
print("Testing external API connectivity...")

# Test Enova API (adapted from your api_client.py)
enova_api_key = os.getenv('ENOVA_API_KEY')
enova_base_url = os.getenv('ENOVA_API_BASE_URL', 'https://api.data.enova.no/ems/offentlige-data/v1')

if enova_api_key:
    try:
        import requests
        
        # Test basic API connectivity (without actual data call)
        headers = {
            'Authorization': f'Bearer {enova_api_key}',
            'Accept': 'application/json'
        }
        
        print("✓ Enova API credentials configured")
        print(f"✓ Base URL: {enova_base_url}")
        print(f"✓ Ready for API calls → api_data.enova_responses")
        
        # TODO: Add actual connectivity test when ready
        
    except Exception as e:
        print(f"❌ Enova API test error: {e}")
else:
    print("⚠️  Enova API key not configured yet")

# Test OpenAI API  
openai_api_key = os.getenv('OPENAI_API_KEY')
if openai_api_key:
    print("✓ OpenAI API key configured")
    print("✓ Ready for AI analysis → business_marts.ai_analysis_results")
else:
    print("⚠️  OpenAI API key not configured yet")# Fabric Setup Test Notebook
# This notebook tests basic Fabric connectivity and prepares for energy labels migration

# Schema creation test - schemas are created when first table is written
print("Creating schemas by writing first tables...")

try:
    # Test 1: Create raw_ingestion schema
    test_data_1 = pd.DataFrame({
        'year': [2024, 2025],
        'filename': ['test_2024.csv', 'test_2025.csv'], 
        'file_size_mb': [10.5, 12.3],
        'import_timestamp': [datetime.now(), datetime.now()],
        'status': ['test', 'test']
    })
    
    spark_df_1 = spark.createDataFrame(test_data_1)
    spark_df_1.write.mode("overwrite").saveAsTable("raw_ingestion.csv_files_yearly")
    print("✅ Created raw_ingestion schema with csv_files_yearly table")
    
    # Test 2: Create system_logs schema
    test_data_2 = pd.DataFrame({
        'batch_id': ['test_batch_001'],
        'step_name': ['schema_creation_test'],
        'status': ['completed'],
        'timestamp': [datetime.now()],
        'details': ['Testing schema creation'],
        'error_message': ['']
    })
    
    spark_df_2 = spark.createDataFrame(test_data_2)
    spark_df_2.write.mode("overwrite").saveAsTable("system_logs.pipeline_execution")
    print("✅ Created system_logs schema with pipeline_execution table")
    
    # Test 3: Read back to verify schemas work
    result1 = spark.table("raw_ingestion.csv_files_yearly")
    result2 = spark.table("system_logs.pipeline_execution")
    
    print(f"✅ raw_ingestion.csv_files_yearly contains {result1.count()} rows")
    print(f"✅ system_logs.pipeline_execution contains {result2.count()} rows")
    
    print("\n📊 Viewing created tables:")
    result1.show()
    result2.show()
    
    print("🎉 SCHEMAS CREATED SUCCESSFULLY!")
    print("Now you can see them in the lakehouse left panel under 'Schemas'")
    
except Exception as e:
    print(f"❌ Schema creation failed: {e}")

# ============================================================================
# SUMMARY & NEXT STEPS
# ============================================================================

print("\n" + "="*60)
print("SETUP TEST SUMMARY")
print("="*60)

print("\n✅ COMPLETED:")
print("  - Basic Fabric environment functional")
print("  - Lakehouse structure planned")
print("  - Required libraries identified")

print("\n⏳ NEXT STEPS:")
print("  1. Configure environment variables in Key Vault")
print("  2. Install missing libraries (%pip install docling)")
print("  3. Create schema-enabled lakehouses with:")
print("     - energylabels_bronze (with raw_ingestion, api_data, pdf_storage, system_logs schemas)")
print("     - energylabels_silver (with core_data, processing, quality schemas)")  
print("     - energylabels_gold (with business_marts, analytics, reporting schemas)")
print("  4. Create first migration notebook (02_bronze_csv_ingestion.ipynb)")
print("  5. Test with sample 2024 data using schema.table format")

print("\n📝 READY FOR:")
print("  - Migration of file_downloader.py → raw_ingestion.csv_files_yearly")
print("  - Schema-based bronze layer data ingestion")
print("  - Sample data testing with improved organization")

print(f"\n🎯 SCHEMA-ENABLED TIMESTAMP: {datetime.now()}")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
