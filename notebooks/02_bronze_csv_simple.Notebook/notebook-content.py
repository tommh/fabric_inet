# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Bronze CSV Migration - Simplified without schemas
# Get the system working first, optimize later

# ============================================================================
# IMPORTS AND SETUP
# ============================================================================

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import time
from io import StringIO
import hashlib

print("🚀 Bronze CSV Migration - Simplified Approach")
print(f"Execution time: {datetime.now()}")
print("="*60)

# Configuration
ENOVA_API_KEY = os.getenv('ENOVA_API_KEY', 'demo-key')
ENOVA_BASE_URL = os.getenv('ENOVA_API_BASE_URL', 'https://api.data.enova.no/ems/offentlige-data/v1')

# Pipeline parameters  
execution_date = datetime.now()
target_year = 2024  # Start with 2024 for testing
batch_id = f"csv_batch_{execution_date.strftime('%Y%m%d_%H%M%S')}"

print(f"Target year: {target_year}")
print(f"Batch ID: {batch_id}")

# ============================================================================
# TEST BASIC FUNCTIONALITY FIRST
# ============================================================================

print("\nTesting basic Spark functionality...")

try:
    # Test 1: Simple DataFrame creation and table write
    test_df = pd.DataFrame({
        'test_id': [1, 2, 3],
        'test_name': ['basic', 'spark', 'test'],
        'timestamp': [datetime.now(), datetime.now(), datetime.now()]
    })
    
    spark_test = spark.createDataFrame(test_df)
    spark_test.write.mode("overwrite").saveAsTable("basic_functionality_test")
    
    print("✅ Basic table write successful")
    
    # Test 2: Read back
    result = spark.table("basic_functionality_test")
    count = result.count()
    print(f"✅ Basic table read successful - {count} rows")
    
    # Test 3: Show current database
    current_db = spark.sql("SELECT current_database()").collect()[0][0]
    print(f"✅ Current database: {current_db}")
    
    print("🎉 Basic Spark functionality is working!")
    
except Exception as e:
    print(f"❌ Basic functionality failed: {e}")
    print("Cannot proceed without basic Spark working")
    exit()

# ============================================================================
# CSV PROCESSING LOGIC (adapted from your file_downloader.py)
# ============================================================================

def download_and_process_csv(year, use_mock_data=True):
    """
    Download CSV from Enova API and process
    Uses mock data if API key not configured
    """
    
    print(f"\n📥 Processing CSV for year {year}")
    
    try:
        if use_mock_data or ENOVA_API_KEY == 'demo-key':
            # Create mock CSV data for testing
            print("📝 Creating mock CSV data for testing...")
            
            mock_csv_data = {
                'bygningsnummer': [f'BYG_{i:06d}' for i in range(1, 101)],
                'energiklasse': ['A', 'B', 'C', 'D', 'E', 'F', 'G'] * 14 + ['A', 'B', 'C'],
                'totalt_energibehov_kwh_m2_aar': [50 + i*2 for i in range(100)],
                'bygningstype': ['Boligblokk', 'Enebolig', 'Kontor', 'Skole', 'Sykehus'] * 20,
                'oppvarmet_areal': [100 + i*5 for i in range(100)],
                'byggeaar': [1950 + (i % 70) for i in range(100)],
                'postnummer': [f'{1000 + i}' for i in range(100)],
                'kommunenummer': [f'{300 + (i % 50)}' for i in range(100)]
            }
            
            csv_df = pd.DataFrame(mock_csv_data)
            csv_content = csv_df.to_csv(index=False)
            file_size = len(csv_content.encode('utf-8'))
            
            print(f"✅ Mock data created: {len(csv_df)} rows, {file_size:,} bytes")
            
        else:
            # Real API call (your existing logic)
            print("📡 Calling Enova API...")
            
            url = f"{ENOVA_BASE_URL}/energiattest/csv/{year}"
            headers = {
                'Authorization': f'Bearer {ENOVA_API_KEY}',
                'Accept': 'text/csv'
            }
            
            response = requests.get(url, headers=headers, timeout=300)
            
            if response.status_code == 200:
                csv_content = response.text
                csv_df = pd.read_csv(StringIO(csv_content))
                file_size = len(csv_content.encode('utf-8'))
                print(f"✅ API download successful: {len(csv_df)} rows")
            else:
                raise Exception(f"API error: {response.status_code}")
        
        # ============================================================================
        # STORE RAW CSV DATA
        # ============================================================================
        
        print("💾 Storing raw CSV data...")
        
        # Add metadata columns
        csv_df['import_batch_id'] = batch_id
        csv_df['import_timestamp'] = datetime.now()
        csv_df['source_year'] = year
        csv_df['data_source'] = 'enova_api'
        
        # Store in lakehouse (simple table name for now)
        spark_csv = spark.createDataFrame(csv_df)
        spark_csv.write.mode("overwrite").saveAsTable(f"bronze_csv_data_{year}")
        
        print(f"✅ Stored raw data in: bronze_csv_data_{year}")
        print(f"   Rows: {len(csv_df):,}")
        print(f"   Columns: {len(csv_df.columns)}")
        
        # ============================================================================
        # LOG PROCESSING METADATA  
        # ============================================================================
        
        print("📊 Logging processing metadata...")
        
        processing_log = pd.DataFrame({
            'batch_id': [batch_id],
            'processing_year': [year], 
            'table_name': [f'bronze_csv_data_{year}'],
            'file_size_bytes': [file_size],
            'file_size_mb': [round(file_size/1024/1024, 2)],
            'csv_rows': [len(csv_df)],
            'csv_columns': [len(csv_df.columns)],
            'column_names': [str(list(csv_df.columns)[:10])],  # First 10 columns
            'processing_timestamp': [datetime.now()],
            'status': ['completed'],
            'data_type': ['mock' if use_mock_data else 'real'],
            'next_step': ['ready_for_api_enrichment']
        })
        
        spark_log = spark.createDataFrame(processing_log)
        spark_log.write.mode("append").saveAsTable("bronze_processing_log")
        
        print("✅ Processing metadata logged")
        
        # ============================================================================
        # BASIC DATA QUALITY CHECKS
        # ============================================================================
        
        print("🔍 Running basic data quality checks...")
        
        quality_checks = []
        
        # Check for required columns (adapt to your data)
        required_columns = ['bygningsnummer', 'energiklasse', 'totalt_energibehov_kwh_m2_aar']
        for col in required_columns:
            if col in csv_df.columns:
                null_count = csv_df[col].isnull().sum()
                quality_checks.append({
                    'batch_id': batch_id,
                    'check_type': 'null_check',
                    'column_name': col,
                    'total_rows': len(csv_df),
                    'null_count': int(null_count),
                    'null_percentage': round(null_count/len(csv_df)*100, 2),
                    'check_timestamp': datetime.now(),
                    'status': 'passed' if null_count < len(csv_df)*0.1 else 'warning'
                })
        
        # Store quality checks
        if quality_checks:
            quality_df = pd.DataFrame(quality_checks)
            spark_quality = spark.createDataFrame(quality_df)
            spark_quality.write.mode("append").saveAsTable("bronze_data_quality")
            print(f"✅ Data quality checks completed: {len(quality_checks)} checks")
        
        return True
        
    except Exception as e:
        print(f"❌ CSV processing failed: {e}")
        
        # Log error
        error_log = pd.DataFrame({
            'batch_id': [batch_id],
            'processing_year': [year],
            'error_timestamp': [datetime.now()],
            'error_type': [type(e).__name__],
            'error_message': [str(e)[:500]],
            'status': ['failed']
        })
        
        spark_error = spark.createDataFrame(error_log)
        spark_error.write.mode("append").saveAsTable("bronze_error_log")
        
        return False

# ============================================================================
# MAIN EXECUTION
# ============================================================================

print("\n" + "="*60)
print("EXECUTING CSV PROCESSING")
print("="*60)

# Process the target year
success = download_and_process_csv(target_year, use_mock_data=True)

if success:
    print(f"\n✅ SUCCESS! CSV processing completed for {target_year}")
    
    # Show what was created
    print(f"\n📊 Tables created:")
    try:
        tables = spark.sql("SHOW TABLES").collect()
        for table in tables:
            if 'bronze' in table[1]:  # table[1] is table name
                row_count = spark.table(table[1]).count()
                print(f"   📋 {table[1]}: {row_count:,} rows")
    except:
        print("   (Could not display table summary)")
    
    print(f"\n🎯 Next steps:")
    print("   1. Verify data in bronze tables")  
    print("   2. Create silver layer processing")
    print("   3. Setup API enrichment")
    
else:
    print(f"\n❌ FAILED! CSV processing failed for {target_year}")

print(f"\nBatch {batch_id} completed at {datetime.now()}")
print("="*60)

# Test proper Fabric schema syntax
print("Testing correct Fabric schema syntax...")

try:
    # Method 1: Create schema explicitly first
    spark.sql("CREATE SCHEMA IF NOT EXISTS raw_ingestion")
    print("✅ Schema 'raw_ingestion' created")
    
    # Then create table in schema
    schema_df = pd.DataFrame({
        'year': [2024, 2025],
        'filename': ['energi_2024.csv', 'energi_2025.csv'],
        'status': ['completed', 'pending'],
        'timestamp': [datetime.now(), datetime.now()]
    })
    
    spark_schema = spark.createDataFrame(schema_df)
    spark_schema.write.mode("overwrite").saveAsTable("raw_ingestion.csv_processing_log")
    
    print("✅ Table created in schema: raw_ingestion.csv_processing_log")
    
    # Verify
    result = spark.table("raw_ingestion.csv_processing_log")
    result.show()
    
    print("🎉 SCHEMAS NOW WORKING WITH EXPLICIT CREATION!")
    
except Exception as e:
    print(f"Method 1 failed: {e}")
    
    # Method 2: Try alternative schema creation
    try:
        print("\nTrying alternative schema approach...")
        
        # Use three-part naming
        spark.sql("CREATE SCHEMA IF NOT EXISTS energylabels_bronze.raw_ingestion")
        
        spark_schema.write.mode("overwrite").saveAsTable("energylabels_bronze.raw_ingestion.csv_processing_log")
        
        result2 = spark.table("energylabels_bronze.raw_ingestion.csv_processing_log")
        result2.show()
        
        print("✅ Alternative schema method worked!")
        
    except Exception as e2:
        print(f"Method 2 also failed: {e2}")
        print("Will proceed with simple tables for now")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
