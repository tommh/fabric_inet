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

# Test schema creation in Fabric Lakehouse
print("Testing schema creation...")

try:
    # Create multiple schemas
    schemas_to_create = ["raw_ingestion", "api_data", "system_logs"]
    
    for schema_name in schemas_to_create:
        print(f"Creating schema: {schema_name}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"✅ Schema '{schema_name}' created")
    
    # Verify schemas exist
    print("\n📊 All schemas in lakehouse:")
    schemas = spark.sql("SHOW SCHEMAS")
    schemas.show()
    
    # Test table creation in each schema
    test_data = pd.DataFrame({
        'test_id': [1, 2],
        'test_name': ['schema_test_1', 'schema_test_2'],
        'created_at': [datetime.now(), datetime.now()]
    })
    
    spark_df = spark.createDataFrame(test_data)
    
    # Create test table in each schema
    for schema_name in schemas_to_create:
        table_name = f"{schema_name}.test_table"
        spark_df.write.mode("overwrite").saveAsTable(table_name)
        print(f"✅ Created table: {table_name}")
    
    # Verify tables
    print("\n📋 Testing table reads:")
    for schema_name in schemas_to_create:
        result = spark.table(f"{schema_name}.test_table")
        count = result.count()
        print(f"✅ {schema_name}.test_table: {count} rows")
    
    print("\n🎉 ALL SCHEMAS AND TABLES CREATED SUCCESSFULLY!")
    print("Ready for full Bronze/Silver/Gold architecture!")
    
except Exception as e:
    print(f"❌ Schema creation failed: {e}")
    
    # Fallback test - show what currently exists
    try:
        print("\nFallback - showing current database structure:")
        current_tables = spark.sql("SHOW TABLES")
        current_tables.show()
    except Exception as e2:
        print(f"❌ Even fallback failed: {e2}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM energylabels_bronze.raw_ingestion.test_table LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test table creation in manually created "test" schema
print("Testing table creation in 'test' schema...")

try:
    # Create simple test data
    test_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'value': [10.5, 20.3, 30.1, 40.8, 50.2],
        'timestamp': [datetime.now() for _ in range(5)]
    })
    
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(test_data)
    
    # Write to test schema
    spark_df.write.mode("overwrite").saveAsTable("test.sample_table")
    
    print("✅ Table created in test schema!")
    
    # Read back to verify
    result = spark.table("test.sample_table")
    print(f"✅ Table contains {result.count()} rows")
    
    # Show the data
    print("📋 Data in test.sample_table:")
    result.show()
    
    print("🎉 SUCCESS! Schema-based tables are working!")
    
except Exception as e:
    print(f"❌ Failed: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test lakehouse connection (denne skal fungere fra lakehouse-notebook)
import pandas as pd
from datetime import datetime

print("Testing from lakehouse-attached notebook...")

test_df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['test1', 'test2', 'test3'],
    'timestamp': [datetime.now(), datetime.now(), datetime.now()]
})

spark_test = spark.createDataFrame(test_df)
spark_test.write.mode("overwrite").saveAsTable("lakehouse_connection_test")

print("✅ SUCCESS! Lakehouse attachment working!")

result = spark.table("lakehouse_connection_test")
result.show()

# Test schema creation now that lakehouse is attached
print("\nTesting schema creation...")

try:
    # Test schema-based table
    schema_df = pd.DataFrame({
        'year': [2024, 2025],
        'filename': ['energi_2024.csv', 'energi_2025.csv'],
        'status': ['completed', 'pending'],
        'timestamp': [datetime.now(), datetime.now()]
    })
    
    spark_schema = spark.createDataFrame(schema_df)
    spark_schema.write.mode("overwrite").saveAsTable("raw_ingestion.csv_processing_log")
    
    print("✅ Schema table created: raw_ingestion.csv_processing_log")
    
    # Verify we can read it
    result = spark.table("raw_ingestion.csv_processing_log")
    result.show()
    
    print("🎉 SCHEMAS ARE WORKING!")
    print("Check your lakehouse left panel for 'Schemas' section")
    
except Exception as e:
    print(f"Schema test: {e}")
    print("Schemas might need different syntax, but basic tables work")


# FULL CSV MIGRATION - Without schemas for now
print("\n" + "="*60)
print("STARTING FULL CSV MIGRATION")
print("="*60)

import requests
from datetime import datetime
import os
import hashlib

# Configuration
ENOVA_API_KEY = os.getenv('ENOVA_API_KEY', 'demo-key')
ENOVA_BASE_URL = 'https://api.data.enova.no/ems/offentlige-data/v1'
target_year = 2024
batch_id = f"csv_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

print(f"Target year: {target_year}")
print(f"Batch ID: {batch_id}")

# CSV Processing Function (adapted from your file_downloader.py)
def process_csv_data(year, use_mock_data=True):
    """Process CSV data - mock or real API"""
    
    print(f"\n📥 Processing CSV for year {year}")
    
    if use_mock_data:
        print("📝 Creating realistic mock energy certificate data...")
        
        # Create realistic mock data based on Norwegian energy certificates
        import random
        random.seed(42)  # Reproducible results
        
        mock_data = {
            'bygningsnummer': [f'BYG{i:08d}' for i in range(1, 151)],  # 150 certificates
            'energiklasse': random.choices(['A', 'B', 'C', 'D', 'E', 'F', 'G'], 
                                         weights=[10, 15, 20, 25, 15, 10, 5], k=150),
            'totalt_energibehov_kwh_m2_aar': [round(50 + random.uniform(-20, 150), 1) for _ in range(150)],
            'bygningstype': random.choices(['Enebolig', 'Boligblokk', 'Kontor', 'Skole', 'Butikk'], 
                                         weights=[40, 30, 15, 10, 5], k=150),
            'oppvarmet_areal': [random.randint(80, 500) for _ in range(150)],
            'byggeaar': [random.randint(1950, 2023) for _ in range(150)],
            'postnummer': [f'{random.randint(1000, 9999)}' for _ in range(150)],
            'kommunenummer': [f'{random.randint(101, 5054):04d}' for _ in range(150)],
            'energiattest_id': [f'CERT_{year}_{i:06d}' for i in range(1, 151)]
        }
        
        csv_df = pd.DataFrame(mock_data)
        file_size = len(csv_df.to_csv().encode('utf-8'))
        
        print(f"✅ Mock data: {len(csv_df)} certificates, {file_size:,} bytes")
        
    else:
        # Real API call (your existing logic)
        print("📡 Calling Enova API...")
        # ... real API implementation
        
    return csv_df, file_size

# Execute CSV processing
csv_data, file_size = process_csv_data(target_year, use_mock_data=True)

# Add processing metadata
csv_data['import_batch_id'] = batch_id
csv_data['import_timestamp'] = datetime.now()
csv_data['source_year'] = target_year

# Store in lakehouse - BRONZE LAYER
print("\n💾 Storing in Bronze layer...")

spark_csv = spark.createDataFrame(csv_data)
spark_csv.write.mode("overwrite").saveAsTable(f"bronze_certificates_{target_year}")

print(f"✅ Stored: bronze_certificates_{target_year}")
print(f"   Rows: {len(csv_data):,}")
print(f"   Columns: {len(csv_data.columns)}")

# Create processing log - METADATA TABLE
print("\n📊 Creating processing log...")

processing_log = pd.DataFrame({
    'batch_id': [batch_id],
    'processing_year': [target_year],
    'table_name': [f'bronze_certificates_{target_year}'],
    'file_size_bytes': [file_size],
    'csv_rows': [len(csv_data)],
    'csv_columns': [len(csv_data.columns)],
    'processing_timestamp': [datetime.now()],
    'status': ['completed'],
    'data_type': ['mock_data'],
    'next_step': ['ready_for_api_enrichment']
})

spark_log = spark.createDataFrame(processing_log)
spark_log.write.mode("append").saveAsTable("bronze_processing_log")

print("✅ Processing log stored")

# Show results
print(f"\n📊 PROCESSING SUMMARY:")
print(f"✅ Bronze table: bronze_certificates_{target_year} ({len(csv_data):,} rows)")
print(f"✅ Processing log: bronze_processing_log")

# Display sample data
print(f"\n📋 Sample certificates:")
csv_data.head().to_string(index=False)    


# CONTINUE FROM WHERE IT WORKED - Add this to the working notebook
print("\n" + "="*60) 
print("FULL CSV MIGRATION - Building on working foundation")
print("="*60)

import random
from datetime import datetime
import os

# Configuration
target_year = 2024
batch_id = f"csv_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

print(f"Target year: {target_year}")
print(f"Batch ID: {batch_id}")

# Create realistic mock energy certificate data
print("\n📝 Creating mock energy certificate data...")

random.seed(42)  # Reproducible results

mock_certificates = {
    'bygningsnummer': [f'BYG{i:08d}' for i in range(1, 101)],  # 100 certificates
    'energiklasse': random.choices(['A', 'B', 'C', 'D', 'E', 'F', 'G'], 
                                 weights=[10, 15, 20, 25, 15, 10, 5], k=100),
    'totalt_energibehov_kwh_m2_aar': [round(50 + random.uniform(-20, 150), 1) for _ in range(100)],
    'bygningstype': random.choices(['Enebolig', 'Boligblokk', 'Kontor', 'Skole', 'Butikk'], 
                                 weights=[40, 30, 15, 10, 5], k=100),
    'oppvarmet_areal': [random.randint(80, 500) for _ in range(100)],
    'byggeaar': [random.randint(1950, 2023) for _ in range(100)],
    'postnummer': [f'{random.randint(1000, 9999)}' for _ in range(100)],
    'kommunenummer': [f'{random.randint(101, 5054):04d}' for _ in range(100)],
    'energiattest_id': [f'CERT_{target_year}_{i:06d}' for i in range(1, 101)],
    'adresse': [f'Testveien {i}' for i in range(1, 101)],
    'bygningskategori': random.choices(['Bolig', 'Kontor', 'Skole', 'Handel'], k=100)
}

# Convert to DataFrame
csv_df = pd.DataFrame(mock_certificates)

# Add processing metadata
csv_df['import_batch_id'] = batch_id
csv_df['import_timestamp'] = datetime.now()
csv_df['source_year'] = target_year
csv_df['data_source'] = 'mock_enova_api'

print(f"✅ Created {len(csv_df)} mock certificates")
print(f"   Columns: {list(csv_df.columns)[:5]}... (showing first 5)")

# Store in bronze layer
print("\n💾 Storing in Bronze layer...")

spark_csv = spark.createDataFrame(csv_df)
spark_csv.write.mode("overwrite").saveAsTable(f"bronze_certificates_{target_year}")

print(f"✅ Stored: bronze_certificates_{target_year}")

# Create processing log
processing_log = pd.DataFrame({
    'batch_id': [batch_id],
    'processing_year': [target_year],
    'table_name': [f'bronze_certificates_{target_year}'],
    'csv_rows': [len(csv_df)],
    'csv_columns': [len(csv_df.columns)],
    'processing_timestamp': [datetime.now()],
    'status': ['completed'],
    'data_type': ['mock_data'],
    'next_step': ['ready_for_api_enrichment']
})

spark_log = spark.createDataFrame(processing_log)
spark_log.write.mode("append").saveAsTable("bronze_processing_log")

print("✅ Processing log stored")

# Verify what we created
print(f"\n📊 VERIFICATION:")
certificates = spark.table(f"bronze_certificates_{target_year}")
log = spark.table("bronze_processing_log")

print(f"✅ bronze_certificates_{target_year}: {certificates.count()} rows")
print(f"✅ bronze_processing_log: {log.count()} rows")

# Show sample data
print(f"\n📋 Sample certificates (first 5 rows):")
certificates.show(5)

print(f"\n📋 Processing log:")
log.show()

print(f"\n🎉 BRONZE LAYER MIGRATION COMPLETED!")
print(f"Ready for Silver layer (API enrichment)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
