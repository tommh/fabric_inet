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

"""
Microsoft Fabric CSV Processor for Norwegian Energy Certificate Data
Handles importing CSV files from OneLake into Delta tables
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FabricCSVProcessor:
    """Service for processing and importing CSV files to Fabric Delta tables"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session if spark_session else SparkSession.getActiveSession()
        if not self.spark:
            raise ValueError("No active Spark session found")
        
        # OneLake paths
        self.base_path = "abfss://19badba5-76c2-44f9-ac2e-2b153bb9c055@onelake.dfs.fabric.microsoft.com/de850aba-df79-4435-9f6c-8d936c425c62/Files/raw_data"
        self.table_name = "energylabels_bronze.raw_ingestion.EnovaApi_ImpHist"
        self.table_path = "Tables/energylabels_bronze/raw_ingestion/EnovaApi_ImpHist"
        
        # Define the schema matching your destination table
        self.schema = StructType([
            StructField("ImpHist_ID", LongType(), True),
            StructField("Knr", IntegerType(), True),
            StructField("Gnr", IntegerType(), True),
            StructField("Bnr", IntegerType(), True),
            StructField("Snr", IntegerType(), True),
            StructField("Fnr", IntegerType(), True),
            StructField("Andelsnummer", LongType(), True),
            StructField("Bygningsnummer", LongType(), True),
            StructField("GateAdresse", StringType(), True),
            StructField("Postnummer", IntegerType(), True),
            StructField("Poststed", StringType(), True),
            StructField("BruksEnhetsNummer", StringType(), True),
            StructField("Organisasjonsnummer", LongType(), True),
            StructField("Bygningskategori", StringType(), True),
            StructField("Byggear", IntegerType(), True),
            StructField("Energikarakter", StringType(), True),
            StructField("Oppvarmingskarakter", StringType(), True),
            StructField("Utstedelsesdato", TimestampType(), True),
            StructField("TypeRegistrering", StringType(), True),
            StructField("Attestnummer", StringType(), False),  # NOT NULL
            StructField("BeregnetLevertEnergiTotaltkWhm2", StringType(), True),
            StructField("BeregnetFossilandel", StringType(), True),
            StructField("Materialvalg", StringType(), True),
            StructField("HarEnergiVurdering", StringType(), True),
            StructField("EnergiVurderingDato", TimestampType(), True),
            StructField("ImportDato", TimestampType(), True)
        ])
    
    def get_csv_file_path(self, year: int) -> str:
        """Generate the full path for a CSV file by year"""
        return f"{self.base_path}/enova_data_{year}.csv"
    
    def check_file_exists(self, file_path: str) -> bool:
        """Check if a file exists in OneLake"""
        try:
            # Try to read just the header to check if file exists
            df_test = self.spark.read.option("header", "true").csv(file_path).limit(1)
            df_test.count()  # This will trigger the read operation
            return True
        except Exception as e:
            logger.warning(f"File does not exist or is not accessible: {file_path} - {str(e)}")
            return False
    
    def analyze_csv_structure(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze CSV file structure to understand columns and data
        """
        try:
            logger.info(f"Analyzing CSV structure: {file_path}")
            
            # Read a sample with automatic schema inference
            df_sample = (self.spark.read
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("sep", ",")  # Start with comma, can be adjusted
                        .csv(file_path)
                        .limit(5))
            
            columns = df_sample.columns
            total_rows = (self.spark.read
                         .option("header", "true")
                         .csv(file_path)
                         .count())
            
            # Get sample data
            sample_data = df_sample.collect()
            
            result = {
                'columns': columns,
                'total_columns': len(columns),
                'total_rows': total_rows,
                'sample_data': [row.asDict() for row in sample_data],
                'file_path': file_path
            }
            
            logger.info(f"CSV Analysis: {result['total_columns']} columns, "
                       f"{result['total_rows']} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing CSV structure: {str(e)}")
            raise
    
    def read_and_clean_csv(self, file_path: str) -> DataFrame:
        """
        Read CSV file and apply data cleaning transformations
        """
        try:
            logger.info(f"Reading and cleaning CSV: {file_path}")
            
            # Read CSV with string schema first to handle all data cleaning
            df = (self.spark.read
                 .option("header", "true")
                 .option("sep", ",")
                 .option("quote", '"')
                 .option("escape", '"')
                 .option("multiline", "true")
                 .csv(file_path))
            
            logger.info(f"Initial read: {df.count()} rows")
            
            # Data cleaning transformations
            cleaned_df = df
            
            # Clean numeric fields - convert empty strings to nulls, then cast to appropriate types
            numeric_fields = {
                'Knr': 'int', 'Gnr': 'int', 'Bnr': 'int', 'Snr': 'int', 'Fnr': 'int',
                'Andelsnummer': 'long', 'Bygningsnummer': 'long', 'Postnummer': 'int',
                'Organisasjonsnummer': 'long', 'Byggear': 'int'
            }
            
            for field, data_type in numeric_fields.items():
                if field in cleaned_df.columns:
                    cleaned_df = cleaned_df.withColumn(
                        field,
                        when(col(field).rlike(r'^\s*$'), None)
                        .otherwise(col(field).cast(data_type))
                    )
            
            # Clean datetime fields
            datetime_fields = ['Utstedelsesdato', 'EnergiVurderingDato']
            
            for field in datetime_fields:
                if field in cleaned_df.columns:
                    cleaned_df = cleaned_df.withColumn(
                        field,
                        when(col(field).rlike(r'^\s*$'), None)
                        .otherwise(to_timestamp(col(field)))
                    )
            
            # Clean string fields - trim whitespace and convert empty strings to nulls
            string_fields = [
                'GateAdresse', 'Poststed', 'BruksEnhetsNummer', 'Bygningskategori',
                'Energikarakter', 'Oppvarmingskarakter', 'TypeRegistrering', 
                'Attestnummer', 'BeregnetLevertEnergiTotaltkWhm2', 'BeregnetFossilandel',
                'Materialvalg', 'HarEnergiVurdering'
            ]
            
            for field in string_fields:
                if field in cleaned_df.columns:
                    cleaned_df = cleaned_df.withColumn(
                        field,
                        when(col(field).rlike(r'^\s*$'), None)
                        .otherwise(trim(col(field)))
                    )
            
            # Special handling for BeregnetFossilandel - convert comma decimal to dot
            if 'BeregnetFossilandel' in cleaned_df.columns:
                cleaned_df = cleaned_df.withColumn(
                    'BeregnetFossilandel',
                    when(col('BeregnetFossilandel').isNotNull(),
                         regexp_replace(col('BeregnetFossilandel'), ',', '.'))
                    .otherwise(col('BeregnetFossilandel'))
                )
            
            # Add ImportDato timestamp
            cleaned_df = cleaned_df.withColumn('ImportDato', current_timestamp())
            
            # Add ImpHist_ID - get the maximum existing ID first
            max_id = 0
            try:
                existing_df = self.spark.table(self.table_name)
                max_id_row = existing_df.agg(max(col('ImpHist_ID')).alias('max_id')).collect()
                max_id = max_id_row[0]['max_id'] if max_id_row[0]['max_id'] else 0
                logger.info(f"Found maximum existing ImpHist_ID: {max_id}")
            except Exception as e:
                logger.info(f"Could not get max ID (table may not exist): {e}")
                max_id = 0
            
            # Add sequential IDs with more deterministic ordering
            from pyspark.sql.window import Window
            # Force into single partition for global row numbering, then repartition
            window_spec = Window.orderBy('Attestnummer', 'ImportDato', monotonically_increasing_id())
            df_with_row_num = cleaned_df.coalesce(1).withColumn('row_num', row_number().over(window_spec))
            cleaned_df = df_with_row_num.withColumn('ImpHist_ID', (col('row_num') + max_id).cast('long')).drop('row_num')
            # Repartition back for better performance
            cleaned_df = cleaned_df.repartition(4)
            
            # Remove rows where all important fields are null
            cleaned_df = cleaned_df.filter(
                col('Attestnummer').isNotNull() | 
                col('GateAdresse').isNotNull() |
                col('Postnummer').isNotNull()
            )
            
            # Remove internal duplicates based on Attestnummer
            if 'Attestnummer' in cleaned_df.columns:
                initial_count = cleaned_df.count()
                cleaned_df = cleaned_df.dropDuplicates(['Attestnummer'])
                final_count = cleaned_df.count()
                if initial_count != final_count:
                    logger.info(f"Removed {initial_count - final_count} internal duplicates from CSV")
            
            # Select only the columns that exist in our target schema
            target_columns = [field.name for field in self.schema.fields]
            available_columns = [col_name for col_name in target_columns if col_name in cleaned_df.columns]
            
            # Add missing columns as nulls
            for col_name in target_columns:
                if col_name not in cleaned_df.columns:
                    # Get the data type from schema
                    data_type = next(field.dataType for field in self.schema.fields if field.name == col_name)
                    cleaned_df = cleaned_df.withColumn(col_name, lit(None).cast(data_type))
            
            # Select columns in schema order (excluding ImpHist_ID which is auto-generated)
            final_df = cleaned_df.select(*target_columns)
            
            logger.info(f"Data cleaned: {final_df.count()} rows ready for import")
            return final_df
            
        except Exception as e:
            logger.error(f"Error reading and cleaning CSV: {str(e)}")
            raise
    
    def check_existing_records(self, df: DataFrame) -> Dict[str, Any]:
        """
        Check which records already exist in the Delta table based on Attestnummer
        """
        try:
            if 'Attestnummer' not in df.columns:
                logger.warning("No Attestnummer column found - cannot check for duplicates")
                return {
                    'existing_count': 0,
                    'new_count': df.count(),
                    'new_records_df': df
                }
            
            logger.info("Checking for existing records in Delta table...")
            
            # Check if the target table exists
            try:
                # Try using the catalog to check table existence
                table_exists = False
                try:
                    table_exists = self.spark.catalog.tableExists(self.table_name)
                except Exception as catalog_error:
                    logger.info(f"Catalog check failed: {catalog_error}")
                
                if table_exists:
                    existing_table = DeltaTable.forName(self.spark, self.table_name)
                    existing_df = existing_table.toDF()
                    
                    # Get existing Attestnummer values
                    existing_attestnummer = existing_df.select('Attestnummer').distinct()
                    
                    # Find new records (those not in existing table)
                    new_records_df = df.join(
                        existing_attestnummer,
                        df.Attestnummer == existing_attestnummer.Attestnummer,
                        'left_anti'  # Left anti join to get records that don't exist in the right table
                    )
                    
                    existing_count = df.count() - new_records_df.count()
                    new_count = new_records_df.count()
                    
                    result = {
                        'existing_count': existing_count,
                        'new_count': new_count,
                        'new_records_df': new_records_df
                    }
                    
                    logger.info(f"Duplicate check completed: {existing_count} existing, {new_count} new records")
                    
                    return result
                else:
                    logger.info("Table does not exist yet - all records are new")
                    return {
                        'existing_count': 0,
                        'new_count': df.count(),
                        'new_records_df': df
                    }
                
            except Exception as table_error:
                logger.warning(f"Could not check table existence: {str(table_error)}")
                # If table doesn't exist, all records are new
                return {
                    'existing_count': 0,
                    'new_count': df.count(),
                    'new_records_df': df
                }
                
        except Exception as e:
            logger.error(f"Error checking existing records: {str(e)}")
            # If check fails, return all records as new to avoid data loss
            return {
                'existing_count': 0,
                'new_count': df.count(),
                'new_records_df': df,
                'check_error': str(e)
            }
    
    def insert_to_delta_table(self, df: DataFrame, skip_duplicates: bool = True) -> Dict[str, Any]:
        """
        Insert DataFrame to Delta table using merge operation
        """
        try:
            total_rows = df.count()
            
            if total_rows == 0:
                logger.info("No records to insert")
                return {
                    'success': True,
                    'total_rows': 0,
                    'inserted_rows': 0,
                    'skipped_rows': 0,
                    'message': 'No records to process'
                }
            
            logger.info(f"Starting Delta table insert: {total_rows} rows")
            
            # Check for existing records if skip_duplicates is True
            if skip_duplicates:
                duplicate_check = self.check_existing_records(df)
                df_to_insert = duplicate_check['new_records_df']
                skipped_count = duplicate_check['existing_count']
                
                if skipped_count > 0:
                    logger.info(f"Skipping {skipped_count} records that already exist in database")
                
                if df_to_insert.count() == 0:
                    logger.info("No new records to insert - all records already exist")
                    return {
                        'success': True,
                        'total_rows': total_rows,
                        'inserted_rows': 0,
                        'skipped_rows': skipped_count,
                        'message': 'All records already exist in database'
                    }
            else:
                df_to_insert = df
                skipped_count = 0
            
            insert_count = df_to_insert.count()
            
            try:
                # Try to check if table exists using SQL                
                table_exists = self.spark.catalog.tableExists(self.table_name)
                
                if table_exists:
                    # Table exists, use merge operation
                    delta_table = DeltaTable.forName(self.spark, self.table_name)
                    
                    merge_result = (delta_table.alias("target")
                                   .merge(df_to_insert.alias("source"), "target.Attestnummer = source.Attestnummer")
                                   .whenNotMatchedInsertAll()
                                   .execute())
                    
                    logger.info(f"Merge operation completed")
                else:
                    # Table doesn't exist, create it
                    logger.info("Table does not exist, creating new table")
                    df_to_insert.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .option("path", self.table_path) \
                        .saveAsTable(self.table_name)
                    logger.info("New Delta table created")
                    
            except Exception as table_error:
                logger.info(f"Table operation failed, trying alternative approach: {str(table_error)}")
                # Fallback: Create/append to table using simpler approach
                try:
                    df_to_insert.write \
                        .format("delta") \
                        .mode("append") \
                        .saveAsTable(self.table_name)
                    logger.info("Data appended to table successfully")
                except Exception as append_error:
                    logger.info(f"Append failed, creating new table: {str(append_error)}")
                    df_to_insert.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .saveAsTable(self.table_name)
                    logger.info("New Delta table created with overwrite")
            
            result = {
                'success': True,
                'total_rows': total_rows,
                'inserted_rows': insert_count,
                'skipped_rows': skipped_count,
                'insert_rate': (insert_count / total_rows) * 100 if total_rows > 0 else 100
            }
            
            logger.info(f"Delta table insert completed: {insert_count} inserted, {skipped_count} skipped")
            
            return result
            
        except Exception as e:
            error_msg = f"Delta table insert failed: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'total_rows': df.count() if df is not None else 0,
                'inserted_rows': 0,
                'skipped_rows': 0
            }
    
    def process_csv_by_year(self, year: int, skip_duplicates: bool = True) -> Dict[str, Any]:
        """
        Process a single year's CSV file
        """
        try:
            logger.info(f"Starting CSV processing for year: {year}")
            
            # Step 1: Build file path and check if exists
            file_path = self.get_csv_file_path(year)
            
            if not self.check_file_exists(file_path):
                return {
                    'success': False,
                    'year': year,
                    'error': f'File not found: enova_data_{year}.csv'
                }
            
            # Step 2: Analyze CSV structure
            analysis = self.analyze_csv_structure(file_path)
            
            # Step 3: Read and clean CSV
            cleaned_df = self.read_and_clean_csv(file_path)
            
            # Step 4: Insert to Delta table
            insert_result = self.insert_to_delta_table(cleaned_df, skip_duplicates)
            
            # Combine results
            result = {
                'success': insert_result['success'],
                'year': year,
                'file_path': file_path,
                'csv_analysis': analysis,
                'delta_insert': insert_result,
                'total_processed': analysis['total_rows'],
                'total_inserted': insert_result.get('inserted_rows', 0),
                'total_skipped': insert_result.get('skipped_rows', 0)
            }
            
            logger.info(f"Year {year} processing completed: "
                       f"{result['total_inserted']} inserted, "
                       f"{result['total_skipped']} skipped")
            
            return result
            
        except Exception as e:
            error_msg = f"CSV processing failed for year {year}: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'year': year,
                'error': error_msg
            }
    
    def process_multiple_years(self, years: List[int], skip_duplicates: bool = True) -> Dict[str, Any]:
        """
        Process multiple years of CSV files
        """
        logger.info(f"Starting batch processing for years: {years}")
        
        results = []
        total_inserted = 0
        total_skipped = 0
        total_processed = 0
        successful_years = []
        failed_years = []
        
        for year in years:
            try:
                result = self.process_csv_by_year(year, skip_duplicates)
                results.append(result)
                
                if result['success']:
                    successful_years.append(year)
                    total_inserted += result.get('total_inserted', 0)
                    total_skipped += result.get('total_skipped', 0)
                    total_processed += result.get('total_processed', 0)
                else:
                    failed_years.append(year)
                    
            except Exception as e:
                error_result = {
                    'success': False,
                    'year': year,
                    'error': str(e)
                }
                results.append(error_result)
                failed_years.append(year)
        
        summary = {
            'success': len(successful_years) > 0,
            'total_years_requested': len(years),
            'successful_years': successful_years,
            'failed_years': failed_years,
            'total_processed': total_processed,
            'total_inserted': total_inserted,
            'total_skipped': total_skipped,
            'individual_results': results
        }
        
        logger.info(f"Batch processing completed: "
                   f"{len(successful_years)} successful, "
                   f"{len(failed_years)} failed, "
                   f"{total_inserted} total inserted")
        
        return summary

def create_processor() -> FabricCSVProcessor:
    """Factory function to create a processor instance"""
    return FabricCSVProcessor()

# Example usage functions
def process_single_year(year: int, skip_duplicates: bool = True):
    """Process a single year - convenience function"""
    processor = create_processor()
    return processor.process_csv_by_year(year, skip_duplicates)

def process_year_range(start_year: int, end_year: int, skip_duplicates: bool = True):
    """Process a range of years - convenience function"""
    years = list(range(start_year, end_year + 1))
    processor = create_processor()
    return processor.process_multiple_years(years, skip_duplicates)

def process_all_available_years(skip_duplicates: bool = True):
    """Process all years from 2009 to current year - convenience function"""
    current_year = datetime.now().year
    years = list(range(2009, current_year + 1))
    processor = create_processor()
    return processor.process_multiple_years(years, skip_duplicates)

# Example usage:
# 
# # Process a single year
# result = process_single_year(2020)
# print(f"Processed {result['total_inserted']} records for 2020")
#
# # Process multiple years
# result = process_year_range(2015, 2020)
# print(f"Processed {result['total_inserted']} records across {len(result['successful_years'])} years")
#
# # Process all available years
# result = process_all_available_years()
# print(f"Processed {result['total_inserted']} total records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DELETE ALL ROWS
# Create empty DataFrame with same schema
processor = FabricCSVProcessor()
empty_df = spark.createDataFrame([], processor.schema)

# Overwrite table with empty data
empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("energylabels_bronze.raw_ingestion.EnovaApi_ImpHist")

print("Table overwritten with empty data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test single Year import
result = process_single_year(2009)
print(f"Year 2009 - Success: {result['success']}")
print(f"Inserted: {result['total_inserted']}, Skipped: {result['total_skipped']}")

if result['success']:
    print("Data import successful!")
    
    # Verify the data was written

    # Use the correct table name
    df_check = spark.table("energylabels_bronze.raw_ingestion.EnovaApi_ImpHist")
    print(f"Rows in table: {df_check.count()}")
    df_check.select("ImpHist_ID", "Attestnummer", "GateAdresse", "Energikarakter").show(5)
    
else:
    print(f"Error: {result.get('delta_insert', {}).get('error', 'Unknown error')}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process a range of years
result = process_year_range(2010, 2012)
print(f"Processed {len(result['successful_years'])} years")
print(f"Total inserted: {result['total_inserted']:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process all available years at once
result = process_all_available_years()
print(f"Total years processed: {len(result['successful_years'])}")
print(f"Failed years: {result['failed_years']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the full dataset
df = spark.table("energylabels_bronze.raw_ingestion.EnovaApi_ImpHist")
print(f"Total rows in table: {df.count():,}")

# Check data distribution by year (based on Utstedelsesdato)
df.select(year("Utstedelsesdato").alias("year")).groupBy("year").count().orderBy("year").show()

# Check duplicate handling works
print(f"Unique Attestnummer count: {df.select('Attestnummer').distinct().count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
