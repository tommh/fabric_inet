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

# --- CONFIG ---
full_name = "raw_ingestion.enovaapi_imphist"  # schema.table

# --- Get schema, partition cols, and location ---
df = spark.table(full_name)
schema = df.schema

# partition columns from catalog
part_cols = [c.name for c in spark.catalog.listColumns(full_name) if c.isPartition]

# table location
loc = spark.sql(f"DESCRIBE DETAIL {full_name}").select("location").first()[0]

# map Spark types to SQL-ish types (Delta in Spark uses lengthless STRING; keep it simple)
def spark_type_to_sql(dt):
    t = dt.simpleString()
    # keep decimals/structs/arrays as-is; Spark’s simpleString is fine for Delta DDL
    return {
        "string": "STRING",
        "boolean": "BOOLEAN",
        "byte": "TINYINT",
        "short": "SMALLINT",
        "integer": "INT",
        "long": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        # fallback to the raw Spark type for complex types
    }.get(t, t.upper())

cols = []
for f in schema.fields:
    nullability = "NOT NULL" if not f.nullable else ""
    cols.append(f"`{f.name}` {spark_type_to_sql(f.dataType)} {nullability}".strip())

cols_sql = ",\n  ".join(cols)
partition_sql = f"\nPARTITIONED BY ({', '.join([f'`{c}`' for c in part_cols])})" if part_cols else ""

ddl = f"""CREATE TABLE `{full_name}`
(
  {cols_sql}
)
USING DELTA{partition_sql}
LOCATION '{loc}';
"""

print(ddl)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define the schema
schema = StructType([
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

# Create empty DataFrame with the schema
empty_df = spark.createDataFrame([], schema)

# Write to Delta table without partitioning first
empty_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("energylabels_bronze.raw_ingestion.EnovaApi_ImpHist")

print("Table created successfully using DataFrame API!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set case-insensitive mode (if supported)
spark.conf.set("spark.sql.caseSensitive", "false")

# Run the query with filter
df = spark.sql("""
SELECT 
    ImpHist_ID, Knr, Gnr, Bnr, Snr, Fnr, Andelsnummer, Bygningsnummer,
    GateAdresse, Postnummer, Poststed, BruksEnhetsNummer, 
    Organisasjonsnummer, Bygningskategori, Byggear, Energikarakter, 
    Oppvarmingskarakter, Utstedelsesdato, TypeRegistrering, Attestnummer,
    BeregnetLevertEnergiTotaltkWhm2, BeregnetFossilandel, Materialvalg, 
    HarEnergiVurdering, EnergiVurderingDato, ImportDato
FROM energylabels_bronze.raw_ingestion.EnovaApi_ImpHist
WHERE GateAdresse = 'sollia'
LIMIT 100
""")

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
