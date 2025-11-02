# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{bookstore.dataset_path}/kafka-raw")
display(files)

# COMMAND ----------

print(f"{bookstore.dataset_path}")
df_raw = spark.read.json(f"{bookstore.dataset_path}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")   # Uses Databricks Auto Loader, which incrementally reads new files.
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{bookstore.dataset_path}/kafka-raw")    # Reads files from a raw zone path (e.g., ADLS folder).
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", f"{bookstore.checkpoint_path}/bronze")  #Saves progress metadata (which files were processed) for **exactly-once** reliability
                      .option("mergeSchema", True)  # Allows schema evolution if new columns appear
                      .partitionBy("topic", "year_month")
                      .trigger(availableNow=True)   # Process all data in multiple micro-batches and then stops
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze
