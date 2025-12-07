# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading JSON
# MAGIC

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema',True)\
            .option('header',True)\
            .option('multiline',False)\
            .load('/Volumes/workspace/stream/streaming/jsonsource/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/stream/streaming/jsonsource')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/workspace/stream/streaming/jsonsource/BigMart Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL SCHEMA

# COMMAND ----------

my_ddl_schema = '''
                        Item_Identifier string,
                        Item_Weight STRING,
                        Item_Fat_Content string,
                        Item_Visibility double,
                        Item_Type string,
                        Item_MRP double,
                        Outlet_Identifier string,
                        Outlet_Establishment_Year integer,
                        Outlet_Size string,
                        Outlet_Location_Type string,
                        Outlet_Type string,
                        Item_Outlet_Sales double   '''

# COMMAND ----------

df = spark.read.format('csv')\
        .schema(my_ddl_schema)\
        .option('header',True)\
        .load('/Volumes/workspace/stream/streaming/jsonsource/BigMart Sales.csv')


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema = StructType([
                               StructField('Item_Identifier', StringType(), True),
                               StructField('Item_Weight', StringType(), True),
                               StructField('Item_Fat_Content', StringType(), True),
                               StructField('Item_Visibility', StringType(), True),
                               StructField('Item_Type', StringType(), True),
                               StructField('Item_MRP', StringType(), True),
                               StructField('Outlet_Identifier', StringType(), True),
                               StructField('Outlet_Establishment_Year', StringType(), True),
                               StructField('Outlet_Size', StringType(), True),
                               StructField('Outlet_Location_Type', StringType(), True),
                               StructField('Outlet_Type', StringType(), True),
                               StructField('Item_Outlet_Sales', StringType(), True)
                            ])

# COMMAND ----------

df = spark.read.format('csv')\
     .schema(my_strct_schema)\
     .option('header',True)\
     .load('/Volumes/workspace/stream/streaming/jsonsource/BigMart Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2

# COMMAND ----------

df.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10) ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col("Outlet_Location_Type").isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-1

# COMMAND ----------

df = df.withColumn('flag', lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario-2

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","Lf")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

