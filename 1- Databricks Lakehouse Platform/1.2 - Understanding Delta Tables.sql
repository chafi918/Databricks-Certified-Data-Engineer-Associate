-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Creating Delta Lake Tables

-- COMMAND ----------

USE CATALOG workspace

-- COMMAND ----------

-- DBTITLE 1,Cellule 3
CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Catalog Explorer
-- MAGIC
-- MAGIC Check the created **employees** table in the **Catalog** explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Inserting Data

-- COMMAND ----------

-- NOTE: With latest Databricks Runtimes, inserting few records in single transaction is optimized into single data file.
-- For this demo, we will insert the records in multiple transactions in order to create 4 data files.

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- NOTE: When executing multiple SQL statements in the same cell, only the last statement's result will be displayed in the cell output.

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Metadata

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table Directory

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://unity-catalog-storage@dbstoragevpsgwzl454hg4.dfs.core.windows.net/7405616136717707/__unitystorage/catalogs/806d24bc-529f-454c-9a5d-65de93e7df39/tables/f3f5bf0c-40e9-4911-9995-2e08cc3eea76'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Updating Table

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

SELECT * FROM employees@v3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exploring Table History

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SET spark.databricks.delta.autoCompact.minNumFiles; 

-- COMMAND ----------

DESCRIBE TABLE EXTENDED employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000005.json'

-- COMMAND ----------


