# Databricks notebook source

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------


storageAccountName = "ipldatastoragesom"
storageAccountAccessKey = "V/Xw2wHLMIODWDF7f5LSPJYZk3ZSzQj91aHb6M9txyI1L+fyAdy2OXLuQ6EbQQl0phSWv9gDcK0P+ASt3jOW8w=="
sasToken = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-14T00:36:10Z&st=2024-08-13T16:36:10Z&spr=https&sig=pofI2yCb7ZDMOt9rOPaj%2BXU%2Fus83W9KFFkPGCVjjsYk%3D"
blobContainerName = "ipl-data"
mountPoint = "/mnt/ipl/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
        source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = mountPoint,
      #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

# MAGIC
# MAGIC %fs ls "/mnt/ipl/"
# MAGIC

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls "dbfs:/mnt/ipl/raw_data/"
