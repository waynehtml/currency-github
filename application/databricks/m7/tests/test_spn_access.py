# Databricks notebook source
# MAGIC %run ops/Utils/auth_util

# COMMAND ----------

dbutils.widgets.text("storage","", "storage") 
storage_name = dbutils.widgets.get('storage')


container_name = "restricted"
cur_path = "curated/currency/"


CURRENY_SECRET_SCOPE = "lake-kv-currency-scope"
CURRENY_SPN_ID = "spn-currencyservices-id"
CURRENY_SPN_KEY = "spn-currencyservices-key"
LAKE_TENANT_ID = "lake-tenant-id"

# COMMAND ----------

def test_spn_access_enabled():
  
    spn_access_enabled = tryEnableSpnAccess(CURRENY_SECRET_SCOPE, storage_name, CURRENY_SPN_ID, CURRENY_SPN_KEY, LAKE_TENANT_ID)
  
    assert spn_access_enabled == True

# COMMAND ----------

test_spn_access_enabled()
