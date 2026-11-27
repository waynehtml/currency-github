# Databricks notebook source
from pyspark.sql.types import IntegerType, DateType, DecimalType,StringType


# COMMAND ----------

# DBTITLE 1,litho
litho_schema = [
    {"tag": "recordnumber,recordno,record#", "name": "record", "type": IntegerType()},
    {"tag": "dated/m/y", "name": "date", "type": DateType()},
    {"tag": "lotnumber,lotnum,lotno.,lotno,lot#,lot", "name": "lot", "type": IntegerType()},
    {"tag": "drmark,dr_marks", "name": "dr_marks", "type": StringType()},
    {"tag": "x_a", "name": "x_a", "type": DecimalType(10, 3)},
    {"tag": "x_b", "name": "x_b", "type": DecimalType(10, 3)},
    {"tag": "x_c", "name": "x_c", "type": DecimalType(10, 3)},
    {"tag": "x_d", "name": "x_d", "type": DecimalType(10, 3)},
    {"tag": "x_e", "name": "x_e", "type": DecimalType(10, 3)},
    {"tag": "x_a2,x2_a", "name": "x_a2", "type": DecimalType(10, 3)},
    {"tag": "x_b2,x2_b", "name": "x_b2", "type": DecimalType(10, 3)},
    {"tag": "x_c2,x2_c", "name": "x_c2", "type": DecimalType(10, 3)},
    {"tag": "x_d2,x2_d", "name": "x_d2", "type": DecimalType(10, 3)},
    {"tag": "x_e2,x2_e", "name": "x_e2", "type": DecimalType(10, 3)},
    {"tag": "x_a3,x3_a", "name": "x_a3", "type": DecimalType(10, 3)},
    {"tag": "x_b3,x3_b", "name": "x_b3", "type": DecimalType(10, 3)},
    {"tag": "x_c3,x3_c", "name": "x_c3", "type": DecimalType(10, 3)},
    {"tag": "x_d3,x3_d", "name": "x_d3", "type": DecimalType(10, 3)},
    {"tag": "x_e3,x3_e", "name": "x_e3", "type": DecimalType(10, 3)},
    {"tag": "scf_a", "name": "scf_a", "type": IntegerType()},
    {"tag": "scf_b", "name": "scf_b", "type": IntegerType()},
    {"tag": "scf_c", "name": "scf_c", "type": IntegerType()},
    {"tag": "scf_d", "name": "scf_d", "type": IntegerType()},
    {"tag": "scf_e", "name": "scf_e", "type": IntegerType()},
    {"tag": "scb_a", "name": "scb_a", "type": IntegerType()},
    {"tag": "scb_b", "name": "scb_b", "type": IntegerType()},
    {"tag": "scb_c", "name": "scb_c", "type": IntegerType()},
    {"tag": "scb_d", "name": "scb_d", "type": IntegerType()},
    {"tag": "scb_e", "name": "scb_e", "type": IntegerType()}
]

# COMMAND ----------

# DBTITLE 1,intaglio
intaglio_schema = [
  {"tag": "recordnumber,recordno,record#", "name": "record", "type": IntegerType()},
  {"tag": "dated/m/y", "name": "date", "type": DateType()},
  {"tag": "lotnumber,lotnum,lotno.,lotno,lot#,lot", "name": "lot", "type": IntegerType()},
  {"tag": "drmark,dr,dr_marks", "name": "dr_marks", "type": StringType()},
  {"tag": "st_a", "name": "st_a", "type": DecimalType(10, 3)},
  {"tag": "st_b", "name": "st_b", "type": DecimalType(10, 3)},
  {"tag": "st_c", "name": "st_c", "type": DecimalType(10, 3)},
  {"tag": "st_d", "name": "st_d", "type": DecimalType(10, 3)},
  {"tag": "st_e", "name": "st_e", "type": DecimalType(10, 3)},
  {"tag": "nm_a", "name": "nm_a", "type": DecimalType(10, 3)},
  {"tag": "nm_b", "name": "nm_b", "type": DecimalType(10, 3)},
  {"tag": "nm_c", "name": "nm_c", "type": DecimalType(10, 3)},
  {"tag": "nm_d", "name": "nm_d", "type": DecimalType(10, 3)},
  {"tag": "nm_e", "name": "nm_e", "type": DecimalType(10, 3)},
  {"tag": "mag_a", "name": "mag_a", "type": IntegerType()},
  {"tag": "mag_b", "name": "mag_b", "type": IntegerType()},
  {"tag": "mag_c", "name": "mag_c", "type": IntegerType()},
  {"tag": "mag_d", "name": "mag_d", "type": IntegerType()},
  {"tag": "mag_e", "name": "mag_e", "type": IntegerType()}
]

# COMMAND ----------

# DBTITLE 1,type
type_schema = [
  {"tag": "recordnumber,recordno,record#", "name": "record", "type": IntegerType()},
  {"tag": "dated/m/y", "name": "date", "type": DateType()},
  {"tag": "lotnumber,lotnum,lotno.,lotno,lot#,lot", "name": "lot", "type": IntegerType()},
  {"tag": "num_a", "name": "num_a", "type": DecimalType(10, 3)},
  {"tag": "num_b", "name": "num_b", "type": DecimalType(10, 3)},
  {"tag": "num_c", "name": "num_c", "type": DecimalType(10, 3)},
  {"tag": "num_d", "name": "num_d", "type": DecimalType(10, 3)},
  {"tag": "num_e", "name": "num_e", "type": DecimalType(10, 3)}
]

# COMMAND ----------

# DBTITLE 1,tactile
tactile_schema = [
  {"tag": "recordnumber,recordno,record#", "name": "record", "type": IntegerType()},
  {"tag": "dated/m/y", "name": "date", "type": DateType()},
  {"tag": "lotnumber,lotnum,lotno.,lotno,lot#,lot", "name": "lot", "type": IntegerType()},
  {"tag": "covert,convert", "name": "covert", "type": StringType()},
  {"tag": "thgt_a,thgt_a1", "name": "thgt_a1", "type": DecimalType(10, 3)},
  {"tag": "thgt_a2", "name": "thgt_a2", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_a3", "name": "thgt_a3", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_a4", "name": "thgt_a4", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_b,thgt_b1", "name": "thgt_b1", "type": DecimalType(10, 3)},
  {"tag": "thgt_b2", "name": "thgt_b2", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_b3", "name": "thgt_b3", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_b4", "name": "thgt_b4", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_c,thgt_c1", "name": "thgt_c1", "type": DecimalType(10, 3)},
  {"tag": "thgt_c2", "name": "thgt_c2", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_c3", "name": "thgt_c3", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_c4", "name": "thgt_c4", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_d,thgt_d1", "name": "thgt_d1", "type": DecimalType(10, 3)},
  {"tag": "thgt_d2", "name": "thgt_d2", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_d3", "name": "thgt_d3", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_d4", "name": "thgt_d4", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_e,thgt_e1", "name": "thgt_e1", "type": DecimalType(10, 3)},
  {"tag": "thgt_e2", "name": "thgt_e2", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_e3", "name": "thgt_e3", "type": DecimalType(10, 3), "null":True},
  {"tag": "thgt_e4", "name": "thgt_e4", "type": DecimalType(10, 3), "null":True}
]

# COMMAND ----------

# DBTITLE 1,varnish
varnish_schema = [
  {"tag": "batchnumber,batchno,batchno.,batch#", "name": "record", "type": IntegerType()},
  {"tag": "lotnumber,lotnum,lotno.,lotno,lot#,lot", "name": "lot", "type": IntegerType()},
  {"tag": "dated/m/y", "name": "date", "type": DateType()},
  {"tag": "gf_a", "name": "gf_a", "type": DecimalType(10, 3)},
  {"tag": "gf_b", "name": "gf_b", "type": DecimalType(10, 3)},
  {"tag": "gf_c", "name": "gf_c", "type": DecimalType(10, 3)},
  {"tag": "gf_d", "name": "gf_d", "type": DecimalType(10, 3)},
  {"tag": "gf_e", "name": "gf_e", "type": DecimalType(10, 3)},
  {"tag": "gf_a2,gf2_a", "name": "gf2_a", "type": DecimalType(10, 3)},
  {"tag": "gf_b2,gf2_b", "name": "gf2_b", "type": DecimalType(10, 3)},
  {"tag": "gf_c2,gf2_c", "name": "gf2_c", "type": DecimalType(10, 3)},
  {"tag": "gf_d2,gf2_d", "name": "gf2_d", "type": DecimalType(10, 3)},
  {"tag": "gf_e2,gf2_e", "name": "gf2_e", "type": DecimalType(10, 3)},
  {"tag": "gb_a", "name": "gb_a", "type": DecimalType(10, 3)},
  {"tag": "gb_b", "name": "gb_b", "type": DecimalType(10, 3)},
  {"tag": "gb_c", "name": "gb_c", "type": DecimalType(10, 3)},
  {"tag": "gb_d", "name": "gb_d", "type": DecimalType(10, 3)},
  {"tag": "gb_e", "name": "gb_e", "type": DecimalType(10, 3)},
  {"tag": "gb_a2,gb2_a", "name": "gb2_a", "type": DecimalType(10, 3)},
  {"tag": "gb_b2,gb2_b", "name": "gb2_b", "type": DecimalType(10, 3)},
  {"tag": "gb_c2,gb2_c", "name": "gb2_c", "type": DecimalType(10, 3)},
  {"tag": "gb_d2,gb2_d", "name": "gb2_d", "type": DecimalType(10, 3)},
  {"tag": "gb_e2,gb2_e", "name": "gb2_e", "type": DecimalType(10, 3)}
]

# COMMAND ----------

# DBTITLE 1,finishedsheetanalysis
finishedsheetanalysis_schema = [
    {"tag": "batchno.,batch#,batchno,batchnumber,batchnum", "name": "record", "type": StringType()},
    {"tag": "lotnumber,lotnum,lotno.,lotno,lot#,lot", "name": "lot", "type": IntegerType()},
    {"tag": "dated/m/y", "name": "date", "type": DateType()},
    {"tag": "covert,convert", "name": "covert", "type": StringType()},
    {"tag": "x_a", "name": "x_a", "type": DecimalType(10, 3)},
    {"tag": "x_b", "name": "x_b", "type": DecimalType(10, 3)},
    {"tag": "x_c", "name": "x_c", "type": DecimalType(10, 3)},
    {"tag": "x_d", "name": "x_d", "type": DecimalType(10, 3)},
    {"tag": "x_e", "name": "x_e", "type": DecimalType(10, 3)},
    {"tag": "scf_a", "name": "scf_a", "type": IntegerType()},
    {"tag": "scf_b", "name": "scf_b", "type": IntegerType()},
    {"tag": "scf_c", "name": "scf_c", "type": IntegerType()},
    {"tag": "scf_d", "name": "scf_d", "type": IntegerType()},
    {"tag": "scf_e", "name": "scf_e", "type": IntegerType()},
    {"tag": "scb_a", "name": "scb_a", "type": IntegerType()},
    {"tag": "scb_b", "name": "scb_b", "type": IntegerType()},
    {"tag": "scb_c", "name": "scb_c", "type": IntegerType()},
    {"tag": "scb_d", "name": "scb_d", "type": IntegerType()},
    {"tag": "scb_e", "name": "scb_e", "type": IntegerType()},
    {"tag": "mag_a", "name": "mag_a", "type": IntegerType()},
    {"tag": "mag_b", "name": "mag_b", "type": IntegerType()},
    {"tag": "mag_c", "name": "mag_c", "type": IntegerType()},
    {"tag": "mag_d", "name": "mag_d", "type": IntegerType()},
    {"tag": "mag_e", "name": "mag_e", "type": IntegerType()},
    {"tag": "st_a", "name": "st_a", "type": DecimalType(10, 3)},
    {"tag": "st_b", "name": "st_b", "type": DecimalType(10, 3)},
    {"tag": "st_c", "name": "st_c", "type": DecimalType(10, 3)},
    {"tag": "st_d", "name": "st_d", "type": DecimalType(10, 3)},
    {"tag": "st_e", "name": "st_e", "type": DecimalType(10, 3)},
    {"tag": "nm_a", "name": "nm_a", "type": DecimalType(10, 3)},
    {"tag": "nm_b", "name": "nm_b", "type": DecimalType(10, 3)},
    {"tag": "nm_c", "name": "nm_c", "type": DecimalType(10, 3)},
    {"tag": "nm_d", "name": "nm_d", "type": DecimalType(10, 3)},
    {"tag": "nm_e", "name": "nm_e", "type": DecimalType(10, 3)},
    {"tag": "thght_a1,thght_a", "name": "thght_a1", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_a2", "name": "thght_a2", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_a3", "name": "thght_a3", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_a4", "name": "thght_a4", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_b1,thght_b", "name": "thght_b1", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_b2", "name": "thght_b2", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_b3", "name": "thght_b3", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_b4", "name": "thght_b4", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_c1,thght_c", "name": "thght_c1", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_c2", "name": "thght_c2", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_c3", "name": "thght_c3", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_c4", "name": "thght_c4", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_d1,thght_d", "name": "thght_d1", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_d2", "name": "thght_d2", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_d3", "name": "thght_d3", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_d4", "name": "thght_d4", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_e1,thght_e", "name": "thght_e1", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_e2", "name": "thght_e2", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_e3", "name": "thght_e3", "type": DecimalType(10, 3), "null":True},
    {"tag": "thght_e4", "name": "thght_e4", "type": DecimalType(10, 3), "null":True},
    {"tag": "num_a", "name": "num_a", "type": DecimalType(10, 3)},
    {"tag": "num_b", "name": "num_b", "type": DecimalType(10, 3)},
    {"tag": "num_c", "name": "num_c", "type": DecimalType(10, 3)},
    {"tag": "num_d", "name": "num_d", "type": DecimalType(10, 3)},
    {"tag": "num_e", "name": "num_e", "type": DecimalType(10, 3)},
    {"tag": "gf_a", "name": "gf_a", "type": DecimalType(10, 3)},
    {"tag": "gf_b", "name": "gf_b", "type": DecimalType(10, 3)},
    {"tag": "gf_c", "name": "gf_c", "type": DecimalType(10, 3)},
    {"tag": "gf_d", "name": "gf_d", "type": DecimalType(10, 3)},
    {"tag": "gf_e", "name": "gf_e", "type": DecimalType(10, 3)},
    {"tag": "gb_a", "name": "gb_a", "type": DecimalType(10, 3)},
    {"tag": "gb_b", "name": "gb_b", "type": DecimalType(10, 3)},
    {"tag": "gb_c", "name": "gb_c", "type": DecimalType(10, 3)},
    {"tag": "gb_d", "name": "gb_d", "type": DecimalType(10, 3)},
    {"tag": "gb_e", "name": "gb_e", "type": DecimalType(10, 3)}
]

# COMMAND ----------


ds_map = [
    {
        "name" : "litho",
        "tag" : "litho",
        "schema" : litho_schema,
        "selector" : "name",
        "table" : "tbl_cbn_processlitho"
    },
    {
        "name" : "intaglio",
        "tag" : "intaglio",
        "schema" : intaglio_schema,
        "selector" : "name",
        "match" : "rough",
        "table" : "tbl_cbn_processintaglio"
    },
    {
        "name" : "type",
        "tag" : "type,numbering",
        "schema" : type_schema,
        "selector" : "name",
        "match" : "rough",
        "table" : "tbl_cbn_processtype"
    },
    {   
        "name" : "tactile",
        "tag" : "tactile",
        "schema" : tactile_schema,
        "selector" : "name",
        "match" : "exact",
        "table" : "tbl_cbn_processtactile"
    },
    {
        "name" : "gloss",
        "tag" : "varnish,gloss",
        "schema" : varnish_schema,
        "selector" : "name",
        "match" : "exact",
        "table" : "tbl_cbn_processvarnish"
    },
    {
        "name" : "finished",
        "tag" : "finishedsheetanalysis,finished",
        "schema" : finishedsheetanalysis_schema,
        "selector" : "name",
        "match" : "exact",
        "table" : "tbl_cbn_finishedsheet"
    }
]

# COMMAND ----------


