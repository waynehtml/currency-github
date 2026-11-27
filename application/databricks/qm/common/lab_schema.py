# Databricks notebook source
from pyspark.sql.types import IntegerType, DateType, DecimalType, StringType

metadata = {
        'denomination': '1,B',
        #'batch_number': '2,B',
        'batch_order': '3,B',
        'batch_processing_date': '4,B',
        'report_date': '5,B',
        'number_of_special_fit': '6,B',
        'visual_inspection_failed': '7,B',
        'infrared_inspection_failed': '8,B',
        'consol_1': '2,D',
        'consol_2': '2,E',
        'consol_3': '2,F',
        'qcd_used': '1,J',
        'sampling_regime_1': '2,J',
        'sampling_regime_2': '2,K',
        'qa_lab_recommendation': '5,J'
    }

# COMMAND ----------

schema = [
  {"tag": "denomination", "name": "denomination", "type": StringType()},
  #{"tag": "batch_number", "name": "batch_number", "type": IntegerType()},
  {"tag": "batch_order", "name": "batch_order", "type": StringType()},
  {"tag": "batch_processing_date", "name": "batch_processing_date", "type": DateType()},
  {"tag": "report_date", "name": "report_date", "type": DateType()},
  {"tag": "number_of_special_fit", "name": "number_of_special_fit", "type": IntegerType()},
  {"tag": "visual_inspection_failed", "name": "visual_inspection_failed", "type": IntegerType()},
  {"tag": "infrared_inspection_failed", "name": "infrared_inspection_failed", "type": IntegerType()},
  {"tag": "consol_1", "name": "consol_1", "type": StringType()},
  {"tag": "consol_2", "name": "consol_2", "type": StringType()},
  {"tag": "consol_3", "name": "consol_3", "type": StringType()},
  {"tag": "qcd_used", "name": "qcd_used", "type": StringType()},
  {"tag": "sampling_regime", "name": "sampling_regime", "type": StringType()},
  {"tag": "qa_lab_recommendation", "name": "qa_lab_recommendation", "type": StringType()},
  {"tag": "serialnumber", "name": "serial_number", "type": StringType()},
  {"tag": "softmag", "name": "soft_mag", "type": IntegerType()},
  {"tag": "hardmag", "name": "hard_mag", "type": DecimalType(10, 2)},
  {"tag": "scf", "name": "sc_f", "type": IntegerType()},
  {"tag": "scb", "name": "sc_b", "type": IntegerType()},
  {"tag": "dr", "name": "dr", "type": StringType()},
  {"tag": "bnrdnm", "name": "bnr_dnm", "type": IntegerType()},
  {"tag": "ira", "name": "ir_a", "type": DecimalType(10, 2)},
  {"tag": "irb", "name": "ir_b", "type": DecimalType(10, 2)},
  {"tag": "irc", "name": "ir_c", "type": DecimalType(10, 2)},
  {"tag": "hlas", "name": "hlas", "type": IntegerType()},
  {"tag": "tf1", "name": "tf_1", "type": IntegerType()},
  {"tag": "tf2", "name": "tf_2", "type": IntegerType()},
  {"tag": "tf3", "name": "tf_3", "type": IntegerType()},
  {"tag": "tf4", "name": "tf_4", "type": IntegerType()},
  {"tag": "glossf", "name": "gloss_f", "type": DecimalType(10, 2)},
  {"tag": "glossb", "name": "gloss_b", "type": DecimalType(10, 2)},
  {"tag": "visinsp", "name": "vis_insp", "type": StringType()},
  {"tag": "bnsize", "name": "bn_size", "type": StringType()},
  {"tag": "foil", "name": "foil", "type": StringType()},
  {"tag": "doe3", "name": "doe3", "type": StringType()},
  {"tag": "bnr", "name": "bnr", "type": StringType()},
  {"tag": "litho", "name": "litho", "type": StringType()},
  {"tag": "intag", "name": "intag", "type": StringType()},
  {"tag": "number", "name": "number", "type": StringType()},
  {"tag": "tfreg", "name": "tf_reg", "type": StringType()},
  {"tag": "tape ", "name": "tape", "type": StringType()},
  {"tag": "doe3test", "name": "doe3_test", "type": StringType()},
  {"tag": "comment", "name": "comment", "type": StringType()},
  {"tag": "process", "name": "process", "type": StringType()},
  {"tag": "process2", "name": "process_2", "type": StringType()},
  {"tag": "process3", "name": "process_3", "type": StringType()},
  {"tag": "irinsp", "name": "ir_insp", "type": StringType()},
  {"tag": "batch", "name": "batch", "type": StringType()}
]

# COMMAND ----------


ds_map = {
    "metadata": metadata,
    "schema": schema,
    "match": "exact",
    "table": "tbl_qm_labdata"
}
