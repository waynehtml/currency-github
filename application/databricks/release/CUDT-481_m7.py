# Databricks notebook source
# MAGIC %md
# MAGIC # Deployment Instruction
# MAGIC **This notebook creates tables for isa and m7 standardized data **
# MAGIC
# MAGIC * Prerequisite:
# MAGIC   * None
# MAGIC * Steps:
# MAGIC   * run this notebook after code deployment
# MAGIC * Tasks: 
# MAGIC   * create table tbl_isa_reissnotes
# MAGIC   *  
# MAGIC * Parameters:
# MAGIC   * environment - deployment environment
# MAGIC   * replace - false or true, replace if tables exist

# COMMAND ----------

# DBTITLE 1,Common
# MAGIC %run ../common/utils

# COMMAND ----------

# DBTITLE 1,Get Parameters
# remove all widgets
dbutils.widgets.removeAll()

# Add widgets
dbutils.widgets.text("environment", "")
dbutils.widgets.text("replace", "false")

# Get parameters from widgets
environment = dbutils.widgets.get("environment")
print(f"environment: {environment}")
replace = dbutils.widgets.get("replace")
print(f"replace: {replace}")

# COMMAND ----------

# Get environment if not provided
if len(environment.strip()) == 0:
    environment = get_env()
print(f"environment: {environment}")

unity_catalog_name = get_unity_catalog_name(environment)
print(f"unity_catalog_name: {unity_catalog_name}")
 

# COMMAND ----------

# DBTITLE 1,replace or skip if exists
# Define create action
create_action = 'CREATE OR REPLACE TABLE' if replace.lower() == 'true' else 'CREATE TABLE IF NOT EXISTS'
print(f"create_action: {create_action}")

# COMMAND ----------

# DBTITLE 1,m7_bn_list
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_bnlist"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    _denomination INT,
    _headercardstatus INT,
    _orientation INT,
    asychronous INT,
    bbd_result INT,
    bfs_cgraffitipixsumf INT,
    bfs_defectareacorner INT,
    bfs_dgraffitipixsumf INT,
    bfs_holeareabn INT,
    bfs_holepixsumf INT,
    bfs_hologrampresent INT,
    bfs_hwearpixsumborder INT,
    bfs_tearareaborder INT,
    bfs_wearpixsumbn INT,
    bfs_wearpixsumborder INT,
    charge STRING,
    checksumerror INT,
    close_feed INT,
    commonerror INT,
    compact_oc_busy INT,
    composed_score INT,
    confidence INT,
    crumples_score INT,
    date DATE,
    denomination STRING,
    dis2quality INT,
    dummy_0 INT,
    dummy_1 INT,
    dummy_10 INT,
    dummy_11 INT,
    dummy_2 INT,
    dummy_3 INT,
    dummy_4 INT,
    dummy_5 INT,
    dummy_6 INT,
    dummy_7 INT,
    dummy_8 INT,
    dummy_9 INT,
    edge_defect_area INT,
    f_bfsldeftmaxdepth_fit INT,
    f_bfsldeftmaxopen_fit INT,
    f_bfsldefttotarea_fit INT,
    f_bfslfoildefectpixelsum_fit INT,
    f_bfslfoildefsize_fit INT,
    f_bfslfoilwearpixelsumborder_fit INT,
    f_bfslfoilwearscore_fit INT,
    f_bfslholodarkpixelchanging_fit INT,
    f_bfslholodefectpixelsum_fit INT,
    f_bfslholoregirvar_fit INT,
    f_bfslholowearpixelsum_fit INT,
    f_bfslpolymerweardefectpixelsum_fit INT,
    f_bfslpolymerwearpixelsum_fit INT,
    f_bfslpolymerwearpixelsumborder_fit INT,
    f_bfslsumholearea_fit INT,
    f_clearwindow_fit INT,
    f_composed_unfit INT,
    f_corner_area_fit INT,
    f_crumple_score_fit INT,
    f_crumpleremborder_fit INT,
    f_crumpleremscore_fit INT,
    f_crumpleremsum_fit INT,
    f_dis_quality_fit INT,
    f_graffiti_back_fit INT,
    f_graffiti_front_fit INT,
    f_hole_area_fit INT,
    f_hole_score_fit INT,
    f_hole_win_score_fit INT,
    f_missingcorner_fit INT,
    f_nsctr_inkwear_bn_fit INT,
    f_nsctr_inkwear_border_fit INT,
    f_nscuvtape_fit INT,
    f_sn_wrongclassifier INT,
    f_snr_found INT,
    f_snsearchlist INT,
    f_soil_back_fit INT,
    f_soil_front_fit INT,
    f_soildens_back_fit INT,
    f_soildens_front_fit INT,
    f_special INT,
    f_stain_backcolorstainarea_fit INT,
    f_stain_background_back_fit INT,
    f_stain_background_front_fit INT,
    f_stain_backinkstainarea_fit INT,
    f_stain_backsoilstainarea_fit INT,
    f_stain_border_back_fit INT,
    f_stain_border_front_fit INT,
    f_stain_frontcolorstainarea_fit INT,
    f_stain_frontinkstainarea_fit INT,
    f_stain_frontsoilstainarea_fit INT,
    f_stain_motive_back_fit INT,
    f_stain_motive_front_fit INT,
    f_stain_whitearea_back_fit INT,
    f_stain_whitearea_front_fit INT,
    f_tape_score_fit INT,
    f_tear_area_fit INT,
    f_tear_depth_fit INT,
    f_tear_score_fit INT,
    f_uv_dull_fit INT,
    f_vs_hologrampresent_fit INT,
    folded_corner_area INT,
    fusion_foilwearscore INT,
    fusion_hcrumplepixsum INT,
    fusion_hscratchpixsum INT,
    gsna_sn1snchar01 INT,
    gsna_sn1snchar02 INT,
    gsna_sn1snchar03 INT,
    gsna_sn1snchar04 INT,
    gsna_sn1snchar05 INT,
    gsna_sn1snchar06 INT,
    gsna_sn1snchar07 INT,
    gsna_sn1snchar08 INT,
    gsna_sn1snchar09 INT,
    gsna_sn1snchar10 INT,
    gsna_sn1snchar11 INT,
    gsna_sn1snchar12 INT,
    gsna_sn1snchar13 INT,
    gsna_sn1snchar14 INT,
    gsna_sn1snchar15 INT,
    gsna_sn2snchar01 INT,
    gsna_sn2snchar02 INT,
    gsna_sn2snchar03 INT,
    gsna_sn2snchar04 INT,
    gsna_sn2snchar05 INT,
    gsna_sn2snchar06 INT,
    gsna_sn2snchar07 INT,
    gsna_sn2snchar08 INT,
    gsna_sn2snchar09 INT,
    gsna_sn2snchar10 INT,
    gsna_sn2snchar11 INT,
    gsna_sn2snchar12 INT,
    gsna_sn2snchar13 INT,
    gsna_sn2snchar14 INT,
    gsna_sn2snchar15 INT,
    gsna_sncountofcompsuspchars INT,
    gsna_sncountofdevchars INT,
    gsna_sncountoffailedchars INT,
    gsna_snsortclassifier INT,
    gsna_sntotalresult INT,
    header_card_number STRING,
    hole_area INT,
    hole_score INT,
    id INT,
    ingestion_date DATE,
    invalid_sorting_class INT,
    jam INT,
    length INT,
    mag_paper INT,
    mag_print INT,
    mag_print_soft INT,
    mag_sn INT,
    mag_softdist INT,
    missing_corner_area INT,
    mpcforcedrj INT,
    msec INT,
    multiple_oc_busy INT,
    no_ip INT,
    no_oc INT,
    no_sorting_information INT,
    nodenomconfig INT,
    nsc2rlcrumplepixelsumborder INT,
    nsc2rlcrumplescore INT,
    nsc2rlirauthentic INT,
    nsc2rlirbright INT,
    nsc2rlirbrightmax INT,
    nsc2rlirbrightpixelsum INT,
    nsc2rlirdark INT,
    nsc2rrcrumplepixelsumborder INT,
    nsc2rrirauthentic INT,
    nsc2rrirbright INT,
    nsc2rrirbrightmax INT,
    nsc2rrirbrightpixelsum INT,
    nsc2rrirdark INT,
    nsc2trcw2wearscore INT,
    nsc2trinkwearbn INT,
    nsc2trinkwearborder INT,
    nsc2trinkwearroi INT,
    nsc2trisdogear0 INT,
    nsc2trisdogear1 INT,
    nsc2trisdogear2 INT,
    nsc2trisdogear3 INT,
    nscbfslfoildefectpixelsum INT,
    nscbfslfoilwearpixelsum INT,
    nscbfslfoilwearscore INT,
    nscbfslholodefectpixelsum INT,
    nscbfslholoregirvar INT,
    nscbfslholowearpixelsum INT,
    nscbfslpolyweardefectpixelsum INT,
    nscbfslpolywearpixelsum INT,
    nscuv2hrluvtape INT,
    nscuv2hrruvtape INT,
    objectwidth INT,
    oc_busy INT,
    opa INT,
    r_bbd INT,
    r_cdil1 INT,
    r_cdir1 INT,
    r_composed INT,
    r_confidence INT,
    r_deno_ext INT,
    r_denomination INT,
    r_folded_corner INT,
    r_hc INT,
    r_highrun INT,
    r_hole INT,
    r_internalerror INT,
    r_internalerror_gsl1 INT,
    r_internalerror_gsl2 INT,
    r_mag_codeelongation INT,
    r_mag_paper INT,
    r_mag_print INT,
    r_mag_print_soft INT,
    r_mag_readcodeelem INT,
    r_mag_snr INT,
    r_mag_softdist INT,
    r_mag_thread INT,
    r_multipleitem INT,
    r_note_format INT,
    r_nsc2r_ir INT,
    r_nscink INT,
    r_orientation INT,
    r_sil INT,
    r_skewrun INT,
    r_sn_composed INT,
    r_sn_unreadable INT,
    r_snsearchlist INT,
    r_snsearchlistalarm INT,
    r_tear INT,
    r_uv_dull INT,
    r_uv_phos INT,
    r_vs_hologrampresent INT,
    r_wrongdeno INT,
    rawsernrlengthfailure INT,
    reg1readerror INT,
    reg2readerror INT,
    reject INT,
    block_seq_num INT,
    serial_number STRING,
    sernumbercorrelation INT,
    sernumberinblacklist INT,
    sernumbernotfullydetected INT,
    sernumberverifynotok INT,
    site_name STRING,
    snrepclass INT,
    snsortclass INT,
    soil_dens_back INT,
    soil_dens_front INT,
    soil_mod_back INT,
    soil_mod_front INT,
    sorting_class STRING,
    stacker STRING,
    stain_backcolorstainarea INT,
    stain_background_back INT,
    stain_background_front INT,
    stain_backinkstainarea INT,
    stain_backsoilstainarea INT,
    stain_border_back INT,
    stain_border_front INT,
    stain_frontcolorstainarea INT,
    stain_frontinkstainarea INT,
    stain_frontsoilstainarea INT,
    stain_graffiti_back INT,
    stain_graffiti_front INT,
    stain_motive_back INT,
    stain_motive_front INT,
    stain_whitearea_back INT,
    stain_whitearea_front INT,
    sum_missing_corner_areas INT,
    tape_score INT,
    tear_area INT,
    tear_depth INT,
    tear_max_open INT,
    tear_score INT,
    time STRING,
    too_long INT,
    transportclf INT,
    vs_hologrampresent INT,
    width INT,
    windowhole_score INT,
    bps_created STRING,
    machine_serialnumber STRING,
    machine_site STRING,
    machine_softwarerelease STRING,
    machine_versioninfo STRING,
    machine_type STRING,
    machine_variant STRING,
    machine_name STRING,
    machine_installationdate TIMESTAMP,
    parametersection_number STRING,
    parametersection_sortmode STRING,
    parametersection_stackingmode STRING,
    parametersection_starttime TIMESTAMP,
    parametersection_endtime TIMESTAMP,
    parametersection_operatorname STRING,
    parametersection_userid STRING,
    shift_number STRING,
    shift_name STRING,
    shift_starttime TIMESTAMP
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,counters
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_counts"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    machine_serial_number INT,
    machine_variant STRING,
    machine_version_info STRING,
    shift_name STRING,
    shift_number INT,
    shift_start_date DATE,
    shift_start_time STRING,
    shift_end_date DATE,
    shift_end_time STRING,
    headercard_id INT,
    headercard_singled INT,
    headercard_start_date DATE,
    headercard_start_time STRING,
    headercard_last_modify STRING,
    headercard_total_amount INT,
    rejects STRING,
    parsect_number INT,
    parsect_container_id INT,
    operation_mode STRING,
    input_mode STRING,
    stacking_mode STRING,
    sorting_mode STRING,
    parsect_expected STRING,
    parsect_denom_id INT,
    parsect_numberau INT,
    parsect_start_date DATE,
    parsect_start_time STRING,
    parsect_end_date DATE,
    parsect_end_time STRING,
    denom_id INT,
    denom_name STRING,
    currency STRING,
    value INT, 
    quality STRING,
    fitness STRING,
    output STRING,
    number INT,
    ingestion_date DATE,
    site_name STRING
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,rejections
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_rejects"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    machine_serial_number INT,
    machine_variant STRING,
    machine_version_info STRING,
    shift_name STRING,
    shift_number INT,
    shift_start_date DATE,
    shift_start_time STRING,
    shift_end_date DATE,
    shift_end_time STRING,
    headercard_id INT,
    headercard_singled INT,
    headercard_start_date DATE,
    headercard_start_time STRING,
    headercard_last_modify STRING,
    headercard_total_amount INT,
    volume INT,
    reject_category STRING,
    reject_property STRING,
    reject_property_count INT,
    ingestion_date DATE,
    site_name STRING
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)

# COMMAND ----------

# DBTITLE 1,unfits
# create table
# Define the table name and construct the SQL
table_name = f"{unity_catalog_name}.standardized_db.tbl_m7_unfits"
print(f"table_name: {table_name}")

# Compose SQL
sql = f"""
{create_action} {table_name} (
    file_date DATE,
    machine_serial_number INT,
    machine_variant STRING,
    machine_version_info STRING,
    shift_name STRING,
    shift_number INT,
    shift_start_date DATE,
    shift_start_time STRING,
    shift_end_date DATE,
    shift_end_time STRING,
    headercard_id INT,
    headercard_singled INT,
    headercard_start_date DATE,
    headercard_start_time STRING,
    headercard_last_modify STRING,
    headercard_total_amount INT,
    currency STRING,
    denomination STRING,
    machine_counter INT,
    unfit_property STRING,
    unfit_property_count INT,
    ingestion_date DATE,
    site_name STRING
) partitioned by (file_date)
 TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name',
    'delta.deletedFileRetentionDuration' = 'interval 60 days',
    'delta.logRetentionDuration' = 'interval 60 days'
);
"""
print(f"sql: {sql}")

# Execute the SQL command to create the table
spark.sql(sql)
