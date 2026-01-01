# Databricks notebook source
# MAGIC %run ./_version/_version_id

# COMMAND ----------

#MLflow Autologging feature is enabled by default in Databricks Runtime 10.4 ML or above. Disabling this feature as it is observed to cause slowdown

import mlflow
import datetime as dt

if hasattr(mlflow, "autolog"): #Autologging is available only for Databricks Runtime 10.4 ML or above
  mlflow.autolog(disable=True)
  print("Disabling the MLFlow Autologging feature as it is observed to cause slowdown....")
else:
  pass

# COMMAND ----------

#**********************************
# DNB schema
#**********************************

class dnb_schema:
  version = "_v2"
  user_ref_summary = f"user_reference.data_dictionary_table_summary{version}"
  user_ref_detail  = f"user_reference.data_dictionary_table_detail{version}"

  def __init__(self,database=""):
    """
      Display columns in this table
      Example:
        schema = dnb_schema("us_marketing")
    """
    
    if database!="":
      default_db=database
    else:
      default_db=self.check_available_db()
      
    self.database = default_db
    
    # set workarea
    self.get_workarea_table()
    
    return
  
  def reset(self):
    self.database = self.check_available_db()
  
  def check_available_db(self,dbs=["us_marketing","us_risk","intl_marketing","intl_risk"]):
    for db in dbs:
      df = self.show_databases(silent=True,like=db)
      if df.count() > 0:
        dbs = df.take(1)[0]
        self.database = dbs.databaseName
        return self.database
    print('no default db found')  
    return ""

  def get_workarea_table(self):
    dbs = self.show_databases(silent=True)
    error_flag = False
    cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
    ## Need to expand this dictionary to include more mappings.
    ## Not the best solution for sure but cleaner than previous version
    cluster2area_map = {'dev_nolimit_73':'dev_workarea',
                        'analytics_packages_dev2_cluster':'analytics_packages_dev_workarea',
                        'analytics_cluster2':'analytics2_workarea',
                        'qa_nolimit_73':'qa_workarea',
                        'qa_immuta_73':'qa_workarea',
                        'analytics_sales_support_cluster_73':'sales_workarea',
                        }
    
    if cluster_name in cluster2area_map.keys():
      self.workarea = cluster2area_map[cluster_name]
    else:
      # TECH 2/10/2022: AAS-5588
      try:
        self.workarea = spark.conf.get("dnb.analyticsStudio.cluster.workarea")
      except:
        error_flag = True
        pass
      # DEV 3/11/2022
      if error_flag: 
        raise ValueError(f'ERROR: workarea not defined for cluster {cluster_name}!')
    return self.workarea
  
  # set current db
  def set_database(self,database):
    """
      Display columns in this table
      Example:
          schema.set_database("us_marketing")
    """
    self.database = database
    return
  
  def show_databases(self,n=100,silent=False,like=""):
    """
      Display all the databases
      Example:
          schema.show_databases()
    """  
    df=spark.sql(f"show databases like '{like}*'")
    if (not silent):
      df.show(n,False)
    return df
  
  # show tables in db
  def show_tables(self,database="",table_filter="",silent=False,n=500):
    """
      Display all the tables in this database
      
      Example:
          df = schema.dnb_show_tables()
          display(df)
    """  
    self.database = database.strip() if database != "" else self.database
    df=(spark.sql(f"show tables in {self.database}")
            .select(F.col('tableName').alias('table_name'), F.lit(None).alias("table_description"), F.lit(self.database).alias("database")))
    if len(table_filter)>0:
      df=df.where(f"table_name like '%{table_filter}%'")
      
    if (self.database not in ["user_reference","default",self.workarea]):
      # fetch table descriptions from data dict 
      right = spark.sql(f"select table_name, table_description as description, database from {self.user_ref_summary} a where database='{self.database}'")
      out = ( df.join(right, (df.table_name == right.table_name) & (df.database == F.lit(self.database)))
                .select("a.table_name", "description", "a.database")
                .sort("a.table_name")
            )
    else:
      out=df
      
    if (not silent):
      out.show(n,False)
    return out

  # show colummns in a table
  def show_cols(self,table,col_filter=""):
    """
      Display columns in this table
      Example:
          df=schema.show_cols("business_firmographics_site_us")
          display(df)
    """
      
    df=spark.sql("describe table " + self.database + "." + table.strip())

    # fetch table descriptions from data dict 
    out = df
    if (self.database not in ["user_reference","default",self.workarea]):
      right = spark.sql(f"""select * 
                        from {self.user_ref_detail} where table_name='{table.strip()}'""")
      out = ( df.join(right, (df.col_name == right.column_name))
                .select("column_name", 
                        F.col("column_data_type").alias("data_type"), 
                        F.col("functional_data_category").alias("fn_category"),
                        F.col("column_description").alias("description")
                       )
                .sort("column_name")
            )
      if len(col_filter)>0:
        out=out.where(f"lower(column_name || ' ' || description) like '%{col_filter.lower()}%'")
    else:
      if len(col_filter)>0:
        out=out.where(f"lower(col_name) like '%{col_filter.lower()}%'")
      
    
    return out
    
  # return pkl path 
  def get_pkl_path(self,model_name,model_version,location=None,customer_name=None,func_name = None):
    """
    Returns path to save pkl which has model_name & model_version as suffix for reffernce 
      
    parameters:
    ----------
    model_name (str) : name of the model 
    model_version (str) : version of the model
    
    return:
    -------
    s3 path (str) : S3 path to save the pkl file
    """
    #location to save pickle
    if not location:
      location = spark.sql(f"""describe database {self.workarea}""").where("database_description_item == 'Location'").collect()[0].database_description_value 
      
    customer_name = customer_name if customer_name else location.split("/")[3]
    
    #assinging customer name
    self.customer_name = customer_name
    
    #time stamp in format (yyyymmdd_hhmm)
    time_stamp = str(dt.datetime.now())[:-10].replace(' ','_').replace('__','_').replace('-', '').replace(':','')
    
    # Path to save the model
    model_pickle_path = location +'/pickle_folder/' + func_name + '_' +customer_name + '_' + model_name + '_' + model_version+ '_' +time_stamp+'.pkl' 
    
    return model_pickle_path.replace(" ", "_")

# COMMAND ----------

# dbfs location to save the intermediate files/results during model building
dbutils.fs.mkdirs("dbfs:/FileStore/files/")

# COMMAND ----------

schema = dnb_schema()
schema.get_workarea_table()