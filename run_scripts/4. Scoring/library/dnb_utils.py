# Databricks notebook source
from dateutil.rrule import *
from datetime import date
import pyspark.sql.functions as F
import pyspark.sql.types as TY 
import os
import uuid
import pickle
import mlflow
import mlflow.sklearn
import joblib
import json

# COMMAND ----------

# MAGIC %run ./dnb_schema

# COMMAND ----------

# MAGIC %run ./dnb_uat

# COMMAND ----------

#**********************************
# DNB UTILITY FUNCTIONS
#**********************************

# todo ("pick the most likely schema")
schema = dnb_schema()

# print(f"Default Schema: {schema.database}")

# set user reference version
version="_v2"

# COMMAND ----------

def custom_split(a):
  if '|' in a:
    return (a.split("|"))
  else:
    return (a.split(","))

# COMMAND ----------

#to remove any special characters from a given string
def remove_special_chars(input_str, special_chars=' .,;:+-=$%!@#^&*()<>?~`/'):
  for cc in special_chars:
    input_str = input_str.replace(cc,'_')

  return input_str
   
# utility function to read pkl file stored in s3 path
def read_pkl_from_s3(s3_path):
  if s3_path.__contains__("dbfs:/") or s3_path.__contains__("/dbfs/") or s3_path.__contains__("/FileStore/"):
    print(f"[INFO] Reading pickle from 'dbfs' path: {s3_path}")
    dbfs_path = "/dbfs/FileStore" + s3_path.partition("FileStore")[2]
    
    with open(dbfs_path, 'rb+') as f:
      artifacts = pickle.load(f) 
  
  # when s3 path is entered as pickle path
  elif s3_path.__contains__("s3:/"):
    print(f"[INFO] Reading pickle from 's3' path: {s3_path}")
    # create temporary dbfs path
    filename_temp = s3_path.split("/")[-1]
    dbfs_path_temp = "/FileStore/files/"+filename_temp
    # copy file from s3_path to temp dbfs path
    dbutils.fs.cp(s3_path, dbfs_path_temp)
    # read the pickle file from temp dbfs path 
    with open("/dbfs"+dbfs_path_temp, 'rb+') as f:
      artifacts = pickle.load(f) 
    # delete temporary dbfs path
    dbutils.fs.rm(dbfs_path_temp)
    
  else:
    raise ValueError("Invalid 'model_pickle_path'. It should be valid dbfs or s3 path")
  
  return artifacts

# COMMAND ----------

def write_pkl_to_table(model_pickle_path):
    '''
    Writes the model_pickle_path to pkl_log_table

    Params
    ------
    model_pickle_path (str) : pickle file's s3 location

    Example
    -------
    write_pkl_to_table("s3://main_location/folder_name/model_pickle_path.pkl")

    '''
    spark.sql(f"CREATE TABLE IF NOT EXISTS {schema.workarea}.pkl_log_table (func_name STRING , s3_pkl_path STRING, run_date DATE, inserted_on TIMESTAMP) USING DELTA ")
    
    #get date
    date_str = model_pickle_path.split('_')[-2]
    run_date = str(date_str[:4]+'-'+date_str[4:6]+'-'+date_str[6:9])
    
    #get function indicator
    func_name = model_pickle_path.split('/')[-1].split('_')[0]

    #insert to table
    spark.sql(f"INSERT INTO {schema.workarea}.pkl_log_table VALUES ('{func_name}', '{model_pickle_path}','{run_date}',CURRENT_TIMESTAMP)")
    print(f"[INFO] Inserted {model_pickle_path} in table pkl_log_table ")

    return None
  
  
# utility function to save pkl file in s3 path
def save_pkl_to_s3(model_pickle_path:str, artifacts:object, to_table = True):
    '''
    This function saves the object to the given S3 location (model_pickle_path) as a pickle file

    Params
    ------
    model_pickle_path (string) : Path where the pickle file will be saved 
    artifacts (object) : The object which needs to be pickled

    returns 
    -------
    model_pickle_path (str)

    '''
    # when dbfs path is entered as pickle path
    if model_pickle_path.__contains__("dbfs:/") or model_pickle_path.__contains__(
            "/dbfs/") or model_pickle_path.__contains__("/FileStore/"):
        print(
            f"[Warning] input 'model_pickle_path' is a 'dbfs' path. Model saved at 'dbfs' path cannot be operationalized.")
        model_pickle_path = "/dbfs/FileStore" + model_pickle_path.partition("FileStore")[2]
        with open(model_pickle_path, 'wb') as f:
            pickle.dump(artifacts, f)
        print(f"[INFO] Saving model and required artifacts at path  --> {model_pickle_path}")

    # when s3 path is entered as pickle path
    elif model_pickle_path.__contains__("s3:/"):
        filename_temp = model_pickle_path.split("/")[-1]
        model_pickle_path_temp = "/FileStore/files/" + filename_temp

        with open("/dbfs" + model_pickle_path_temp, 'wb') as f:
            pickle.dump(artifacts, f)

        print("[INFO] Saving model and required artifacts at path  -->", model_pickle_path)
        dbutils.fs.cp(model_pickle_path_temp, model_pickle_path)
        dbutils.fs.rm(model_pickle_path_temp)
        
        if to_table:
          write_pkl_to_table(model_pickle_path)

    else:
        raise ValueError("Invalid 'model_pickle_path'. It should be valid dbfs or s3 path")
        
    return model_pickle_path

# COMMAND ----------

# utility function to convert all the decimal type columns to double type in a spark dataframe
def convert_decimal_to_double(df):
    """
    Function to convert Decimal datatype to Double datatype in a spark dataframe
  
    """

    decimal_cols_list = [x[0] for x in df.dtypes if x[1].__contains__("decimal")]
    print(F"[INFO] {len(decimal_cols_list)} columns are identified as Decimal Type")

    df_casted = (
        df
            .select(
            *(c for c in df.columns if c not in decimal_cols_list),
            *(col(c).cast("double").alias(c) for c in decimal_cols_list)
        )
    )

    return df_casted

# COMMAND ----------

def dnb_append(df, append_table, load_year, load_month, include_cols=[], where_clause="", debug=False):
  """
  Append additional DNB features to current dataframe (assumes join key is "duns" on both tables)
  
  Note: currently does an inner join and not left join
  
  Example:
  
      df = spark.sql("select duns,target from demo_analytics_workarea.mcl_demo_data_csv")
      
      df = dnb_append(df, append_table="us_marketing.dnb_core_bus_firm_site_us", 
                        load_month=11, load_year=2019, 
                        include_cols=["sic1", "sales_us_dolr_amt", "employees_total_cnt"])
                        
      df = dnb_append(df, append_table="us_marketing.dnb_core_risk_scores_hq_us", 
                        load_month=11, load_year=2019, 
                        include_cols=["via_raw_score", "ccs_score", "fss_score"])

      print('cols-->', df.columns)
      df.count()    
      
  Returns: appended data frame
  """
  
  # create temp view
  view_name="temp_dnb_append"
  df.createOrReplaceTempView(view_name)
  df=df.repartition(200)
  
  # TODO: support include all columns (but drop duplicates)
  # TODO: support for drop_cols=[]
  
  cols1=", b.".join(include_cols)
  cols2=", ".join(include_cols)
  
  hint =  " /*+  BROADCASTJOIN(a) */ "
  
  # set where condition
  where_clause = f" where {where_clause}" if where_clause != "" else ""
  
  # build sql
  sql=f""" select {hint} a.*, b.{cols1} from {view_name} a
  left join (select duns, {cols2} from {append_table} where load_year={load_year} and load_month={load_month}) b 
     on (a.duns = b.duns)
     {where_clause}
  """

  if (debug):
    print('sql--->', sql)
    
  return spark.sql(sql)


def dnb_select(df, keep_cols=[], drop_cols=[]):
  """
  
  Select and/or Drop a list of columns from a dataframe
  
  Example:
      drop_cols=["HTTPStatus","errorCode","ErrorMessage"]
      keep_cols=["duns","primaryName","confidenceCode"]
      display(
        dnb_select(df, keep_cols=keep_cols, drop_cols=drop_cols)
      )  
      
  Returns: filtered columns
  """
  if len(keep_cols)==0:
    return df.drop(*drop_cols)
  
  return df.select(*keep_cols)


# COMMAND ----------

def dnb_generate_random_duns(n=1000, target=True, target_pct=0.2, source_table="dnb_core_id_geo_us", active_only=True, load_month=1, load_year=2019):
  """
  Generate a random list of duns from the firmographics table (default)
  
  Example:
  
      df = dnb_generate_random_duns()
      df.show(100)

  Returns: dataframe
  
  """
  
  # todo: check if access to us_marketing or us_risk
  marketable = " and marketability_ind=1 " if active_only else ""
  
  sql=f"""
    select duns, case when rand() > {target_pct} then 0 else 1 end as target 
    from {schema.database}.{source_table} 
    where load_month={load_month} and load_year={load_year} {marketable}              
    order by rand() 
    limit {n}
  """
  #print('random duns sql--->',sql)
  return spark.sql(sql)

# COMMAND ----------

def dnb_dataframe_to_csv(df,out_file):
  """
    Saves given dataframe to a csv file
  """
  fname = os.path.basename(out_file)
  path  = os.path.dirname(out_file)
  
  # parque file name
  parquet_fname = path + "/" + fname + "_" + str(uuid.uuid4())[:8]
  
  #print("dnb_dataframe_to_csv()::temp_fname: " + temp_fname)
  
  # save as parquet first
  (df.coalesce(1).write
      .option("header","true")
      .option("sep",",")
      .option("escape","\"")   # newly added
      .csv(parquet_fname)
  )

  # get the single file name
  csv_file = [x.path for x in dbutils.fs.ls(parquet_fname) if "part-000" in x.name][0]

  # copy to target file
  #dbutils.fs.cp(csv_file, out_file)
  dbutils.fs.mv(csv_file, out_file)

  # delete temp parquet csv
  dbutils.fs.rm(parquet_fname,True)
  
  return out_file


# save data frame to csv
# def dnb_dataframe_to_csv2(df,file_location):

#   # save as distributed format first
#   (df.write
#       .option("header","true")
#       .option("sep",",")
#       .option("escape","\"")   # newly added
#       .csv(file_location)
#   )

#   # get the single file name
#   # Note: select all files matching "part-" but exclude first one which is for some reason only the header record
#   csv_file_list = [x.path for x in dbutils.fs.ls(file_location) if ("part-" in x.name) and (not "part-00000" in x.name) ]

#   return csv_file_list


# file_location or url
def dnb_csv_to_dataframe(file_location, header=True, inferschema=True, delimiter=',', multiline=False, escape='\"'):
  """
    Converts a CSV file to a dataframe

    Parameters:
      file_location : file path or url
      Header, inferschema, delimiter, multiline, escape

    Return: data frame
  """
  df = (spark.read
        .format('com.databricks.spark.csv')
        .options(header=str(header).lower(), 
                 inferSchema=str(inferschema).lower(), 
                 delimiter=delimiter, 
                 multiLine=str(multiline).lower(), 
                 escape=escape)
        .load(file_location)
  )
  
  return df

# load csv file into target table (target_table_name doesn't require workspace)
def dnb_csv_to_table(file_location, target_table_name, overwrite=False):
  # TODO: check if permanent table exist
  permanent_table_name = f"{schema.workarea}.{target_table_name}"
  
  df = dnb_csv_to_dataframe(file_location)
  
  df.write.format("parquet").saveAsTable(permanent_table_name)

  #print("Table {permanent_table_name} created")
  
  return df, permanent_table_name


def dnb_dict_to_dataframe(d):
  """
    Convert a dictionary list to a dataframe
    
    dict_lst = [{'A': '1','B':'2'},{'A': '111','B':'2222'}]
    df = dict_to_dataframe(dict_lst)
  """
  return spark.read.json(sc.parallelize([d]))



# write data frame to workarea table
def dnb_write_df(df, workarea_tbl_name):
  """
    Saves datafame into a table in your workarea
    
    Parameters:   
         df:        your dataframe
         workarea_tbl_name:     full table name (ex: "dbname.tablename")
                 
    Example: 
         dnb_write_df(mydf, "my_workarea.mytable")
    
  """
  # drop, if same table already exists
  spark.sql(f"drop table if exists {workarea_tbl_name}")
  
  try:
    # write the dataframe in workarea table
    df.write.format("delta").mode("overwrite").saveAsTable(workarea_tbl_name)
    print(f"[INFO] dataframe saved to '{workarea_tbl_name}' table")
  ## Handling exception: when s3 location already exists
  except Exception as e:
    e = str(e)
    if ((e.__contains__("Can not create the managed table")) and (e.__contains__("The associated location")) and (e.__contains__("already exists"))):
      df = spark.sql(f"""describe database {workarea_tbl_name.split(".")[0]}""")
      # to get the s3 location associated with workarea_tbl_name 
      location = df.collect()[2][1]
      print(f"[Handling Exception] {e}")
      # remove s3 location which already exists and has thrown error
      path_to_remove = location + f"/{workarea_tbl_name.split('.')[1]}/"
      print(f"[INFO] Path to remove : {path_to_remove}")
      dbutils.fs.rm(path_to_remove, True)
      # save the dataframe to "workarea_tbl_name"
      df.write.mode("overwrite").saveAsTable(workarea_tbl_name)
      print(f"[INFO] dataframe saved to '{workarea_tbl_name}' table")
    else:
      # if there is any other exception then raise it
      raise Exception(e)

      
def dnb_split_dataframe(df,batch_size=2000):
  """
    Split the dataframe in to multiple dataframes based on the batch_size.
    The number of records will be approximatily what's specified in the batch_size.
    The last file will have the remaining # of records.
    
    Parameters:  df -            your dataframe
                 batch_size -    approx # of records per batch
                 
    Returns:     list of dataframes

    Ex:  
        # creates N number of dataframes about 500 records each   
        dflist = dnb_split_dataframe(mydf, batch_size=500)

  """
  if df == None:
    return []
  
  # split dataframe 
  n_files= math.ceil(df.count() / batch_size)
  retlist = df.randomSplit([1/n_files for x in range(n_files)])
  #counts = [x.count() for x in df_list]
  #print('counts-->', counts)
  return retlist


def dnb_union(database,table_filter=""):
  """
    Union the tables matching the filter criterial
  """
  df = schema.show_tables(database=database, table_filter=table_filter, silent=True)
  try:
      tables = [x.table_name for x in df.collect()]
      
      result_df = None
      for t in tables:
        df_i = spark.table(f"{database}.{t}")
        if result_df == None:
          result_df = df_i
        else:
          result_df = result_df.union(df_i)
      
      return result_df
    
  except e:
      print("Unable to union tables ", e)
      return None
  
  


# COMMAND ----------

# calculate fill rates
def dnb_col_coverage(cols=["mc_employee_sales_segment","buydex"],  db="us_marketing", table="dnb_core_marketing_scores_us", load_month=8, load_year=2020):
  """
  Example: 
  
  # get coverage of 2 fields
  dnb_get_col_coverage(cols=["mc_employee_sales_segment","buydex"], load_month=5, load_year=2020)  

  # get coverage of all fields in target table
  dnb_get_col_coverage(cols=None,load_year=2020, table="dnb_core_marketing_scores_us", db="us_marketing")  
  
  # depends on dnb_schema
  
  """
  
  def make_col(col):
    return f"""\n  sum(if(a.{col} is null or a.{col} = '',0,1)) {col} """
  
  def get_cols(db, table):
    sql=f"show columns in {db}.{table}"
    cols=spark.sql(sql).select("col_name").collect()
    return [x.col_name for x in cols]

  # get all cols in the table if not provided
  if cols is None:
    cols = [x for x in get_cols(db,table)]
    
  if "duns" not in cols:
    cols.append("duns")
  
  # create col sql
  cols_s = ",".join([make_col(x) for x in cols])
  
  sql=f"""
  select a.load_year || '.' || left(cast('0'||a.load_month as varchar(10)),2) as load_date, {cols_s}
  from {db}.{table} a,
       {db}.dnb_core_id_geo_us b
  where a.load_year={load_year} {"" if load_month is None else 'and a.load_month='+str(load_month)}
  and b.load_year={load_year}
  and a.load_month=b.load_month
  and a.duns = b.duns
  and b.active_ind = 1  
  group by 1
  order by 1
  """
  #print(sql)
  df = spark.sql(sql).select(*(["duns"]+[x for x in cols if x != "duns"]))
  display(df)
  

# COMMAND ----------

def dnb_get_cols(db, table):
  """
  Example: dnb_get_cols("us_marketing","dnb_bus_dimensions_us")
  """
  sql=f"show columns in {db}.{table}"
  cols=spark.sql(sql).select("col_name").collect()
  return [x.col_name for x in cols]


# COMMAND ----------

from inspect import signature

def dnb_help(obj=None, include_uat=False):
  """
  Display documentation or signature of python objects, methods, functions, class

  Example:
    dnb_help(dnb_schema.show_databases)
    dnb_help(dnb_generate_random_duns)
    dnb_help(dnb_help)
  """
  try:
    if obj is None:
        uat=[x for x in globals() if ("dnb_uat_" in x) ]
        print(*[x for x in globals() if ("dnb_" in x) and (x not in uat or include_uat)], sep="\n")
        return
        
    doc = obj.__doc__
    sig = str(signature(obj)).replace("\'","\"")
    #print("------------------")
    print(f"{obj.__name__}{sig} \n{obj.__doc__}")
  except:
    if doc:
      print(doc)
    else:
      print(f"No Help found")


# COMMAND ----------

import pkgutil
import importlib as ib
import sys
  
def dnb_get_version(lib):
  imported=False
  path=''
  version=''
  try:
    x = ib.import_module(lib)
    imported=True
    try:
      path=x.__path__[0]
    except:
      path=str(x).split(" ")[-1:][0][1:-2]
    try:
      version=str(x.__version__)
    except:
      version=''
    return {'module': lib, 'version': version, 'path': path, 'err': ''}
  except:
    #error=sys.exc_info()[0]
    return {'module': lib, 'version': version if imported else "import failed", 'path': path, 'err': ''}  
  
  
def dnb_python_modules(show_all=True, filter='', show_exception=False):
  """
    dnb_python_modules(show_all, filter)
    
    Show all available python libraries and the versions. 
    Parameters:
      show_all (bool)            : shows all version & non-version
      filter (string)            : match by library name (contains)      
    
    Ex: dnb_python_modules(filter='xgb')
    
  """
  filter=filter.lower()
  l = list(pkgutil.iter_modules())
  modules=sorted([[x.name.lower(),x.name] for x in l])
  out = [ dnb_get_version(str(x[1])) for i,x in enumerate(modules) ]
  df=dnb_dict_to_dataframe(out)
  
  if show_exception:
    df=df.where(f"version in ('import failed') and left(module,1) != '_' and lower(module) like '%{filter}%'")
    
  else:
    if show_all:
      df=df.where(f"version not in ('import failed')    and left(module,1) != '_' and lower(module) like '%{filter}%'")
    else:
      df=df.where(f"version not in ('import failed','') and left(module,1) != '_' and lower(module) like '%{filter}%'")
    
  return df.select("module","version","path")

def dnb_check_python_packages(show_all=True, filter='', show_exception=False):
  """
  # check for installed R packages
  """
  df = dnb_python_modules(show_all, filter, show_exception)
  display(df)
  
  

def dropDupeDfCols(df):

    df = df.toDF(*[c.lower() for c in df.columns])
    
    newcols = []
    dupcols = []

    for i in range(len(df.columns)):
        if df.columns[i] not in newcols:
            newcols.append(df.columns[i])
        else:
            dupcols.append(i)

    df = df.toDF(*[str(i) for i in range(len(df.columns))])
    for dupcol in dupcols:
        df = df.drop(str(dupcol))

    return df.toDF(*newcols)

# COMMAND ----------

# MAGIC %r
# MAGIC dnb_check_r_packages <- function() {
# MAGIC   df = as.data.frame(installed.packages())
# MAGIC   display(df[order(df$Package),])
# MAGIC }

# COMMAND ----------

import pickle

# provide functionality to pickle binary objects

def dnb_pickle_save(object, object_name):
  """
    object      : model object to be saved
    object_name : unique name of the model 

    Example:

    favorite_color = { "lion": "yellow", "kitty": "red", "tiger": "xorange" }
    dnb_pickle_save(favorite_color, "my_favorite_color")
    
    c = dnb_pickle_load("my_favorite_color")
    print(c)

  """
  try:
    # init file locations & directory
    dbutils.fs.mkdirs("dbfs:/FileStore/models") 
    target_location = f"dbfs:/FileStore/models/{object_name}.pkl"
    src_location = f"/immuta-scratch/{object_name}.pkl"

    pickle.dump( object, open( src_location, "wb" ) )  

    dbutils.fs.cp(f"file:{src_location}", target_location)
    
  except Exception as e:
    print(f'Unable to save file to {src_location}.  Error: {e}')
    
def dnb_pickle_load(object_name):
  """
    object_name : unique name of the model 
    
    Example:

    favorite_color = { "lion": "yellow", "kitty": "red", "tiger": "xorange" }
    dnb_pickle_save(favorite_color, "my_favorite_color")
    
    c = dnb_pickle_load("my_favorite_color")
    print(c)
  """
  try:
    src_location = f"dbfs:/FileStore/models/{object_name}.pkl" 
    target_location = f"/immuta-scratch/{object_name}_restored.pkl"
    dbutils.fs.cp(src_location, f"file:{target_location}")
    return pickle.load( open( target_location, "rb" ) )
  except:
    print(f'unable to load file to {src_location}')
    




# COMMAND ----------

def mlflow_tracking(**kwargs):
    """
      Parameters:
      ___________
      customer_name(str) : Customer Name 
      model_name(str) : Model Name
      model_version(str) : Model Version
      All the above to create MLFlow Run name with timestamp
      log_parameters(dict) : Parameters to be logged to MLFlow run
      log_metrics(dict) : Metrics to be logged to MLFlow run
      model_artifacts(html) : Artifacts(ideally reports) to be logged to MLFlow run
      
      Returns:
      ________
      MLFlow experiment path(str)  

      Usage:
      _______
      mlflow_tracking_check(log_parameters=log_parameters, log_metrics=log_metrics, model_artifacts=model_artifacts, customer_name=customer_name,        model_name=model_name, model_version=model_version)
    """    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    exp_path = notebook_path + '_mlflow_exp'
    user_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    artifact_path = '/dbfs/FileStore/shared_uploads/'+user_id
    print(f"Creating and Setting up MLFlow Tracker: Experiment created at {exp_path}")
    
    try:
        customer_name = kwargs['customer_name']
        model_name = kwargs['model_name']
        model_version = kwargs['model_version']
        log_parameters = kwargs['log_parameters']
        log_metrics = kwargs['log_metrics']
        model_artifacts = kwargs['model_artifacts']
    except KeyError:
      raise KeyError("Please pass the 'customer_name', 'model_name' and 'model_version' for creating the MLFlow run name. Pass the parameters, metrics and artifacts to be logged in the MLFlow run as 'log_parameters', 'log_metrics' and 'model_artifacts' respectively")
        
    run_name = customer_name + '_' + model_name + '_' + model_version + '_' + str(datetime.now())[:-7].replace(' ','_').replace('_','__').replace('-', '_').replace(':','_')

    try:
      # Create mlflow experiment 
      mlflow.create_experiment(exp_path)
    except Exception as e:
      # Set mlflow experiment if already exists
      mlflow.set_experiment(exp_path)
    
    try:    
      mlflow.start_run(run_name=run_name)
      for key, val in log_parameters.items():
        mlflow.log_param(key,val)
      for key, val in log_metrics.items():
        mlflow.log_metric(key,val)

      run = mlflow.active_run()
      run_id = run.info.run_id 

      print("Saving Model Report...") 
      model_artifact_path = artifact_path+'/model_report_'+run_id+'.html'   
      
      with open(model_artifact_path, 'wb') as f:
          joblib.dump(model_artifacts, f)

      print(f"Model Artifact path: {model_artifact_path}")
      
      mlflow.log_artifact(model_artifact_path) # logging model artifacts
      print("MLFlow tracking Completed...") 
      
    finally:
      mlflow.end_run()
      
    return exp_path
  
  
def pandas_to_spark(pandas_df):
    '''
    convert a Pandas DataFrame into a Spark DataFrame
    
    Parameters :
    ----------
    pandas_df (pd.DataFrame) : Pandas Dataframe
    
    Returns :
    -------
    Spark DataFrame
    
    '''
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    
    def equivalent_type(f):
      if f == 'datetime64[ns]': return TY.TimestampType()
      elif f == 'int64': return TY.LongType()
      elif f == 'int32': return TY.IntegerType()
      elif f == 'float64': return TY.DoubleType()
      elif f == 'float32': return TY.FloatType()
      else: return TY.StringType()
    
    def define_structure(string, format_type):
      try: typo = equivalent_type(format_type)
      except: typo = TY.StringType()
      return TY.StructField(string, typo)
    
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = TY.StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)  

# COMMAND ----------

#end of notebook