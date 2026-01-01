# Databricks notebook source
#**********************************
# (INTERNAL) DNB UAT FUNCTIONS
#**********************************
def dnb_uat_distribution_month(load_year=2019, load_month=5, exclude_db=[]):
#def dnb_uat_distribution_month(load_year=2019, load_month=5, db=['us_marketing_3yrs','us_risk_3yrs']):
  """
  Run counts of all tables from all databases & return as dataframe for a specific month
  """

  def show_tables(db):
      df = spark.sql(f"SHOW tables in {db}")
      return df
  
  dbs = schema.show_databases(silent=True)
  exclude_db = exclude_db + ['default','user_reference','immuta','dnb_data_globalarea']   # alway exclude these & workarea
  db_list = [x.databaseName for x in dbs.take(100) if (x.databaseName not in exclude_db and "workarea" not in x.databaseName ) ] 
  load_date= str(load_year) + "." + ("0"+str(load_month))[-2:]

  # check all tables
  out = []
  for db in db_list:
    #tablesdf = schema.show_tables(database=db, silent=True)
    tablesdf = show_tables(db)
    for t in tablesdf.take(2000):
      try:
        # todo: try catch here in case problem with reading table
        table = t.database + "." + t.tableName
        print(f'table-->{table}')
        df=spark.table(table)
        first_col = df.columns[0]
        if t.database != "user_reference":
          df = spark.sql(f"select {first_col} from {table} where load_year={load_year} and load_month={load_month} /*limit 5*/")
        else:
          df = spark.sql(f"select {first_col} from {table}")
        cnt = df.count()
        out.append({'load_date':load_date, 'database':  db, 'table': t.tableName,  'count': cnt,  'status': 'ok'})
      except Exception as e:
        print('Exception-->', type(e).__name__, e)
        out.append({'load_date':load_date, 'database':  db, 'table': t.tableName,  'count': 0,  'status': "--- Error ---"})
  
  result = dnb_dict_to_dataframe(out)
  return result.select("load_date","database","table","count","status")

def dnb_uat_distribution_test(start_year=2017, max_months=120, month_filter=[], table="dnb_uat_distribution", 
                              exclude_db=['default','user_reference','immuta','dnb_data_globalarea','alternative_sources','dnb_bundles_prd']):
  """
    (INTERNAL) verify data for a set of months 
    dnb_uat_distribution_test(start_year=2017, max_months=120, month_filter=[1,4,7,10])
  """
  months = list(map(
      date.isoformat,
      rrule(MONTHLY, dtstart=date(start_year, 1, 1), until=date.today())
  ))
  # get list of months in reverse order
  month_list = [ [int(y) for y in x.split("-")][:2] for x in months[::-1] ]
  
  if len(month_filter) > 0:
    month_list = [ x for x in month_list if x[1] in month_filter ]
  
  # limit # of months
  month_list = month_list[:max_months]
  print(month_list)

  result = None
  for x in month_list:
    df = dnb_uat_distribution_month(load_year=x[0], load_month=x[1], exclude_db=exclude_db)
    if result is None:
      result = df
    else:
      result = result.union(df)

  workarea = schema.get_workarea_table()
  dnb_write_df(result,f"{workarea}.{table}")
  
  return result


# (INTERNAL) verify workarea is writable
def dnb_uat_writes_test(table="dnb_uat_writable_test"):
  """
    UAT - Verify workarea is writable
  """
  workarea = schema.workarea
  if workarea == "":
    return None
  else:
    # create data table
    dict_lst = [{'A': 111,'B': 222},{'A': 333,'B': 444}]
    input_df = spark.read.json(sc.parallelize([dict_lst]))  
    try:
      dnb_write_df(input_df, table=f"{workarea}.{table}")
      print(f"BVT: table write ok")
      return input_df
    except:
      print(f"BVT: table write FAILED - unable to write to {workarea}")


# (INTERNAL) UAT functions
def dnb_uat_check_load_dates(table="us_risk.collections_risk_score_us"):
  sql=f"""select min(load_year || '.' || left(cast('0'||load_month as varchar(10)),2)) as min_dt,
                 max(load_year || '.' || left(cast('0'||load_month as varchar(10)),2)) as max_dt 
          from {table}"""
  df = spark.sql(sql)
  out = df.take(1)
  return out

# count of tables by database
def dnb_uat_user_reference_table_count():
  
  def get_table_counts(exclude_db=['default','dnb_bundles_prd','immuta']):
    df = spark.sql("show databases")
    t_list = []
    tables = [x.databaseName for x in df.collect() if x.databaseName not in exclude_db and ("workarea" not in x.databaseName) ]
    for x in tables:
      n_tables = spark.sql(f"show tables in {x}").count()
      print(x, n_tables)
      t_list.append({'database':x, 'actual_tbl_cnt':n_tables})

    out = dnb_dict_to_dataframe(t_list)
    return out
  
  def ref_tbl_count_flag(df_pandas):
    if (df_pandas[df_pandas['database'] == 'user_reference']['actual_tbl_cnt'].values[0] - df_pandas[df_pandas['database'] == 'user_reference']['user_ref_table_cnt'].values[0]) <= 6: 
        df_pandas = df_pandas[(~df_pandas.database.str.contains('tkn')) & (df_pandas.database != 'user_reference')]
        flag = 'Pass - Actual vs User Reference Table count matched' if list(df_pandas.actual_tbl_cnt) == list(df_pandas.user_ref_table_cnt) else 'Fail - Actual vs User Reference Table count mismatch'
        return flag
    else:
      flag = 'Fail - Actual vs User Reference Table count mismatch'
      return flag

  
  schema_df = get_table_counts()
  #display(schema_df.select("database","actual_tbl_cnt"))

  sql="""select database, count(1) as user_ref_table_cnt from user_reference.data_dictionary_table_summary_v2 
  group by database"""
  user_ref_df = spark.sql(sql)

#   df = user_ref_df.join(schema_df, on="database", how="left")
  df = schema_df.join(user_ref_df, on="database", how="left")
  display(df.sort("database"))

# Adding the below lines
  df_pandas = df.sort("database").toPandas()
  
  ref_tbl_flag = ref_tbl_count_flag(df_pandas)

  return ref_tbl_flag



# COMMAND ----------

def dnb_uat_data_audit(max_months=48, start_year=2017, run_all=True, table=f"dnb_uat_distribution", include_db=[]):
  if run_all:
    df = dnb_uat_distribution_test(max_months=max_months, start_year=start_year, table=table) #, month_filter=[1,4,7,10])
    df.show()

  else:
    # keep list
    #include_db = ['us_marketing','alternative_sources','significant_events_us']

    # exclude list
    dbs = spark.sql("show databases")
    exclude_db = [x.databaseName for x in dbs.collect() if x.databaseName not in include_db]

    # rull the test
    df = dnb_uat_distribution_test(max_months=max_months, start_year=start_year, table=table, exclude_db=exclude_db) #, month_filter=[1,4,7,10]
    df.show()


# COMMAND ----------

# test if network is locked
def dnb_uat_network_test():
  try:
    import urllib.request
    with urllib.request.urlopen('http://www.google.com/') as f:
        html = f.read()   
    print('BVT: network test FAILED - network is open')    
  except Exception as e:
    print('BVT: network test - status ok')     


# COMMAND ----------

# end of notebook