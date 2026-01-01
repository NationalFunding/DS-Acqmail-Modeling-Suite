import os

import pandas as pd
import sqlalchemy as sa

from config.run_env import STAGING_FOLDER
from config.run_env import DEV_SERVER, PROD_SERVER, UID


class SQLHandler:
    """
    Handler for Database operations. To be used by the Session class with the `.sql` accessor. 
    """

    def __init__(self, session_obj):
        """
        Handler for Database operations. To be used by the Session class with the `.sql` accessor. 

        Methods
        -------
        archive_table(self, table_name, archive_table_name, schema='dbo', create_archive_if_none=True, add_literal_columns=None, literal_column_dtypes=None, overwrite_stale_archive=False)
            Archives a table in the database.
        """

        self.__session = session_obj
        self.__session.log.info(f"Initializing SQLHandler")

        # Grab configuration params
        driver = '{ODBC Driver 17 for SQL Server}'
        server = PROD_SERVER if self.__session.running_in_prod else DEV_SERVER
        database = 'DNB'
        pw = self.__session._credentials.PROD_DB_PW

        # Authorize depending on the run environment
        auth_str = f'UID={UID};PWD={pw}' if self.__session.running_in_prod else 'Trusted_Connection=yes'

        # Create Connection String
        connection_url = f'mssql+pyodbc:///?odbc_connect=Driver={driver};Server={server};Database={database};QuotedID=Yes;AnsiNPW=Yes;'
        connection_url += auth_str

        # Create engine, metadata
        self.engine = sa.create_engine(connection_url, future=True, echo=False)
        self.metadata = sa.MetaData()

    def __call__(self):
        """
        A convenience feature to access a connected context manager by calling the `.sql` accessor. 
        Autocommits when exiting the context manager.

        E.g. 
        >>> with session.sql() as conn:
        >>>     conn.execute(f"INSERT INTO {table_name} VALUES ({values})")
        """

        return self.engine.begin()

    def _create_table(self, table_name, columns, schema):
        """
        Uses SQLAlchemy to create a table in the database. This function is likely to change in the future.
        Uses the already-existing `.metadata` attribute of the SQLHandler to create the table.

        Parameters
        ----------
        table_name : str
            Name of the table to be created
        columns : list
            List of columns in table to create. Must be sqlalchemy.Column objects which specify name and type.
        schema : str
            Name of the schema to create the table in, e.g. 'dbo'
        
        Returns
        -------
        table : sqlalchemy.Table
            Table object created by SQLAlchemy
        """
        table = sa.Table(
            table_name,
            self.metadata,
            *columns,
            schema=schema,
        )

        with self.engine.begin() as conn:
            self.metadata.create_all(bind=conn, tables=[table])

        return table

    def _parse_table_arg(self, table, schema):
        """
        Internal function to parse the table argument.
        Allows the table and schema to be passed in as strings or sqlalchemy.Table/selectable objects.
        This function creates a sqlalchemy selectable object from whatever is passed in.

        Parameters
        ----------
        table : str or sqlalchemy.Table, sa.sql.selectable.Select or sa.sql.selectable.Subquery
        
        """

        if isinstance(table, str):

            if '.' in table:
                raise ValueError(f"Table name cannot contain a '.' Please specify the schema as a seperate argument")

            if not self.table_exists(table_name=table, schema=schema):
                raise ValueError(f"Tried to parse string table arg but table {table} does not exist.")
            
            table = sa.Table(table, self.metadata, schema=schema, autoload_with=self.engine)
        
        elif isinstance(table, sa.sql.selectable.Select):
            table = table.subquery()
        
        elif isinstance(table, sa.sql.selectable.Subquery):
            pass
        
        elif isinstance(table, sa.Table):
            pass
        
        else:
            self.__session.log_and_raise(ValueError(f"Invalid table passed in. Must be str, or sqlalchemy object (table, query, subquery)"))

        return table

    def get_row_count(self, table, schema='dbo', log=True, assert_count=None):
        """
        Returns the number of rows in a table.
        
        Parameters
        ----------
        table : str or sqlalchemy selectable
            Name of the table to count rows in. Can also be a sqlalchemy.Table object.
        schema : str
            Name of the schema of table, e.g. 'dbo'
        log : bool
            Whether to log the row count. Default is True.
        assert_count : callable
            A function that takes a single argument, the row count, and returns a single truthy/falsy value. 
            If the returned value is falsy, the function will raise an ValueError and log details.
            E.g. Pass assert_count=lambda x: x > 0 to make sure the row count is positive.

        Returns
        -------
        row_count : int
        """

        table = self._parse_table_arg(table=table, schema=schema)
        query = sa.select(sa.func.count()).select_from(table)

        with self() as conn:
            row_count = conn.execute(query).scalar()

        if log:
            self.__session.log.info(f"Table {table.fullname} has {row_count:,} rows")

        if assert_count is not None:
            if not assert_count(row_count):
                self.__session.log_and_raise(ValueError(f"Table {table.fullname} has {row_count:,} rows. This contradicts the assertion passed."))

        return row_count

    def table_exists(self, table_name, schema='dbo'):
        """
        Returns True if table exists in the database.

        Parameters
        ----------
        table_name : str
            Name of the table to check for
        schema : str
            Name of the schema to check for the table in, default 'dbo'
        
        Returns
        -------
        exists : bool
        """
        
        if not isinstance(table_name, str):
            self.__session.log_and_raise(ValueError(f"table_name arg must be str"))

        insp = sa.inspect(self.engine)
        exists =  insp.has_table(table_name=table_name, schema=schema)

        return exists

    def archive_table(self, table, archive_table_name, schema='dbo', create_archive_if_none=True, add_literal_columns=None, literal_column_dtypes=None, overwrite_stale_archive=False, ignore_cols=None):
        """
        Archives a (staging) table in the database by taking all rows from the (staging) table, adding column(s) to act as keys, and inserting them into a new or existing (archive) table.

        Parameters
        ----------
        table : str or sqlalchemy.Table
            If str, name of the table to be archived. This table must already exist in the database. 
            If sqlalchemy.Table or sqlalchemy.sql.selectable.Select, the table or query to be archived.
        archive_table_name : str
            Name of the archive table to insert rows into. If the archive table does not exist, and create_archive_if_none is True, the archive table will be created.
        schema : str
            Name of the schema to create the table in, default is 'dbo'
        create_archive_if_none : bool
            If True, and the archive table does not exist, it will be created. If False, and the archive table does not exist, an error will be raised.
        add_literal_columns : list
            List of columns to add to the archive table. These columns will act as keys for the archive table. For example, if we are archiving a table each month, we would add a column called 'month' to the archive table so we know when the row was archived.
            E.g. add_literal_columns = add_literal_columns={'MONTH':datetime.today().strftime("%Y-%m-%d")}
        literal_column_dtypes : list
            List of dtypes for the literal columns. Must be in the same order as add_literal_columns.
            This is only needed in the case when we are adding literal columns (add_literal_columns not None), the archive table does not already exist, and we want to create it (create_archive_if_none == True) since we need to specify the dtype of the column in the new table.
        overwrite_stale_archive : bool
            Rows already in the archive table with the same literal column keys (add_literal_columns) when a new archive is being made are considered *stale* rows.
                E.g. A staging table used once a month is archived with the literal column keys cooresponding to the month. If archived twice in one month, the rows from the first archive will be considered stale since the "Month" column is the same for both archived sets. 
            If True, all *stale* rows in the archive table will be deleted, and the new rows will be inserted, as to overwrite the old rows. A log warning will show the number of stale rows deleted.  
            If False, the new rows will be inserted without deleting any *stale* rows even if they exist. A log warning will show the number of stale rows found (in case exepecting none).
            Does not depend on whether the archive table already exists.
        ignore_cols : list
            List of columns to ignore when archiving. These columns will not be added to the archive table, and will not be inserted into the archive table.
        """
        if ignore_cols is None: ignore_cols = []
        table = self._parse_table_arg(table=table, schema=schema)

        try:
            archive_table = sa.Table(archive_table_name, self.metadata, schema=schema, autoload_with=self.engine)

        except sa.exc.NoSuchTableError as e:
            
            assert not self.table_exists(table_name=archive_table_name, schema=schema), f"Caught NoSuchTableError but table_exists(table_name={archive_table_name}, schema={schema}) returned True. This should not happen."    

            if not create_archive_if_none:
                self.__session.log_and_raise(e, message='Archive table does not exist and create_archive_if_none is False')

            if (add_literal_columns is not None) and (literal_column_dtypes is None):
                self.__session.log_and_raise(ValueError('Tried to create archive table with literal columns but no literal column dtypes were specified. Please specify karg literal_column_dtypes with SQLAlchemy dtype objects'))

            if (add_literal_columns is not None) and len(add_literal_columns) != len(literal_column_dtypes):
                self.__session.log_and_raise(ValueError('Tried to create archive table with literal columns but the number of literal columns and number of literal column dtypes do not match: len(add_literal_columns) != len(literal_column_dtypes)'))

            self.__session.log.info(f"Did not find archive table, creating archive table {archive_table_name}")
            
            columns =  [sa.Column(col.name, col.type) for col in table.columns]
            if add_literal_columns is not None and literal_column_dtypes is not None:
                columns += [sa.Column(column_name, dtype) for column_name, dtype in zip(add_literal_columns, literal_column_dtypes)]

            archive_table = self._create_table(table_name=archive_table_name, columns=columns, schema=schema)

        # columns_to_archive = the columns from the staging table that we want to push to the archive table
        columns_to_archive = [col for col in table.c if col.name not in ignore_cols]
        # columns_to_insert = columns_to_archive + the literal columns we are adding to the archive table, like the date 
        columns_to_insert = columns_to_archive.copy()
        literal_columns = []

        if add_literal_columns:
            literal_columns += [sa.literal(column_value).label(column_name) for column_name, column_value in add_literal_columns.items()]
            columns_to_insert += [c.name for c in literal_columns]

        select_data_to_archive_statement = sa.select(*columns_to_archive, *literal_columns)
        insert_data_to_archive_statement = sa.insert(archive_table).from_select(columns_to_insert, select_data_to_archive_statement)

        with self.engine.begin() as conn:

            if overwrite_stale_archive:
                delete_stale_archive_statement = sa.delete(archive_table).filter_by(**add_literal_columns or {})
                delete_stale_archive_result = conn.execute(delete_stale_archive_statement)
                if (rowcount := delete_stale_archive_result.rowcount) > 0:
                    self.__session.log.warning(f"Deleted {rowcount:,} stale rows from archive table {archive_table_name}")
            else:
                select_stale_archive_statement = sa.select(sa.func.count('*')).select_from(archive_table).filter_by(**add_literal_columns or {})
                select_stale_archive_result = conn.execute(select_stale_archive_statement)
                if (rowcount := select_stale_archive_result.scalar()) > 0:
                    self.__session.log.warning(f"Found {rowcount:,} stale rows from archive table {archive_table_name}. Proceeding to insert new data into archive table without deleting stale data.")

            insert_data_to_archive_result = conn.execute(insert_data_to_archive_statement)
            self.__session.log.info(f"Inserted {insert_data_to_archive_result.rowcount:,} rows into archive table {archive_table_name}")

    def delete_all_rows(self, table, schema='dbo'):
        """
        Delete all rows from a table.

        Parameters
        ----------
        table : str, or sqlalchemy selectable
            (Name of) the table to delete all rows from.
        schema : str, optional (default: 'dbo')
        """
        table = self._parse_table_arg(table, schema=schema)

        with self.engine.begin() as conn:
            delete_statement = sa.delete(table)
            delete_result = conn.execute(delete_statement)
            self.__session.log.info(f"Deleted {delete_result.rowcount:,} rows from table {table.fullname}")

    def drop_table(self, table, schema='dbo'):
        """
        Drops the specified table. Try to use delete_all_rows() instead when possible.

        Parameters
        ----------
        table : str, or sqlalchemy selectable
            (Name of) the table to delete all rows from.
        schema : str, optional (default: 'dbo')
        """
        if isinstance(table, str):
            if not self.table_exists(table, schema=schema):
                self.__session.log.info(f"Cannot find table {table} to drop. Continuing.")
                return 

        table = self._parse_table_arg(table, schema=schema)

        table.drop(self.engine, checkfirst=True)

        self.__session.log.info(f"Dropped table {table.fullname}")

    def _read_sql_script(self, sql_script_path):

        if not os.path.isfile(sql_script_path):
            self.__session.log_and_raise(ValueError(f"SQL script {sql_script_path} does not exist."))

        with open(sql_script_path, 'r') as file:
            raw_query_text = file.read()
        
        return raw_query_text

    def _bind_script_params(self, query_str, script_params):

        sql_alchemy_text = sa.text(query_str)
        if script_params:

            if not isinstance(script_params, dict):
                self.__session.log_and_raise(ValueError(f"script_params must be a dict. Got {type(script_params)} instead."))

            sql_alchemy_text = sql_alchemy_text.bindparams(**script_params)

        return sql_alchemy_text

    def read_script(self, sql_script_path, script_params=None, **kwargs):
        """
        Wrapper for pandas read_sql function so that we can pass SQL script paths and SQL strings. 
        This function does not commit any changes to the database, but will return a pandas dataframe.
        This should be used for queries that have multiple statements, only single statements, like a select statement,

        Parameters
        ----------
        sql_script_path : str
            Path to SQL script to execute.
        script_params : dict, optional
            dictionary of parameters to pass to the sql script
        """

        raw_query_text = self._read_sql_script(sql_script_path)
        sql_alchemy_text = self._bind_script_params(raw_query_text, script_params)

        self.__session.log.info(f"Reading Query from SQL Script {sql_script_path}...")
        with self() as conn:
            df = pd.read_sql_query(sql=sql_alchemy_text, con=conn, params=script_params, **kwargs)
        self.__session.log.info(f"SQL Script Completed")

        return df

    def execute_script(self, sql_script_path, script_params=None):
        """
        Function to run a SQL script at the specified path.
        This is used for executing a script like a stored procedure, which does not return a dataframe or any results.
        You can also pass in optional parameters to the script using the kwargs.
        See this tutorial for more information: https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql    
        THIS WILL NOT RETURN ANY RESULTS. If you want to return results, use the read_script function.

        Parameters
        ----------
        sql_script_path : str
            Path to SQL script to execute.
        script_params : dict, optional
            dictionary of parameters to pass to the sql script
        """

        raw_query_text = self._read_sql_script(sql_script_path)
        sql_alchemy_text = self._bind_script_params(raw_query_text, script_params)

        with self() as conn:
            self.__session.log.info(f"Executing SQL Script {sql_script_path}...")
            conn.execute(sql_alchemy_text)           
            self.__session.log.info(f"SQL Script Completed")

    def bulk_insert_file(self, table_name, schema, file_path, delimiter, first_row):
        """
        Function to bulk insert a file into a table.

        Parameters
        ----------
        table_name : str
            Name of the table to insert into.
        schema : str
            Name of the schema to insert into.
        file_path : str
            Path to the file to insert.
        delimiter : str
            Delimiter used in the file e.g. "," or "|"
        first_row : int
            The first row of the file to start inserting from

        """
        sql = f"BULK INSERT {schema}.{table_name} FROM '{file_path}' WITH (FIELDTERMINATOR = '{delimiter}', FIRSTROW={first_row})"
        
        sql_alchemy_text = sa.text(sql)

        with self() as conn:
            self.__session.log.info(f"Bulk Inserting file {file_path} into {table_name}...")
            conn.execute(sql_alchemy_text)           
            self.__session.log.info(f"Bulk Insert Completed")

    def bulk_insert_df(self, table_name, schema, df):
        """
        Function to bulk insert a dataframe into a table. 
        This is an alternative to using the `.to_sql` method which is extremely slow for SQL Server.

        Parameters
        ----------
        table_name : str
            Name of the table to insert into.
        schema : str
            Name of the schema to insert into.
        df : pandas dataframe
            Dataframe to insert into the table. This will automatically be written to a delimited file and then bulk inserted.
        """
        temp_file_path = STAGING_FOLDER/'_temp_for_bulk_insert.csv'
        df.to_csv(temp_file_path, sep = '|', index=False)
        
        self.bulk_insert_file(
            table_name=table_name, 
            schema=schema,
            file_path=temp_file_path,
            delimiter='|',
            first_row=2
            )

        os.remove(temp_file_path)


