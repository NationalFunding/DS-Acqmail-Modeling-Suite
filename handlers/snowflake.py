
import os

import pandas as pd
from snowflake.sqlalchemy import URL
import sqlalchemy as sa

from config.run_env import SNOWFLAKE_USER, STAGING_FOLDER, SNOWFLAKE_PROD, SNOWFLAKE_DEV, SNOWFLAKE_HIGHTOUGH_ROLE

class SnowflakeHandler:
    """
    Handler for Database operations. To be used by the Session class with the `.snowflake` accessor. 
    """

    def __init__(self, session_obj):
        """
        Handler for Database operations. To be used by the Session class with the `.snowflake` accessor. 

        Methods
        -------
        archive_table(self, table_name, archive_table_name, create_archive_if_none=True, add_literal_columns=None, literal_column_dtypes=None, overwrite_stale_archive=False)
            Archives a table in the database.
        """

        # Create engine, metadata
        self.__session = session_obj
        self.__session.log.info(f"Initializing SnowflakeHandler")

        snowflake_server = SNOWFLAKE_PROD['server'] if self.__session.running_in_prod else SNOWFLAKE_DEV['server']
        snowflake_warehouse = SNOWFLAKE_PROD['warehouse'] if self.__session.running_in_prod else SNOWFLAKE_DEV['warehouse']

        if SNOWFLAKE_HIGHTOUGH_ROLE and SNOWFLAKE_USER == 'GNOLASCO@FAIRSQUARE.COM':
            self.engine = sa.create_engine(URL(
                        account=f'{snowflake_server}.us-west-2',
                        user=SNOWFLAKE_USER,
                        authenticator = 'externalbrowser',
                        schema = 'ACQUISITIONMAIL',
                        database='DNB',
                        warehouse = 'NEAR_REALTIME',
                        role = 'HIGHTOUCH_ROLE'
            ))
        else:
            self.engine = sa.create_engine(URL(
                        account=f'{snowflake_server}.us-west-2',
                        user=SNOWFLAKE_USER,
                        authenticator = 'externalbrowser',
                        schema = 'ACQUISITIONMAIL',
                        database='DNB',
                        warehouse = snowflake_warehouse,
                        role = 'DSUSER'
            ))
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

    def _parse_table_arg(self, table):
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

            if not self.table_exists(table_name=table):
                raise ValueError(f"Tried to parse string table arg but table {table} does not exist.")
            
            table = sa.Table(table, self.metadata, autoload_with=self.engine)
        
        elif isinstance(table, sa.sql.selectable.Select):
            table = table.subquery()
        
        elif isinstance(table, sa.sql.selectable.Subquery):
            pass
        
        elif isinstance(table, sa.Table):
            pass
        
        else:
            self.__session.log_and_raise(ValueError(f"Invalid table passed in. Must be str, or sqlalchemy object (table, query, subquery)"))

        return table

    def _create_table(self, table_name, columns):
        """
        Uses SQLAlchemy to create a table in the database. This function is likely to change in the future.
        Uses the already-existing `.metadata` attribute of the SnowflakeHandler to create the table.

        Parameters
        ----------
        table_name : str
            Name of the table to be created
        columns : list
            List of columns in table to create. Must be sqlalchemy.Column objects which specify name and type.
        
        Returns
        -------
        table : sqlalchemy.Table
            Table object created by SQLAlchemy
        """
        table = sa.Table(
            table_name,
            self.metadata,
            *columns
        )

        with self.engine.begin() as conn:
            self.metadata.create_all(bind=conn, tables=[table])

        return table
    
    def get_row_count(self, table, log=True, assert_count=None):
        """
        Returns the number of rows in a table.
        
        Parameters
        ----------
        table : str or sqlalchemy selectable
            Name of the table to count rows in. Can also be a sqlalchemy.Table object.
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

        table = self._parse_table_arg(table=table)
        query = sa.select(sa.func.count()).select_from(table)

        with self() as conn:
            row_count = conn.execute(query).scalar()

        if log:
            self.__session.log.info(f"Table {table.fullname} has {row_count:,} rows")

        if assert_count is not None:
            if not assert_count(row_count):
                self.__session.log_and_raise(ValueError(f"Table {table.fullname} has {row_count:,} rows. This contradicts the assertion passed."))

        return row_count
    
    def table_exists(self, table_name):
        """
        Returns True if table exists in the database.

        Parameters
        ----------
        table_name : str
            Name of the table to check for
        
        Returns
        -------
        exists : bool
        """
        
        if not isinstance(table_name, str):
            self.__session.log_and_raise(ValueError(f"table_name arg must be str"))

        insp = sa.inspect(self.engine)
        exists =  insp.has_table(table_name=table_name)

        return exists
    
    def delete_all_rows(self, table):
        """
        Delete all rows from a table.

        Parameters
        ----------
        table : str, or sqlalchemy selectable
            (Name of) the table to delete all rows from.
        """
        table = self._parse_table_arg(table)

        with self.engine.begin() as conn:
            delete_statement = sa.delete(table)
            delete_result = conn.execute(delete_statement)
            self.__session.log.info(f"Deleted {delete_result.rowcount:,} rows from table {table.fullname}")
    
    def drop_table(self, table):
        """
        Drops the specified table. Try to use delete_all_rows() instead when possible.

        Parameters
        ----------
        table : str, or sqlalchemy selectable
            (Name of) the table to delete all rows from.
        """
        if isinstance(table, str):
            if not self.table_exists(table):
                self.__session.log.info(f"Cannot find table {table} to drop. Continuing.")
                return 

        table = self._parse_table_arg(table)

        table.drop(self.engine, checkfirst=True)

        self.__session.log.info(f"Dropped table {table.fullname}")

    def execute_script(self, sql_script_path, script_params=None):
        """
        Function to run a SQL script at the specified path.
        This is used for executing a script like a stored procedure, which does not return a dataframe or any results.
        You can also pass in optional parameters to the script using the kwargs.
        This is similar to execute_script in SQL handler, but the only difference is that you have to loop through each query since Snowflake doesn't support multiple statements in one query.
        THIS WILL NOT RETURN ANY RESULTS. If you want to return results, use the read_script function.

        Parameters
        ----------
        sql_script_path : str
            Path to SQL script to execute.
        script_params : dict, optional
            dictionary of parameters to pass to the sql script
        """
        raw_query_text = self._read_sql_script(sql_script_path, script_params)
        raw_query_text_list = raw_query_text.replace('\n', ' ').split(';')
        query_text_list = [sa.text(query) for query in raw_query_text_list]

        with self() as conn:
            self.__session.log.info(f"Executing SQL Script {sql_script_path}...")
            for query in query_text_list:
                conn.execute(query)
            self.__session.log.info(f"SQL Script Completed")

    def read_script(self, sql_script_path, script_params=None, **kwargs):
        """
        Wrapper for pandas read_sql function so that we can pass SQL script paths and SQL strings. 
        This function does not commit any changes to the database, but will return a pandas dataframe.
        This should be used for queries with single statements, like a select statement.

        Parameters
        ----------
        sql_script_path : str
            Path to SQL script to execute.
        script_params : dict, optional
            dictionary of parameters to pass to the sql script
        """

        raw_query_text = self._read_sql_script(sql_script_path, script_params)

        self.__session.log.info(f"Reading Query from SQL Script {sql_script_path}...")
        with self() as conn:
            df = pd.read_sql_query(sql=raw_query_text, con=conn,  **kwargs)
        self.__session.log.info(f"SQL Script Completed")

        return df
        
    def _read_sql_script(self, sql_script_path, script_dict = None):

        if not os.path.isfile(sql_script_path):
            self.__session.log_and_raise(ValueError(f"SQL script {sql_script_path} does not exist."))

        with open(sql_script_path, 'r') as file:
            raw_query_text = file.read()

        if script_dict is not None:
            raw_query_text = raw_query_text.format(**script_dict).replace('\\', '/')
        
        return raw_query_text
    

    def bulk_insert_file(self, table_name, stage_name, file_path, type_file, **kwargs):
        """
        Function to bulk insert a file into a Snowflake table.

        Parameters
        ----------
        table_name : str
            Name of the table to insert into.
        stage_name : str
            Name of the Snowflake stage.
        file_path : str
            Local file path to upload.
        type_file : str
            File type ('CSV', 'PARQUET', etc.)
        kwargs :
            Optional file format options like FIELD_DELIMITER, SKIP_HEADER, etc.
        """
   

        if '\\' in str(file_path):
            file_path = str(file_path).replace('\\', '/')

        file_name = os.path.basename(file_path)
        type_file = type_file.upper()

        conn = self.engine.raw_connection()  # ✅ Native Snowflake connection
        cursor = conn.cursor()

        try:
            self.__session.log.info(f"Bulk Inserting file {file_path} into {table_name}...")

            # Upload file to Snowflake stage
            cursor.execute(f"PUT 'file://{file_path}' @{stage_name} auto_compress=False")

            # Construct COPY INTO command
            if len(kwargs) == 0:
                if type_file == 'PARQUET':
                    copy_cmd = f"""
                        COPY INTO {table_name}
                        FROM @{stage_name}
                        FILES = ('{file_name}')
                        FILE_FORMAT = (TYPE = '{type_file}')
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    """
                else:
                    copy_cmd = f"""
                        COPY INTO {table_name}
                        FROM @{stage_name}
                        FILES = ('{file_name}')
                        FILE_FORMAT = (TYPE = '{type_file}')
                    """
            else:
                options = ' '.join(f"{k.upper()}={v}" for k, v in kwargs.items())
                match_by = 'MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE' if type_file == 'PARQUET' else ''
                copy_cmd = f"""
                    COPY INTO {table_name}
                    FROM @{stage_name}
                    FILES = ('{file_name}')
                    FILE_FORMAT = (TYPE = {type_file} {options})
                    {match_by}
                """

            # Execute COPY INTO
            cursor.execute(copy_cmd)

            # Clean up: remove file from stage
            cursor.execute(f"REMOVE @{stage_name}/{file_name}")

            self.__session.log.info("Bulk Insert Completed")

            conn.commit()

        finally:
            cursor.close()
            conn.close()

    def bulk_insert_df(self, table_name, stage_name, df):
        """
        Function to bulk insert a dataframe into a table. 
        This is an alternative to using the `.to_sql` method which is extremely slow for SQL Server.

        Parameters
        ----------
        table_name : str
            Name of the table to insert into.
        df : pandas dataframe
            Dataframe to insert into the table. This will automatically be written to a delimited file and then bulk inserted.
        """
        temp_file_path = STAGING_FOLDER/'_temp_for_bulk_insert.csv'
        df.to_csv(temp_file_path, sep = '|', index=False)
        
        self.bulk_insert_file(
            table_name=table_name,
            stage_name=stage_name,
            file_path=temp_file_path,
            type_file = 'CSV',
            FIELD_DELIMITER = "'|'",
            SKIP_HEADER = 1
            )

        os.remove(temp_file_path)