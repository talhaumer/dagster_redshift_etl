import time
import os
from typing import Optional, Dict, Any, List, Union
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
import psutil
import boto3
from io import StringIO
import csv
import logging
from psycopg2 import DatabaseError as Psycopg2DatabaseError
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError as SQLADatabaseError
from sqlalchemy import inspect, text, DDL
from botocore.exceptions import ClientError
from psycopg2 import DatabaseError  # Recommended for Redshift

logger = logging.getLogger(__name__)


class RedshiftDataHandler:
    """
    Optimized handler for Redshift data operations with pandas DataFrames.

    This class provides methods for:
    - Connecting to Redshift using SQLAlchemy
    - Preprocessing pandas DataFrames for Redshift compatibility
    - Writing DataFrames to Redshift (direct insert or S3 COPY)
    - Generating Redshift DDL from DataFrame schema
    - Replacing or appending data with conflict resolution
    - Cleaning up resources

    Attributes:
        TYPE_MAPPING (dict): Maps pandas dtypes to Redshift SQL types.
        config (dict): Redshift and AWS configuration.
        engine (sqlalchemy.Engine): SQLAlchemy engine for Redshift.
        s3_client (boto3.client): Boto3 S3 client for file uploads.
    """

    TYPE_MAPPING = {
        "object": "VARCHAR(255)",
        "string": "VARCHAR(255)",
        "int64": "BIGINT",
        "int32": "INTEGER",
        "int16": "SMALLINT",
        "float64": "DOUBLE PRECISION",
        "float32": "REAL",
        "bool": "BOOLEAN",
        "datetime64[ns]": "TIMESTAMP",
        "timedelta64[ns]": "BIGINT",
    }

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize RedshiftDataHandler with configuration.

        Args:
            config (dict): Redshift and AWS connection parameters.
        """
        self.config = config
        self.engine = self._create_engine()
        self.s3_client = boto3.client(
            "s3",
            region_name=config.get("aws_region", "eu-west-2"),
            aws_access_key_id=config.get("S3_KEY"),
            aws_secret_access_key=config.get("S3_SECRET"),
        )
        self._validate_connection()

    def _validate_connection(self) -> None:
        """
        Validate the Redshift connection with detailed error handling.

        Raises:
            DatabaseError: If connection fails.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except sa.exc.DBAPIError as e:
            logger.error("Database API error during connection: %s", str(e))
            raise DatabaseError(f"Database connection failed: {e.orig}") from e
        except sa.exc.SQLAlchemyError as e:
            logger.error("SQLAlchemy error during connection: %s", str(e))
            raise DatabaseError(f"Connection setup failed: {str(e)}") from e
        except Exception as e:
            logger.error("Unexpected connection error: %s", str(e))
            raise DatabaseError(f"Connection validation failed: {str(e)}") from e

    def _create_engine(self) -> Engine:
        """
        Create optimized SQLAlchemy engine with connection pooling.

        Returns:
            sqlalchemy.Engine: SQLAlchemy engine for Redshift.
        """
        connection_string = (
            f"postgresql+psycopg2://{self.config['user']}:"
            f"{quote_plus(self.config['password'])}@"
            f"{self.config['host']}:{self.config.get('port', 5439)}/"
            f"{self.config['database']}"
        )

        return sa.create_engine(
            connection_string,
            connect_args={"sslmode": "require"},
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True,
            execution_options={"isolation_level": "AUTOCOMMIT"},
        )

    def _preprocess_dataframe(
        self, df: pd.DataFrame, max_varchar: int = 65535
    ) -> pd.DataFrame:
        """
        Preprocess DataFrame for Redshift compatibility.

        Args:
            df (pd.DataFrame): Input DataFrame.
            max_varchar (int): Maximum VARCHAR length for string columns.

        Returns:
            pd.DataFrame: Preprocessed DataFrame.
        """
        processors = [
            self._convert_datetimes,
            self._process_strings,
            self._optimize_integers,
            self._handle_special_types,
            self._standardize_nulls,
        ]

        for processor in processors:
            df = processor(df, max_varchar)
        return df

    def _convert_datetimes(self, df: pd.DataFrame, _) -> pd.DataFrame:
        """
        Convert timezone-aware datetimes to naive and coerce errors.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with processed datetime columns.
        """
        dt_cols = df.select_dtypes(include=["datetime"]).columns
        for col in dt_cols:
            try:
                df[col] = df[col].dt.tz_localize(None)
            except Exception:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def _process_strings(self, df: pd.DataFrame, max_len: int) -> pd.DataFrame:
        """
        Convert columns to string and truncate to max_len.

        Args:
            df (pd.DataFrame): Input DataFrame.
            max_len (int): Maximum string length.

        Returns:
            pd.DataFrame: DataFrame with processed string columns.
        """
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        for col in str_cols:
            df[col] = df[col].astype(str)
            lens = df[col].str.len()
            if lens.max() > max_len:
                df[col] = df[col].str.slice(0, max_len)
        return df

    def _optimize_integers(self, df: pd.DataFrame, _) -> pd.DataFrame:
        """
        Downcast integer columns based on value ranges.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with optimized integer columns.
        """
        int_cols = df.select_dtypes(include=["int"]).columns
        for col in int_cols:
            vals = df[col]
            if vals.abs().max() > 2**31:
                df[col] = vals.astype("float64")
            elif vals.abs().max() > 2**15:
                df[col] = vals.astype("int32")
            else:
                df[col] = vals.astype("int16")
        return df

    def _handle_special_types(self, df: pd.DataFrame, _) -> pd.DataFrame:
        """
        Convert boolean columns to int8.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with processed boolean columns.
        """
        bool_cols = df.select_dtypes(include=["bool"]).columns
        for col in bool_cols:
            df[col] = df[col].astype("int8")
        return df

    def _standardize_nulls(self, df: pd.DataFrame, _) -> pd.DataFrame:
        """
        Standardize all null representations to None.

        Args:
            df (pd.DataFrame): Input DataFrame.

        Returns:
            pd.DataFrame: DataFrame with standardized nulls.
        """
        return df.replace([np.nan, pd.NA, "NULL", "null", ".", "NaN", "nan"], None)

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = None,
        if_exists: str = "append",
        chunksize: int = 10000,
    ) -> None:
        """
        Write DataFrame to Redshift using the optimal method.

        Uses S3 COPY for large DataFrames, direct insert for small ones.

        Args:
            df (pd.DataFrame): DataFrame to write.
            table_name (str): Target table name.
            schema (str, optional): Target schema. Defaults to config or 'public'.
            if_exists (str): Behavior if table exists ('append', 'replace', etc.).
            chunksize (int): Chunk size for direct insert.
        """
        target_schema = schema or self.config.get("schema", "public")

        if len(df) > 100000:  # Use S3 COPY for large datasets
            self._bulk_load_via_s3(df, table_name, target_schema)
        else:
            self._direct_insert(df, table_name, target_schema, if_exists, chunksize)

    def _bulk_load_via_s3(self, df: pd.DataFrame, table_name: str, schema: str) -> None:
        """
        Load DataFrame to Redshift using S3 and COPY command.

        Args:
            df (pd.DataFrame): DataFrame to load.
            table_name (str): Target table name.
            schema (str): Target schema.
        """
        s3_bucket = self.config.get("s3_bucket", "mb-analytics-redshift")
        s3_key = f"temp_redshift_data/{table_name}_{int(time.time())}.json"

        try:
            # Write DataFrame to JSON in memory
            json_buffer = StringIO()
            df.to_json(json_buffer, orient="records", lines=True)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=s3_bucket, Key=s3_key, Body=json_buffer.getvalue()
            )

            # Generate DDL and create table if not exists
            create_ddl = self.generate_table_ddl(
                df=df, table_name=table_name, schema=schema
            )
            with self.engine.begin() as conn:
                inspector = inspect(self.engine)
                table_exists = inspector.has_table(table_name, schema=schema)

                if not table_exists:
                    conn.execute(create_ddl)
                    logger.info(f"Table {schema}.{table_name} created successfully")
                else:
                    logger.info(
                        f"Table {schema}.{table_name} already exists - skipping creation"
                    )

            # Run COPY command to load data from S3
            copy_sql = text(
                f"""
                COPY {schema}.{table_name}
                FROM 's3://{s3_bucket}/{s3_key}'
                CREDENTIALS 'aws_iam_role={self.config["AWS_IAM_ROLE"]}'
                FORMAT AS JSON 'auto ignorecase'
                TIMEFORMAT 'epochmillisecs'
                TRUNCATECOLUMNS
                ACCEPTINVCHARS
                MAXERROR 100;
            """
            )

            with self.engine.begin() as conn:
                conn.execute(copy_sql)
                logger.info(
                    f"Copy from {s3_key} to {schema}.{table_name} completed successfully."
                )

        finally:
            # Clean up S3 temp file
            self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)

    def _direct_insert(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: str,
        chunksize: int,
    ) -> None:
        """
        Insert DataFrame directly into Redshift for small datasets.

        Args:
            df (pd.DataFrame): DataFrame to insert.
            table_name (str): Target table name.
            schema (str): Target schema.
            if_exists (str): Behavior if table exists.
            chunksize (int): Chunk size for insertion.
        """
        with self.engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=chunksize,
            )
        logger.info(
            f"Direct data load to {schema}.{table_name} completed successfully."
        )

    def generate_table_ddl(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "public",
        distkey: str = None,
        sortkey: Union[str, List[str]] = None,
    ) -> str:
        """
        Generate Redshift CREATE TABLE DDL from DataFrame schema.

        Args:
            df (pd.DataFrame): DataFrame to infer schema from.
            table_name (str): Target table name.
            schema (str): Target schema.
            distkey (str, optional): Redshift DISTKEY column.
            sortkey (str or list, optional): Redshift SORTKEY column(s).

        Returns:
            str: CREATE TABLE DDL statement.
        """
        columns = []
        for col, dtype in df.dtypes.items():
            rs_type = self.TYPE_MAPPING.get(dtype.name, "VARCHAR(255)")
            if dtype.kind in ["O", "S"]:
                max_len = df[col].astype(str).str.len().max()
                rs_type = f"VARCHAR({min(int(max_len * 1.3), 65535)})"
            columns.append(f"{col} {rs_type}")

        column_defs = ",\n  ".join(columns)
        ddl = f"CREATE TABLE {schema}.{table_name} (\n  {column_defs}\n)"

        if distkey:
            ddl += f"\nDISTKEY({distkey})"
        if sortkey:
            keys = sortkey if isinstance(sortkey, list) else [sortkey]
            ddl += f"\nSORTKEY({', '.join(keys)})"

        return ddl

    def close(self) -> None:
        """
        Dispose of the SQLAlchemy engine and clean up resources.
        """
        if hasattr(self, "engine"):
            self.engine.dispose()

    def replace_data(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = None,
        chunksize: int = 10000,
    ) -> None:
        """
        Completely replaces table data by first truncating the table and then inserting new data.

        Args:
            df (pd.DataFrame): DataFrame containing data to insert.
            table_name (str): Name of the target table.
            schema (str, optional): Database schema name (defaults to config schema or 'public').
            chunksize (int): Number of rows to insert at a time (for direct insert method).
        """
        target_schema = schema or self.config.get("schema", "public")

        # First truncate the table to remove all existing data
        with self.engine.begin() as conn:
            conn.execute(f"TRUNCATE TABLE {target_schema}.{table_name}")

        # Then insert new data using the appropriate method
        if len(df) > 100000:  # Use S3 COPY for large datasets
            self._bulk_load_via_s3(df, table_name, target_schema)
        else:
            self._direct_insert(df, table_name, target_schema, "append", chunksize)

    def append_with_id_conflict_resolution(
        self,
        df: pd.DataFrame,
        table_name: str,
        id_column: str,
        schema: str = None,
        chunksize: int = 10000,
    ) -> None:
        """
        Append data to a table with conflict resolution on a specified ID column.
        Removes existing records matching IDs in the new data before appending.

        Args:
            df (pd.DataFrame): DataFrame containing data to append.
            table_name (str): Name of the target table.
            id_column (str): Name of the column used for duplicate detection.
            schema (str, optional): Database schema name (defaults to config schema or 'public').
            chunksize (int): Number of rows to insert at a time.
        Raises:
            ValueError: If id_column is not present in DataFrame.
        """
        if id_column not in df.columns:
            raise ValueError(f"ID column '{id_column}' not found in DataFrame")

        target_schema = schema or self.config.get("schema", "public")
        ids_to_replace = df[id_column].unique().tolist()
        quoted_table = f'"{target_schema}"."{table_name}"'

        with self.engine.begin() as conn:
            inspector = inspect(self.engine)
            table_exists = inspector.has_table(table_name, schema=schema)

            if table_exists:
                # Delete existing records with matching IDs
                # Use parameter binding for safety
                placeholders = ",".join(["%s"] * len(ids_to_replace))
                delete_query = (
                    f"DELETE FROM {quoted_table} WHERE {id_column} IN ({placeholders})"
                )
                conn.execute(delete_query, ids_to_replace)

        # Append new data using the appropriate method
        if len(df) > 100000:  # Use S3 COPY for large datasets
            self._bulk_load_via_s3(df, table_name, target_schema)
        else:
            self._direct_insert(df, table_name, target_schema, "append", chunksize)
