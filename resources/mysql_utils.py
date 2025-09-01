import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)


def get_mysql_engine(config: dict):
    """
    Create a SQLAlchemy engine for MySQL using the provided configuration.

    Args:
        config (dict): Dictionary with MySQL connection parameters.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance.

    Raises:
        KeyError: If required config keys are missing.
        SQLAlchemyError: If engine creation fails.
    """
    try:
        user = config["MYSQL_USER"]
        password = quote_plus(config["MYSQL_PASSWORD"])
        host = config["MYSQL_HOST"]
        port = config["MYSQL_PORT"]
        database = config["MYSQL_DATABASE"]
        conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(conn_str, pool_recycle=3600)
        return engine
    except KeyError as e:
        logger.error(f"Missing MySQL config key: {e}")
        raise
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error creating engine: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating MySQL engine: {e}")
        raise


def read_mysql_table(query: str, config: dict, chunksize: int = None) -> pd.DataFrame:
    """
    Read data from a MySQL table or query into a pandas DataFrame.

    Args:
        query (str): SQL query or table name.
        config (dict): MySQL connection configuration.
        chunksize (int, optional): Number of rows per chunk for chunked reading.

    Returns:
        pd.DataFrame: DataFrame containing the query results.

    Raises:
        SQLAlchemyError: If SQL execution fails.
        Exception: For any other errors.
    """
    engine = None
    try:
        engine = get_mysql_engine(config)
        if chunksize:
            chunks = []
            for chunk in pd.read_sql(query, engine, chunksize=chunksize):
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_sql(query, engine)
        return df
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error during read: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error reading MySQL table: {e}")
        raise
    finally:
        if engine:
            engine.dispose()
