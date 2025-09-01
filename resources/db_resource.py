from dagster import resource, Failure
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


@resource(config_schema={"connection_string": str})
def db_engine_resource(context):
    """
    Dagster resource that provides a SQLAlchemy engine.

    Config:
        connection_string (str): The database connection string.

    Returns:
        sqlalchemy.engine.Engine: SQLAlchemy engine instance.

    Raises:
        dagster.Failure: If the engine cannot be created.
    """
    try:
        engine = create_engine(context.resource_config["connection_string"])
        # Optionally test connection
        with engine.connect() as conn:
            pass
        return engine
    except SQLAlchemyError as e:
        raise Failure(description=f"Failed to create SQLAlchemy engine: {str(e)}")
