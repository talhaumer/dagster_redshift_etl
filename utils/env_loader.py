from dotenv import load_dotenv
import os


def load_env() -> dict:
    load_dotenv()
    config = {
        # MySQL
        "MYSQL_USER": os.getenv("MYSQL_USER"),
        "MYSQL_PASSWORD": os.getenv("MYSQL_PASSWORD"),
        "MYSQL_HOST": os.getenv("MYSQL_HOST"),
        "MYSQL_PORT": int(os.getenv("MYSQL_PORT", 3306)),
        "MYSQL_DATABASE": os.getenv("MYSQL_DATABASE"),
        # Redshift
        "REDSHIFT_USER": os.getenv("REDSHIFT_USER"),
        "REDSHIFT_PASSWORD": os.getenv("REDSHIFT_PASSWORD"),
        "REDSHIFT_HOST": os.getenv("REDSHIFT_HOST"),
        "REDSHIFT_PORT": int(os.getenv("REDSHIFT_PORT", 5439)),
        "REDSHIFT_DATABASE": os.getenv("REDSHIFT_DATABASE"),
        "REDSHIFT_SCHEMA": os.getenv("REDSHIFT_SCHEMA", "public"),
        "REDSHIFT_S3_BUCKET": os.getenv("REDSHIFT_S3_BUCKET"),
        "REDSHIFT_AWS_IAM_ROLE": os.getenv("REDSHIFT_AWS_IAM_ROLE"),
        "REDSHIFT_AWS_REGION": os.getenv("REDSHIFT_AWS_REGION", "eu-west-2"),
    }
    return config
