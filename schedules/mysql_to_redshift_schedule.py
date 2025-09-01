from dagster import schedule
from jobs.mysql_to_redshift_job import mysql_to_redshift_job
from utils.query_loader import load_query


@schedule(
    cron_schedule="0 14 * * *",  # Every day at 2 AM UTC
    job=mysql_to_redshift_job,
    execution_timezone="UTC",
)
def daily_mysql_to_redshift_schedule():
    """
    Schedule to run MySQL → Redshift ETL job daily.
    """
    return {
        "ops": {
            "extract_from_mysql": {
                "config": {"daily_user_query": load_query("daily_users.sql")}
            },
            "load_to_redshift": {"config": {"redshift_table": "users"}},
        }
    }
