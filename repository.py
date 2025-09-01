from jobs.mysql_to_redshift_job import mysql_to_redshift_job
from schedules.mysql_to_redshift_schedule import daily_mysql_to_redshift_schedule
from dagster import Definitions

defs = Definitions(
    jobs=[mysql_to_redshift_job],
    schedules=[daily_mysql_to_redshift_schedule],
)
