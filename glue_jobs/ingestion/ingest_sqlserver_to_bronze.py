import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import current_timestamp, lit


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "CONNECTION_NAME",
        "BRONZE_BUCKET",
        "BRONZE_PREFIX",
        "SQL_DATABASE",
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

connection_name = args["CONNECTION_NAME"]
bronze_bucket = args["BRONZE_BUCKET"]
bronze_prefix = args["BRONZE_PREFIX"].rstrip("/")
sql_database = args["SQL_DATABASE"]

batch_id = datetime.now(timezone.utc).strftime("batch_%Y%m%dT%H%M%SZ")

tables = [
    "order_items",
    "order_item_options",
    "date_dim",
]


def read_sqlserver_table(table_name: str):
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="sqlserver",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": connection_name,
            "dbtable": f"dbo.{table_name}",
        },
        transformation_ctx=f"read_{table_name}",
    )

    return dynamic_frame.toDF()


def write_bronze(df, table_name: str):
    output_path = (
        f"s3://{bronze_bucket}/{bronze_prefix}/bronze/{table_name}/"
        f"batch_id={batch_id}/"
    )

    (
        df.withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("source_system", lit("sqlserver"))
          .withColumn("batch_id", lit(batch_id))
          .write
          .mode("overwrite")
          .parquet(output_path)
    )

    print(f"Wrote {table_name} to {output_path}")


for table in tables:
    df = read_sqlserver_table(table)
    row_count = df.count()

    if row_count == 0:
        raise ValueError(f"Source table {table} returned 0 rows.")

    write_bronze(df, table)

job.commit()