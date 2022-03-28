from os.path import join
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from operators.vivareal_operator import VivarealOperator


ARGS = {
    "owner": "lucaspfigueiredo",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

S3_BUCKET = join(
    "s3a://",
    "YOURBUCKET",
    "{stage}"
    )

dag = DAG(
    dag_id="ELT-Pipeline",
    default_args=ARGS,
    description="",
    schedule_interval="0 0 1 * *",
    tags=["ELT", "s3", "spark"]
)

# Extract data from vivareal API
vivareal_operator = VivarealOperator(
    task_id="extract",
    s3_key="extracted_date={{ ds }}/vivareal_{{ ts_nodash }}.json",
    s3_bucket_name="YOURBUCKET", # only the name of the bucket
    dag=dag
)

# transform apartments data 
transformation = SparkSubmitOperator(
    task_id="transformation",
    application="/opt/airflow/spark/transformation.py",
    name="vivareal_transformation",
    conn_id="spark_default",
    conf={
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.driver.extraClassPath": "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true"
    },
    application_args=[
        join(S3_BUCKET.format(stage="raw"), "extracted_date={{ ds }}/*.json"),
        join(S3_BUCKET.format(stage="processed"), "extracted_date={{ ds }}/"),
    ],
    dag=dag
)

# verify file existence
s3_sensor = S3KeySensor(
    task_id="verify_s3",
    bucket_key="extracted_date={{ ds }}/*.parquet",
    bucket_name="YOURBUCKET", # only the name of the bucket
    aws_conn_id="s3_connection",
    wildcard_match=True,
    poke_interval=15,
    timeout=60,
    dag=dag
)

# add columns to DataFrame and partition by neighborhood
curated = SparkSubmitOperator(
   task_id="curated",
   application="/opt/airflow/spark/curated.py",
   name="vivareal_curated",
   conn_id="spark_default",
   conf={
       "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
       "spark.driver.extraClassPath": "/opt/airflow/spark/jars/hadoop-aws-3.3.1.jar:/opt/airflow/spark/jars/aws-java-sdk-bundle-1.11.901.jar",
       "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
       "spark.hadoop.fs.s3a.path.style.access": "true"
   },
   application_args=[
       join(S3_BUCKET.format(stage="processed"), "extracted_date={{ ds }}/*.parquet"),
       join(S3_BUCKET.format(stage="curated"), "extracted_date={{ ds }}/"),
   ],
   dag=dag
)

vivareal_operator >> transformation >> s3_sensor >> curated