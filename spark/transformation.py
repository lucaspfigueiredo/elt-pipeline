import sys

from pyspark.sql import functions as f
from pyspark.sql.session import SparkSession
from pyspark import SparkConf, SparkContext

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# get s3 credentials (from Airflow UI)
s3_conn = S3Hook.get_connection("s3_connection")

# set Login and Password
awsAccessKey = s3_conn.login
awsSecretKey = s3_conn.password

# Args from SparkSubmitOperator
s3_src = sys.argv[1]
s3_dest = sys.argv[2]

# set config
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") 
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") 
    .set("spark.hadoop.fs.s3a.access.key", awsAccessKey)
    .set("spark.hadoop.fs.s3a.secret.key", awsSecretKey) 
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.connection.maximum", 100)
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()

# select useful variables
def get_apartments_data(df):
    return df \
        .select(
        "link.href",
        "listing.title",
        "listing.description",
        "listing.address.street",
        "listing.address.streetNumber",
        "listing.address.neighborhood",
        "listing.address.city",
        "listing.address.state",
        "listing.address.zipcode",
        "listing.address.point.lat",
        "listing.address.point.lon",
        f.element_at("listing.pricingInfos.price", 1).alias("price"),
        f.element_at("listing.pricingInfos.monthlyCondoFee", 1).alias("monthlyCondoFee"),
        f.element_at("listing.pricingInfos.yearlyIptu", 1).alias("yearlyIptu"),
        f.explode("listing.usableAreas").alias("area"),
        f.element_at("listing.bedrooms", 1).alias("bedrooms"),
        f.element_at("listing.bathrooms", 1).alias("bathrooms"),
        f.element_at("listing.suites", 1).alias("suites"),
        f.element_at("listing.parkingSpaces", 1).alias("parkingSpace"),
        "listing.amenities",
        )

# spark write in .parquet to s3 bucket
def export_data(df, dest):
    df.write.mode("overwrite").parquet(dest)

# apply transformation
def transformation(spark, src, dest):
    df = spark.read.json(src)

    apartments_df = get_apartments_data(df)

    export_data(apartments_df, dest)


if __name__ == "__main__":
    
    # init spark session
    spark = SparkSession \
        .builder \
        .appName("vivareal_transformation") \
        .getOrCreate()

    # apply transformation
    transformation(spark=spark, src=s3_src, dest=s3_dest)

    # end spark session
    spark.stop()