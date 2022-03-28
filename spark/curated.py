import sys
from distances import lonbeach, latbeach
from math import radians, cos, sin, asin, sqrt

from pyspark.sql import functions as f
from pyspark.sql.types import *
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


if __name__ == "__main__":

    # init spark session
    spark = SparkSession \
        .builder \
        .appName("vivareal_curated") \
        .getOrCreate()
    
    # read DataFrame
    df = spark.read.parquet(s3_src)

    # create column if apartment contains elevator
    df = df.withColumn(
        "elevator", f.when(f.array_contains(df.amenities, "ELEVATOR"), 1).otherwise(0)
        )
        
    # create column if apartment have view to the ocean
    df = df.withColumn(
        "oceanView", f.when(f.col("description").rlike("frente para o mar|frente pro mar|vista para o mar|vista pro mar|vista ao mar| mar "), 1).otherwise(0)
        )

    # calculation of the shortest distance from beach
    def calculate_dist_beach(lon1, lat1): 

        if (lon1 != lon1) | (lon1 is None) | (lat1 != lat1) | (lat1 is None):        
            return 0.0

        else:
            min_dist = []
            lon2 = lonbeach
            lat2 = latbeach

            for lon2, lat2 in zip(lon2, lat2):
                lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
                dlon = lon2 - lon1 
                dlat = lat2 - lat1 
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * asin(sqrt(a)) 
                km = round((6367 * c) * 1000, 2)
                min_dist.append(float(km))
        
        return min(min_dist)

    # create UDF
    dist_beach_udf = f.udf(calculate_dist_beach,FloatType())

    # create column distance from beach
    df = df.withColumn("dist_from_beach", dist_beach_udf(df.lon, df.lat))
    
    # write DataFrame to s3
    df.write.mode("overwrite").partitionBy("neighborhood").parquet(s3_dest)

    # end spark session
    spark.stop()