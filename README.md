# ELT Pipeline Project

## About

Personal project using data engineering concepts, to get apartments data for further analysis.

The project is an ELT (Extract, Load, Transform) data pipeline, orchestrated with Apache Airflow through Docker Containers.

An AWS S3 bucket is used as a Data Lake in which the files are stored through the layers of the Data Lake. The data is extracted from [vivareal](https://www.vivareal.com.br/) API and loaded in json to the first layer. It is then processed with Spark and loaded in parquet to the second layer. And finally, transformed with additional variables and partitioned by neighborhood.

## Architecture 

![alt text](/images/diagram.png)

## Scenario

Buying an apartment is a big deal, specially because of the price and all the variables that makes an apartment. It is important for the buyer and big companies like banks, that finance the property to know if the price is worth it. This analysis compare similar apartments statistically to each other to have a better understanding of all the features that makes the price. And for a better valuation, it needs data.:grinning:

## Prerequisites

- [AWS S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Spark](https://spark.apache.org/docs/latest/)

## Setup

Clone the project to your desired location:

    $ git clone https://github.com/lucaspfigueiredo/elt-pipeline

Execute the following command that will create the .env file containig the Airflow UID needed by docker-compose:

    $ echo -e "AIRFLOW_UID=$(id -u)" > .env

In the dags/elt_dag.py file, put your s3 bucket url:

    S3_BUCKET= join("https://s3a:", "YOUR BUCKET")

Build Docker:

    $ docker-compose build 

Initialize Airflow database:

    $ docker-compose up airflow-init

Start Containers:

    $ docker-compose up -d

![alt text](/images/docker.gif)

When everything is done, you can check all the containers running:

    $ docker ps

## Airflow Interface

Now you can access Airflow web interface by going to http://localhost:8080 with the default user which is in the docker-compose.yml. **Username/Password: airflow**

With your AWS S3 user and bucket created, you can store your credentials in the **connections** in Airflow. And we can store which port Spark is exposed when we submit our jobs:

![alt text](/images/s3-connection.png)
![alt text](/images/spark-connection.png)

Now, we can trigger our DAG and see all the tasks running.

![alt text](/images/airflow.gif)

And finally, check the S3 bucket if our partitioned data is in the right place.

![alt text](/images/s3-bucket.png)

## Shut down or restart Airflow

If you need to make changes or shut down:

    $ docker-compose down

## References 

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)
- [Spark by examples](https://sparkbyexamples.com/pyspark-tutorial/)
- [Spark s3 integration](https://spark.apache.org/docs/latest/cloud-integration.html)
- [Airflow and Spark with Docker](https://medium.com/data-arena/building-a-spark-and-airflow-development-environment-with-docker-f0b9b625edd8)

## License

You can check out the full license [here](https://github.com/lucaspfigueiredo/elt-pipeline/blob/main/LICENSE)

This project is licensed under the terms of the **MIT** license.