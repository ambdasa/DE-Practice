import sys
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

# Set up GlueContext
sc = SparkContext()
sqlContext = SQLContext(sc)
glueContext = GlueContext(sc)

# Load data from S3
s3_path = 's3://my-etl-bucket/customers.csv'
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# Apply Transformation - Capitalizing City Names
df = dynamic_frame.toDF()
df = df.withColumn("city", df.city.title())  
transformed_dynamic_frame = glueContext.create_dynamic_frame.fromDF(df, glueContext, "transformed")

# Load Transformed Data into Redshift
redshift_jdbc_url = "jdbc:redshift://<redshift_endpoint>:<port>/<database>"
redshift_temp_dir = "s3://my-etl-bucket/temp/"

glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="redshift",
    connection_options={
        "url": redshift_jdbc_url,
        "dbtable": "customers",
        "user": "<your_redshift_username>",
        "password": "<your_redshift_password>",
        "redshiftTmpDir": redshift_temp_dir
    }
)

print("ETL Job Completed Successfully!") 
