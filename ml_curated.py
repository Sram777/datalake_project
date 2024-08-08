import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1704180813549 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1704180813549",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1704180814847 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1704180814847",
)

# Script generated for node SQL Query
SqlQuery2552 = """
select * from myDataSource
join myDataSource2
on myDataSource.timeStamp=myDataSource2.sensorReadingTime
"""
SQLQuery_node1704180920285 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2552,
    mapping={
        "myDataSource": accelerometer_trusted_node1704180814847,
        "myDataSource2": step_trainer_trusted_node1704180813549,
    },
    transformation_ctx="SQLQuery_node1704180920285",
)

# Script generated for node Amazon S3
AmazonS3_node1704181172899 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1704180920285,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://sram-lakehouse/ml_curated/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1704181172899",
)

job.commit()
