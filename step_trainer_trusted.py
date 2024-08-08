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

# Script generated for node step trainer landing
steptrainerlanding_node1704119751471 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1704119751471",
)

# Script generated for node customer curated
customercurated_node1704119842171 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1704119842171",
)

# Script generated for node SQL Query
SqlQuery2739 = """
select * from myDataSource
join myDataSource2
on myDataSource.serialNumber=myDataSource2.serialnumber
"""
SQLQuery_node1704179107210 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2739,
    mapping={
        "myDataSource": steptrainerlanding_node1704119751471,
        "myDataSource2": customercurated_node1704119842171,
    },
    transformation_ctx="SQLQuery_node1704179107210",
)

# Script generated for node Drop Fields
DropFields_node1704180082409 = DropFields.apply(
    frame=SQLQuery_node1704179107210,
    paths=[
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1704180082409",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1704120150287 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704180082409,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sram-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1704120150287",
)

job.commit()
