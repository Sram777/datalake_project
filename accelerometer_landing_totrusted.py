import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1704116710655 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sram-lakehouse/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1704116710655",
)

# Script generated for node customers _trusted
customers_trusted_node1704116711833 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sram-lakehouse/customers/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customers_trusted_node1704116711833",
)

# Script generated for node accelerometer privacy join
accelerometerprivacyjoin_node1704116841638 = Join.apply(
    frame1=customers_trusted_node1704116711833,
    frame2=accelerometer_landing_node1704116710655,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="accelerometerprivacyjoin_node1704116841638",
)

# Script generated for node Drop Fields
DropFields_node1704117004968 = DropFields.apply(
    frame=accelerometerprivacyjoin_node1704116841638,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1704117004968",
)

# Script generated for node Amazon S3
AmazonS3_node1704117124390 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1704117004968,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sram-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1704117124390",
)

job.commit()
