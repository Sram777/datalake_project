import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1704116710655 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sram-lakehouse/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1704116710655",
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

# Script generated for node curated privacy join
curatedprivacyjoin_node1704116841638 = Join.apply(
    frame1=customers_trusted_node1704116711833,
    frame2=accelerometer_trusted_node1704116710655,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="curatedprivacyjoin_node1704116841638",
)

# Script generated for node Drop Fields
DropFields_node1704175589123 = DropFields.apply(
    frame=curatedprivacyjoin_node1704116841638,
    paths=["z", "y", "x", "user", "timestamp"],
    transformation_ctx="DropFields_node1704175589123",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1704176790093 = DynamicFrame.fromDF(
    DropFields_node1704175589123.toDF().dropDuplicates(["customerName"]),
    glueContext,
    "DropDuplicates_node1704176790093",
)

# Script generated for node customer curated
customercurated_node1704117124390 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1704176790093,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sram-lakehouse/customers/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customercurated_node1704117124390",
)

job.commit()
