import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node cust_trusted
cust_trusted_node1766846373914 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://abc-s3link-s3/customer/trusted/"], "recurse": True}, transformation_ctx="cust_trusted_node1766846373914")

# Script generated for node acc_landing
acc_landing_node1766846375037 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://abc-s3link-s3/accelerometer/landing/"], "recurse": True}, transformation_ctx="acc_landing_node1766846375037")

# Script generated for node Join
Join_node1766846396830 = Join.apply(frame1=acc_landing_node1766846375037, frame2=cust_trusted_node1766846373914, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1766846396830")

# Script generated for node Drop Fields
DropFields_node1766846400739 = DropFields.apply(frame=Join_node1766846396830, paths=["email", "customername", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1766846400739")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1766846400739, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766846314012", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766846409371 = glueContext.getSink(path="s3://abc-s3link-s3/accelerometer/trusted2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766846409371")
AmazonS3_node1766846409371.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted2")
AmazonS3_node1766846409371.setFormat("json")
AmazonS3_node1766846409371.writeFrame(DropFields_node1766846400739)
job.commit()
