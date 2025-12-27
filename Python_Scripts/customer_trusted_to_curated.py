import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1766847151998 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://abc-s3link-s3/accelerometer/trusted2/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1766847151998")

# Script generated for node customer_trusted
customer_trusted_node1766847151559 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://abc-s3link-s3/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1766847151559")

# Script generated for node joins
SqlQuery1 = '''
select * from a join c on a.user = c.email
'''
joins_node1766847159322 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"a":accelerometer_trusted_node1766847151998, "c":customer_trusted_node1766847151559}, transformation_ctx = "joins_node1766847159322")

# Script generated for node drop_fields
SqlQuery0 = '''
SELECT DISTINCT
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM myDataSource;
'''
drop_fields_node1766847207333 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":joins_node1766847159322}, transformation_ctx = "drop_fields_node1766847207333")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=drop_fields_node1766847207333, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766847065176", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1766847164359 = glueContext.getSink(path="s3://abc-s3link-s3/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1766847164359")
customer_curated_node1766847164359.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
customer_curated_node1766847164359.setFormat("json")
customer_curated_node1766847164359.writeFrame(drop_fields_node1766847207333)
job.commit()
