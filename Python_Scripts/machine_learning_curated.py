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
accelerometer_trusted_node1766850170303 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://abc-s3link-s3/accelerometer/trusted2/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1766850170303")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1766850171435 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://abc-s3link-s3/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1766850171435")

# Script generated for node SQL Query to join
SqlQuery0 = '''
select * 
from  a
join s on a.timestamp = s.sensorreadingtime;
'''
SQLQuerytojoin_node1766850279244 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":step_trainer_trusted_node1766850171435, "a":accelerometer_trusted_node1766850170303}, transformation_ctx = "SQLQuerytojoin_node1766850279244")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuerytojoin_node1766850279244, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1766849552811", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1766850333103 = glueContext.getSink(path="s3://abc-s3link-s3/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1766850333103")
AmazonS3_node1766850333103.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
AmazonS3_node1766850333103.setFormat("json")
AmazonS3_node1766850333103.writeFrame(SQLQuerytojoin_node1766850279244)
job.commit()
