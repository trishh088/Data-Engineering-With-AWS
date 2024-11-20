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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1731804257438 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1731804257438")

# Script generated for node accelerometer_landing
accelerometer_landing_node1731804311748 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1731804311748")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.* from accelerometer_landing a
join customer_landing c on a.user = c.email
-- where a.timestamp >= c.sharewithresearchasofdate
'''
SQLQuery_node1731804349776 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_trusted_node1731804257438, "accelerometer_landing":accelerometer_landing_node1731804311748}, transformation_ctx = "SQLQuery_node1731804349776")

# Script generated for node Amazon S3
AmazonS3_node1731806495092 = glueContext.getSink(path="s3://stedi-datalake/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731806495092")
AmazonS3_node1731806495092.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1731806495092.setFormat("json")
AmazonS3_node1731806495092.writeFrame(SQLQuery_node1731804349776)
job.commit()