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

# Script generated for node customer_curated_datacatalog
customer_curated_datacatalog_node1731856641577 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_datacatalog_node1731856641577")

# Script generated for node step_trainer_landing
step_trainer_landing_node1731808051323 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1731808051323")

# Script generated for node join query
SqlQuery0 = '''
select s.* from step_trainer_landing s
join (select distinct serialnumber from customer_curated) c
on s.serialnumber =c.serialnumber
-- where s.serialnumber in (select serialnumber from customer_curated)
'''
joinquery_node1731808089730 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":step_trainer_landing_node1731808051323, "customer_curated":customer_curated_datacatalog_node1731856641577}, transformation_ctx = "joinquery_node1731808089730")

# Script generated for node Amazon S3
AmazonS3_node1731810246958 = glueContext.getSink(path="s3://stedi-datalake/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731810246958")
AmazonS3_node1731810246958.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1731810246958.setFormat("json")
AmazonS3_node1731810246958.writeFrame(joinquery_node1731808089730)
job.commit()