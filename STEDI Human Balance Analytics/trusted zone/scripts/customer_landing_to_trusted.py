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

# Script generated for node Amazon S3
AmazonS3_node1731796780406 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1731796780406")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where 
sharewithresearchasofdate > 0;

'''
SQLQuery_node1731796913636 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1731796780406}, transformation_ctx = "SQLQuery_node1731796913636")

# Script generated for node Amazon S3
AmazonS3_node1731796991438 = glueContext.getSink(path="s3://stedi-datalake/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731796991438")
AmazonS3_node1731796991438.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1731796991438.setFormat("json")
AmazonS3_node1731796991438.writeFrame(SQLQuery_node1731796913636)
job.commit()