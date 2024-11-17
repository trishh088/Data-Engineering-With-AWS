import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from awsglue import DynamicFrame
import hashlib

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
customer_trusted_node1731811357535 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1731811357535")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1731811414811 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-datalake/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1731811414811")

# Script generated for node SQL Query
SqlQuery0 = '''
select c.* from customer_trusted c
join accelerometer_trusted a on a.user=c.email
'''
SQLQuery_node1731811495750 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_trusted_node1731811414811, "customer_trusted":customer_trusted_node1731811357535}, transformation_ctx = "SQLQuery_node1731811495750")

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(SQLQuery_node1731811495750, ["EMAIL", "PERSON_NAME"], 1.0, 0.1, "HIGH")

def pii_column_hash(original_cell_value):
    return hashlib.sha256(str(original_cell_value).encode()).hexdigest()

pii_column_hash_udf = udf(pii_column_hash, StringType())

def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")

DetectSensitiveData_node1731811780439 = hashDf(SQLQuery_node1731811495750, list(classified_map.keys()))

# Script generated for node SQL Query distinct
SqlQuery1 = '''
select distinct * from myDataSource
'''
SQLQuerydistinct_node1731863088325 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":DetectSensitiveData_node1731811780439}, transformation_ctx = "SQLQuerydistinct_node1731863088325")

# Script generated for node Amazon S3
AmazonS3_node1731811833828 = glueContext.getSink(path="s3://stedi-datalake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731811833828")
AmazonS3_node1731811833828.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1731811833828.setFormat("json")
AmazonS3_node1731811833828.writeFrame(SQLQuerydistinct_node1731863088325)
job.commit()