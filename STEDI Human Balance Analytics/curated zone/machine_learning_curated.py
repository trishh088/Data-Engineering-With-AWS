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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1731858658138 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1731858658138")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1731858680245 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1731858680245")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from accelerometer_trusted a join step_trainer_trusted s 
on a.timestamp = s.sensorreadingtime


'''
SQLQuery_node1731858708579 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_trusted_node1731858680245, "step_trainer_trusted":step_trainer_trusted_node1731858658138}, transformation_ctx = "SQLQuery_node1731858708579")

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(SQLQuery_node1731858708579, ["EMAIL"], 1.0, 0.1, "HIGH")

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

DetectSensitiveData_node1731859506525 = hashDf(SQLQuery_node1731858708579, list(classified_map.keys()))

# Script generated for node Amazon S3
AmazonS3_node1731859592691 = glueContext.getSink(path="s3://stedi-datalake/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731859592691")
AmazonS3_node1731859592691.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1731859592691.setFormat("json")
AmazonS3_node1731859592691.writeFrame(DetectSensitiveData_node1731859506525)
job.commit()