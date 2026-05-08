# Pyspark Code for AWS Glue Job to read csv data from S3, apply transformations and write back to S3 in parquet format with data quality evaluation



import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

# Added dynamicframe
from awsglue.dynamicframe import DynamicFrame


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

# Predicate pushdown to filter data at source level - to avoid conflict with non-UTF characters in the data
# Script generated for node Source csv files - READ FROM CATALOG instead of S3 options to get the partition column
predicate_pushdown = "region in ('ca','gb','us')"

Sourcecsvfiles_node1778242479619 = glueContext.create_dynamic_frame.from_catalog(
    database="yt-de-project-raw-db", 
    table_name="raw_statistics", 
    transformation_ctx="Sourcecsvfiles_node1778242479619",
    push_down_predicate = predicate_pushdown
)

# Script generated for node Change Schema - ADD REGION TO MAPPING so it carries through to the target
ChangeSchema_node1778242696204 = ApplyMapping.apply(frame=Sourcecsvfiles_node1778242479619, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "bigint", "category_id", "bigint"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "bigint", "views", "bigint"), ("likes", "bigint", "likes", "bigint"), ("dislikes", "bigint", "dislikes", "bigint"), ("comment_count", "bigint", "comment_count", "bigint"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "boolean"), ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"),
    ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1778242696204")

# changes- conversion to Dataframe
datasink1 = ChangeSchema_node1778242696204.toDF().coalesce(1)
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")


# Script generated for node Target parquet files
EvaluateDataQuality().process_rows(frame=df_final_output, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1778242271782", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Targetparquetfiles_node1778242539405 = glueContext.write_dynamic_frame.from_options(frame=df_final_output, connection_type="s3", format="glueparquet", connection_options={"path": "s3://yt-de-project-cleansed-dev/youtube/raw_statistics/", "partitionKeys": ["region"]}, format_options={"compression": "snappy"}, transformation_ctx="Targetparquetfiles_node1778242539405")

job.commit()