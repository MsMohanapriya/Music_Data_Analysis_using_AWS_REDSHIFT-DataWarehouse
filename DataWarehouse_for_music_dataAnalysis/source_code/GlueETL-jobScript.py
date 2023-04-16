import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://my-music-data-bucket/musicdata_source/music_data.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("artist_familiarity", "string", "artist_familiarity", "long"),
        ("artist_hotttnesss", "string", "artist_hotttnesss", "string"),
        ("artist_id", "string", "artist_id", "varchar"),
        ("artist_latitude", "string", "artist_latitude", "string"),
        ("artist_longitude", "string", "artist_longitude", "string"),
        ("artist_name", "string", "artist_name", "string"),
        ("artist_terms", "string", "artist_terms", "string"),
        ("artist_terms_freq", "string", "artist_terms_freq", "string"),
        ("release_id", "string", "release_id", "string"),
        ("song_id", "string", "song_id", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Aggregate
Aggregate_node1681626112622 = sparkAggregate(
    glueContext,
    parentFrame=ApplyMapping_node2,
    groups=["song_id", "artist_id"],
    aggs=[["artist_id", "avg"]],
    transformation_ctx="Aggregate_node1681626112622",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="default",
    table_name="music_dbartist_data1_csv",
    redshift_tmp_dir="s3://aws-glue-assets-379882916970-ap-south-1/temporary/",
    additional_options={
        "aws_iam_role": "arn:aws:iam::379882916970:role/service-role/AWSGlueServiceRole-musicRole"
    },
    transformation_ctx="AmazonRedshift_node3",
)

job.commit()
