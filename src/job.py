import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Custom Transform
def IoTFlattenTransform(glueContext, dfc) -> DynamicFrameCollection:
    dynamic_frames = {}

    def process_row(record):
        blocks = record["HISTORY"]["BLOCKS"]
        results = []  # Initialize an empty list to collect results

        for b in blocks:
            for tag in b["TAGS"]:
                for data_entry in tag["DATA"]:
                    result = {
                        "start": record["HISTORY"]["ENV"]["TIME"]["START"]["POSIX"],
                        "end": record["HISTORY"]["ENV"]["TIME"]["END"]["POSIX"],
                        "id": tag["DEFINITION"]["ID"],
                        "source_object": tag["DEFINITION"]["SOURCEOBJECT"],
                        "t": data_entry["T"],
                        "q": data_entry["Q"],
                        "v": data_entry["V"],
                    }
                    results.append(result)

        return results  # Return the list of results

    for key in dfc.keys():
        df = dfc[key]
        rdd = df.toDF().rdd.flatMap(process_row)
        new_df = rdd.toDF()
        new_dynamic_frame = DynamicFrame.fromDF(
            new_df, glueContext, "new_dynamic_frame"
        )
        dynamic_frames[key] = new_dynamic_frame

    return DynamicFrameCollection(dynamic_frames, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1700222341275 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:eu-west-1:018744099911:stream/lundbeck-Kinesis-Stream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true",
    },
    transformation_ctx="dataframe_AmazonKinesis_node1700222341275",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        AmazonKinesis_node1700222341275 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        # Script generated for node Custom Transform
        CustomTransform_node1701074388122 = IoTFlattenTransform(
            glueContext,
            DynamicFrameCollection(
                {"AmazonKinesis_node1700222341275": AmazonKinesis_node1700222341275},
                glueContext,
            ),
        )

        # Script generated for node Select From Collection
        SelectFromCollection_node1701074537972 = SelectFromCollection.apply(
            dfc=CustomTransform_node1701074388122,
            key=list(CustomTransform_node1701074388122.keys())[0],
            transformation_ctx="SelectFromCollection_node1701074537972",
        )

        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1701070996315_path = (
            "s3://lundbeck-018744099911-eu-west-1-raw/devices_raw_stream"
            + "/ingest_year="
            + "{:0>4}".format(str(year))
            + "/ingest_month="
            + "{:0>2}".format(str(month))
            + "/ingest_day="
            + "{:0>2}".format(str(day))
            + "/ingest_hour="
            + "{:0>2}".format(str(hour))
            + "/"
        )
        AmazonS3_node1701070996315 = glueContext.write_dynamic_frame.from_options(
            frame=SelectFromCollection_node1701074537972,
            connection_type="s3",
            format="glueparquet",
            connection_options={
                "path": AmazonS3_node1701070996315_path,
                "partitionKeys": [],
            },
            format_options={"compression": "snappy"},
            transformation_ctx="AmazonS3_node1701070996315",
        )


glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1700222341275,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
