import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node track
track_node1711303584602 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datawarehouse/staging/spotify_tracks_data_2023.csv"], "recurse": True}, transformation_ctx="track_node1711303584602")

# Script generated for node album
album_node1711303581081 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datawarehouse/staging/spotify-albums_data_2023.csv"], "recurse": True}, transformation_ctx="album_node1711303581081")

# Script generated for node artist
artist_node1711303583341 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datawarehouse/staging/spotify_artist_data_2023.csv"], "recurse": True}, transformation_ctx="artist_node1711303583341")

# Script generated for node Join artist & album
Joinartistalbum_node1711303723735 = Join.apply(frame1=album_node1711303581081, frame2=artist_node1711303583341, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinartistalbum_node1711303723735")

# Script generated for node Join track
Jointrack_node1711304046899 = Join.apply(frame1=Joinartistalbum_node1711303723735, frame2=track_node1711303584602, keys1=["track_id"], keys2=["id"], transformation_ctx="Jointrack_node1711304046899")

# Script generated for node Drop Fields
DropFields_node1711304113753 = DropFields.apply(frame=Jointrack_node1711304046899, paths=["`.id`", "id"], transformation_ctx="DropFields_node1711304113753")

# Script generated for node data warehouse
datawarehouse_node1711304176978 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711304113753, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-datawarehouse/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="datawarehouse_node1711304176978")

job.commit()