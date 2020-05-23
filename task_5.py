from pyspark.sql.functions import udf, explode
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_date, date_format
import json
from datetime import timedelta, datetime


def task_5(data_csv):
    channels = {"channels": []}
    videos_to_trending = data_csv\
        .select("video_id", "channel_title", "title", "trending_date")\
        .join(data_csv.groupby("video_id").count().withColumnRenamed("count", "video_days_trending"), on="video_id")\
        .dropDuplicates(["video_id"])
    channel_trending = videos_to_trending.join(videos_to_trending.groupby("channel_title").agg(F.sum("video_days_trending")).withColumnRenamed("sum(video_days_trending)", "channel_popular"), on="channel_title")
    channel_trending = channel_trending.join(channel_trending.groupBy("channel_title")\
        .agg(F.collect_list(F.struct("video_id","title", "video_days_trending")).alias("video")), on="channel_title")\
        .dropDuplicates(["channel_title"]).orderBy("channel_popular", ascending=False)
    result = channel_trending.select("channel_title", "channel_popular", "video").collect()
    for i in result:
        videos = {"videos_days": []}
        for j in i.video:
            videos["videos_days"].append({"video_id": j.video_id, "video_title": j.title, "trending_days": j.video_days_trending})
            # print(videos)
        channels["channels"].append({"channel_name": i.channel_title, "total_trending_days": i.channel_popular, "video_days": videos})
        # print(channels)
    with open("results5.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(channels, ensure_ascii=False, sort_keys=True, indent=4))
