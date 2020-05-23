from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import json

def task_4(data_csv):
    results = {"channels": []}
    channel_title_ordered = data_csv.orderBy("trending_date", ascending=False).dropDuplicates(["video_id"]).groupby('channel_title').sum("views").orderBy("sum(views)", ascending=False).head(20)
    for i in channel_title_ordered:
        video_view = []
        publish_time_ordered = data_csv.filter(data_csv.channel_title == i.channel_title).dropDuplicates(["video_id"]).orderBy("publish_time").collect()
        aggr_v = data_csv.groupby("video_id").agg(F.max("views")).withColumnRenamed("max(views)", "views")
        total_count = 0
        for j in publish_time_ordered:
            video_view.append({"video_id":j.video_id, "views":aggr_v.filter(aggr_v.video_id==j.video_id).head().views})
            total_count += aggr_v.filter(aggr_v.video_id==j.video_id).head().views
        results["channels"].append({"channel_name":i.channel_title, "start_date":publish_time_ordered[0].publish_time.strftime('%y.%m.%d'), "end_date":publish_time_ordered[-1].publish_time.strftime('%y.%m.%d'), "total_views":total_count, "video_views":video_view})

    with open("results4.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(results, ensure_ascii=False, sort_keys=True, indent=4))




