from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import json



def task_1(data_csv):
    data_dict = {"videos": []}
    trending_videos = data_csv.groupBy("video_id").count().orderBy("count", ascending=False).head(10)
    trending_id_list = [i.video_id for i in trending_videos]
    # print(trending_id_list)
    for i in trending_id_list:
        trending_days = []
        filtered_video = data_csv.filter(data_csv.video_id == i)
        video_id_val = filtered_video.groupby("video_id").agg(F.collect_list("trending_date"), F.collect_list("views"), F.collect_list("likes"), F.collect_list("dislikes"))
        resutled = video_id_val.withColumnRenamed("collect_list(trending_date)", "trending_date").withColumnRenamed("collect_list(views)", "views").withColumnRenamed("collect_list(likes)", "likes").withColumnRenamed("collect_list(dislikes)", "dislikes").head()
        for j in range(len(resutled.trending_date)):
            trending_days.append({"date": resutled.trending_date[j], "views":resutled.views[j], "likes":resutled.likes[j], "dislikes":resutled.dislikes[j]})
        data_video={"id" : resutled.video_id, "title" : filtered_video.head().title, "description" : filtered_video.head().description, "trending_days" : trending_days, "latest_views":trending_days[-1]["views"], "latest_likes":trending_days[-1]["likes"], "latest_dislikes":trending_days[-1]["dislikes"]}
        data_dict["videos"].append(data_video)
    with open("results1.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(data_dict, ensure_ascii=False, sort_keys=True, indent=4))




