from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
import json

def task_6(data_csv, data_category):
    categories_title = []
    data_dict = {"categories": []}
    with open(data_category, 'r') as f:
        categories_json = json.load(f)
    for i in categories_json["items"]:
        if i["snippet"]["assignable"]:
            categories_title.append({"id":i["id"], "category":i["snippet"]["title"]})
    video_to_rate = data_csv.filter(data_csv.views >= 100000).withColumn("rate", F.col("likes") / F.col("dislikes"))
    for i in categories_title:
        by_category = video_to_rate.filter(video_to_rate.category_id==i["id"]).groupby("video_id", "title").agg(F.max('rate')).orderBy("max(rate)", ascending=False).withColumnRenamed("max(rate)", "rate").head(10)
        views_to_rate = video_to_rate.select("views", "rate")
        video_dict = []
        for j in by_category:
            video_dict.append({"video_id":j.video_id, "video_title":j.title, "ratio_likes_dislikes":j.rate, "Views":views_to_rate.filter(video_to_rate.rate == j.rate).head().views})
        data_dict["categories"].append({"category_id":i["id"], "category_name":i["category"], "videos":video_dict})
    with open("results6.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(data_dict, ensure_ascii=False, sort_keys=True, indent=4))

