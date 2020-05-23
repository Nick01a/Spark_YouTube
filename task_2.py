from pyspark.sql.functions import udf, explode
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_date, date_format
import json
from datetime import timedelta, datetime


def task_2(data_csv, data_json, session):
    with open(data_json, 'r') as f:
        categories_json = json.load(f)

    weeks_dict = {"weeks": []}
    schema = StructType([
        # StructField('kind', StringType()),
        # StructField('etag', StringType()),
        StructField('id', StringType()),
        StructField('snippet', StructType([
                                            #StructField('channelId', StringType()),
                                            StructField('title', StringType()),
                                            # StructField('assignable', BooleanType())
                                             ]))
    ])
    rdd = session.sparkContext.parallelize(categories_json["items"])
    df = session.createDataFrame(rdd, schema)

    get_all_videos = data_csv.join(data_csv.groupby("video_id").count(), on="video_id").filter(F.col("count")>=2).select("video_id", "views", "count", "trending_date", "category_id").orderBy("count", ascending = False)
    get_all_videos = get_all_videos.join(df, get_all_videos.category_id == df.id)
    starting_date = data_csv.select("trending_date").agg(F.min("trending_date")).withColumnRenamed("min(trending_date)", "trending_date").collect()[0].trending_date
    ending_date = data_csv.select("trending_date").agg(F.max("trending_date")).withColumnRenamed("max(trending_date)", "trending_date").collect()[0].trending_date
    starting_date = datetime.strptime(starting_date, "%y.%m.%d")
    ending_date = datetime.strptime(ending_date, "%y.%m.%d")
    week_counter = 0
    while(starting_date<=ending_date):
        filtered_data = get_all_videos.filter(get_all_videos.trending_date >= starting_date.strftime("%y.%m.%d")).filter(get_all_videos.trending_date <= (starting_date + timedelta(days=7)).strftime("%y.%m.%d"))
        minimax = filtered_data.join(filtered_data.groupby("video_id").agg(F.max("views").alias("max")), on="video_id").join(filtered_data.groupby("video_id").agg(F.min("views").alias("min")), on="video_id")
        dropouts = minimax.withColumn("average", (minimax["max"]-minimax["min"])).dropDuplicates(["video_id"])
        dropouts = dropouts.join(dropouts.groupby("category_id").count().withColumnRenamed("count", "total_videos"), on="category_id")
        dropouts = dropouts.join(dropouts.groupBy('category_id').agg(F.collect_list('video_id')).withColumnRenamed("collect_list(video_id)", "video_ids"), on="category_id")
        dropouts = dropouts.join(dropouts.groupby("category_id").agg(F.sum("views")).withColumnRenamed("sum(views)", "total_views"), on="category_id").dropDuplicates(["category_id"]).collect()
        starting_date += timedelta(days=7)
        week_counter+=1
        for i in dropouts:

            week = {"start_date": datetime.strftime(starting_date, "%y.%m.%d"), "end_date": datetime.strftime(starting_date + timedelta(days=7), "%y.%m.%d"), "category_id": i.category_id, "category_name": i.snippet, "number_of_videos": i.total_videos, "total_views": i.views, "video_ids": i.video_ids}
        weeks_dict["weeks"].append(week)

    with open("results2.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(weeks_dict, ensure_ascii=False, sort_keys=True, indent=4))



