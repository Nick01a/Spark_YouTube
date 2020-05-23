from pyspark.sql.functions import udf, explode
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import to_date, date_format
import json
from datetime import timedelta, datetime
import operator

def all_tags(grouped_tags):
    lst_dict = dict()
    for j in range(len(grouped_tags)):
        day_tags = grouped_tags[j]
        for i in range(len(day_tags)):
            flat_list = str(day_tags[i])
            lst = flat_list.split('|')
            for k in lst:
                if k != '[none]' and k != '':
                    if k in lst_dict:
                        lst_dict[k] += flat_list.count(k)
                    else:
                        lst_dict[k] = flat_list.count(k)
    lst_dict = dict(sorted(lst_dict.items(), key=operator.itemgetter(1), reverse=True)[:10])
    return lst_dict


def task_3(data_csv):
    tags_dict = {"months": []}
    starting_date = data_csv.select("trending_date").agg(F.min("trending_date")).withColumnRenamed("min(trending_date)", "trending_date").collect()[0].trending_date
    ending_date = data_csv.select("trending_date").agg(F.max("trending_date")).withColumnRenamed("max(trending_date)","trending_date").collect()[0].trending_date
    starting_date = datetime.strptime(starting_date, "%y.%m.%d")
    ending_date = datetime.strptime(ending_date, "%y.%m.%d")
    all_trending_tags = data_csv.select("video_id", "trending_date", "tags")
    while (starting_date <= ending_date):
        tags = []
        filtered_data = all_trending_tags.filter(
                    all_trending_tags.trending_date >= starting_date.strftime("%y.%m.%d")).filter(
                    all_trending_tags.trending_date <= (starting_date + timedelta(days=30)).strftime("%y.%m.%d")).dropDuplicates(["video_id"])
        grouped_tags = filtered_data.groupby("trending_date").agg(F.collect_list("tags")).withColumnRenamed("collect_list(tags)", "tags").select("tags").collect()
        all_month_tags = all_tags(grouped_tags)
        for key, value in all_month_tags.items():
            tag= {"tag":key, "number_of_videos": value}
            tags.append(tag)
        month = {"start_date": datetime.strftime(starting_date, "%y.%m.%d"), "end_date": datetime.strftime(starting_date + timedelta(days=30), "%y.%m.%d"), "tags":tags}
        starting_date += timedelta(days=30)
        tags_dict["months"].append(month)
    with open("results3.json", 'w', encoding='utf-8') as f:
        f.write(json.dumps(tags_dict, ensure_ascii=False, sort_keys=True, indent=4))
