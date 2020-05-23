import time
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import task_1, task_6, task_4, task_5, task_3, task_2


def connect_to_spark():
    return SparkSession.builder.master("local[*]").appName("spark").getOrCreate()


def date_formatting(date):
    date = datetime.datetime.strptime(date, '%y.%d.%m').strftime('%y.%m.%d')
    return date


def main(csv_file_path, json_file_path):
    connection = connect_to_spark()
    df = connection.read.json(json_file_path, multiLine=True)
    data = connection.read.csv(csv_file_path, inferSchema=True, header=True, multiLine=True)
    f = udf(date_formatting)
    data = data.withColumnRenamed("description\r", "description")
    data = data.withColumn("trending_date", f(data.trending_date))
    # task_1.task_1(data)
    # task_4.task_4(data)
    # task_5.task_5(data)
    # task_3.task_3(data)
    task_2.task_2(data, json_file_path, connection)
    # task_6.task_6(data, json_file_path)


if __name__ == "__main__":
    csv_file_path = sys.argv[1]
    json_file_path = sys.argv[2]
    main(csv_file_path, json_file_path)
