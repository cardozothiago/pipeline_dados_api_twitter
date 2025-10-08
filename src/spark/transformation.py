from ast import arg
from pyspark.sql import functions as f
from os.path import join
from pyspark.sql import SparkSession
import argparse

def get_tweets_data(df):
    return df.select(f.explode("data").alias("tweets"))\
                .select("tweets.author_id", "tweets.conversation_id",
                "tweets.created_at", "tweets.id",
                "tweets.public_metrics.*", "tweets.text")

def get_users_data(df):
    return df.select(f.explode("includes.users").alias("users")).select("users.*")

def export_json(df, path):
    # Estou utilizando  coalesce por que o volume de dados é pequeno, mas para grandes volumes de dados é melhor utilizar repartition
    df.coalesce(1).write.mode('overwrite').json(path)

def transform_data(spark, src, dest_path, process_date):
    df = spark.read.json(src)
    tweets_df = get_tweets_data(df)
    users_df = get_users_data(df)
    
    table_dest_path = join(dest_path, "{table_name}", f"process_date={process_date}")
    export_json(tweets_df,table_dest_path.format(table_name="tweets"))
    export_json(users_df,table_dest_path.format(table_name="users"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform API data")
    parser.add_argument("--src", required=True, help="Source path for raw data")
    parser.add_argument("--dest_path", required=True, help="Destination path for transformed data")
    parser.add_argument("--process_date", required=True, help="Process date in YYYY-MM-DD")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("api_transformation").getOrCreate()

    transform_data(spark, args.src, args.dest_path, args.process_date)