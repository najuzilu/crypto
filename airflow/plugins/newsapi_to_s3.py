# python3
from utils import MyConfigParser
from typing import List
from io import StringIO
from utils import (
    get_json_objects,
    flatten_json,
    process_ds,
)
import pandas as pd
import boto3


def dump_to_s3(s3, bucket: str, category: str, language: str) -> None:
    """
    Extract data through REST API and dump to S3 bucket
    :param s3:          AWS S3 resource
    :param bucket:      AWS S3 bucket name
    :param category:    Filter news by category (i.e., "bitcoin")
    :param language:    Search news with language (i.e, "en")
    """
    # set page size to 100 which is max
    page_size = 100
    # extract 100 most popular articles for category
    newsapi = "https://newsapi.org/v2/everything?"
    newsapi += f"qInTitle={category}&from={after}&to={before}&"
    newsapi += f"language={language}&pageSize={page_size}&"
    newsapi += f"sortBy=popularity&apiKey={NEWS_SECRET_ACCESS_KEY}"

    monthly_obs = get_json_objects(newsapi)

    if len(monthly_obs["articles"]) == 0:
        print(
            f"""
            ERROR: NewsAPI `{category}` has no observations for
            specified time period."""
        )
        return

    data = []

    for article in monthly_obs["articles"]:
        flat_article = flatten_json(article)
        data.append(flat_article)

    df = pd.DataFrame(data)
    df["source_id"].fillna(
        df.source_name.str.replace(" ", "_").str.lower(),
        inplace=True,
    )

    # set index to allow easy groupby
    df.index = pd.to_datetime(df["publishedAt"])
    # iterate over group by
    for (source_id, year, month), group in df.groupby(
        [df.source_id, df.index.year, df.index.month]
    ):
        csv_buffer = StringIO()
        group.to_csv(csv_buffer, index=False)

        try:
            s3.Object(
                bucket, f"news-articles/{year}/{month}/{category}_{source_id}.csv"
            ).put(Body=csv_buffer.getvalue())
        except Exception as e:
            msg = f"""ERROR: Could not dump {category}/
                {source_id}/{year}/{month} in S3."""
            print(msg, e)
            continue


def NewsApiToS3(ds: str, categories: List[str], language: str, **kwargs) -> None:
    """
    Extract data using NewsAPI REST API and dump to AWS S3
    :param ds:              Airflow macro reference `ds`
    :param categories:      List of categories to filter news
    :param language:    Search news with language (i.e, "en")
    """
    my_config = MyConfigParser()

    global NEWS_SECRET_ACCESS_KEY
    NEWS_SECRET_ACCESS_KEY = my_config.news_secret_access_key()
    AWS_SECRET_ACCESS_KEY = my_config.aws_secret_access_key()
    AWS_ACCESS_KEY_ID = my_config.aws_access_key_id()
    AWS_REGION = my_config.aws_region()
    S3_BUCKET = my_config.s3_bucket()

    global after, before
    after, before = process_ds(ds)

    s3 = boto3.resource(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    for category in categories:
        dump_to_s3(s3, S3_BUCKET, category, language)
