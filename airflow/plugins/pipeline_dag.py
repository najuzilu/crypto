# python3
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from pipeline_subdag import load_dimension_tables_dag
from pipeline_subdag import get_s3_to_redshift_dag
from airflow.operators.udacity_plugin import (
    LoadDimensionOperator,
    DataQualityOperator,
    LoadFactOperator,
    S3BucketOperator,
)
from cryptowatch_to_s3 import CryptowatchToS3
from sentiment import DetectNewsSentiment
from newsapi_to_s3 import NewsApiToS3
from airflow.utils import timezone
from utils import MyConfigParser
from helpers import SqlQueries
from airflow import DAG

my_config = MyConfigParser()
AWS_REGION = my_config.aws_region()
S3_BUCKET = my_config.s3_bucket()

# set arguments
start_date = timezone.utcnow()
default_args = {
    "owner": "udacity",
    "start_date": start_date,
    "retries": 3,
    "catchup": False,
    "email_on_retry": False,
}

# set schedule interval to the first of
# every month at 7am
dag_name = "etl_pipeline"
dag = DAG(
    dag_name,
    default_args=default_args,
    description="ETL Pipeline for Automating Monthly Cryptoasset Reports.",
    schedule_interval="0 7 1 * *",
)

# Setup Operators
start_operator = DummyOperator(
    task_id="Begin_execution",
    dag=dag,
)

crypto_task = PythonOperator(
    task_id="Get_crypto_to_s3",
    dag=dag,
    python_callable=CryptowatchToS3,
    provide_context=True,
    op_kwargs={
        "pair_base": ["btc", "eth", "ada", "doge", "dot", "uni"],
        "pair_curr": ["usd", "gbp", "eur"],
    },
)

newsapi_task = PythonOperator(
    task_id="Get_newsapi_to_s3",
    dag=dag,
    python_callable=NewsApiToS3,
    provide_context=True,
    op_kwargs={
        "categories": [
            "bitcoin",
            "ethereum",
            "cardano",
            "dogecoin",
            "polkadot",
            "uniswap",
        ],
        "language": "en",
        "sentiment_column": "title",
    },
)

crypto_bucket_task = S3BucketOperator(
    task_id="Check_candlestick_bucket",
    dag=dag,
    aws_credentials_id="aws-credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="ohlc-candlestick",
)

news_bucket_task = S3BucketOperator(
    task_id="Check_newsapi_bucket",
    dag=dag,
    aws_credentials_id="aws-credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="news-articles",
)


sentiment_task = PythonOperator(
    task_id="Detect_news_sentiment",
    dag=dag,
    python_callable=DetectNewsSentiment,
    op_kwargs={"column_name": "title"},
)

sent_bucket_task = S3BucketOperator(
    task_id="Check_sentiment_bucket",
    dag=dag,
    aws_credentials_id="aws-credentials",
    region=AWS_REGION,
    s3_bucket=S3_BUCKET,
    s3_prefix="news-articles-sent",
)

staging_crypto_id = "Stage_crypto"
staging_crypto_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        dag_name,
        staging_crypto_id,
        "redshift",
        "aws-credentials",
        AWS_REGION,
        "staging_crypto",
        S3_BUCKET,
        "ohlc-candlestick",
        start_date=start_date,
    ),
    task_id=staging_crypto_id,
    dag=dag,
)


staging_news_id = "Stage_news"
staging_news_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        dag_name,
        staging_news_id,
        "redshift",
        "aws-credentials",
        AWS_REGION,
        "staging_news",
        S3_BUCKET,
        "news-articles-sent",
        start_date=start_date,
    ),
    task_id=staging_news_id,
    dag=dag,
)


load_articles_task_id = "Load_articles_table"
load_articles_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag=dag_name,
        task_id=load_articles_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws-credentials",
        table="articles",
        query=SqlQueries.articles_table_insert,
        start_date=start_date,
    ),
    task_id=load_articles_task_id,
    dag=dag,
)

load_time_task_id = "Load_time_table"
load_time_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag=dag_name,
        task_id=load_time_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws-credentials",
        table="time",
        query=SqlQueries.time_table_insert,
        start_date=start_date,
    ),
    task_id=load_time_task_id,
    dag=dag,
)

load_sources_task_id = "Load_sources_table"
load_sources_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag=dag_name,
        task_id=load_sources_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws-credentials",
        table="sources",
        query=SqlQueries.sources_table_insert,
        start_date=start_date,
    ),
    task_id=load_sources_task_id,
    dag=dag,
)

load_asset_base_task_id = "Load_asset_base_table"
load_asset_base_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag=dag_name,
        task_id=load_asset_base_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws-credentials",
        table="asset_base",
        query=SqlQueries.asset_base_table_insert,
        start_date=start_date,
    ),
    task_id=load_asset_base_task_id,
    dag=dag,
)

load_asset_quote_task_id = "Load_asset_quote_table"
load_asset_quote_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag=dag_name,
        task_id=load_asset_quote_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws-credentials",
        table="asset_quote",
        query=SqlQueries.asset_quote_table_insert,
        start_date=start_date,
    ),
    task_id=load_asset_quote_task_id,
    dag=dag,
)

load_asset_markets_task_id = "Load_asset_markets_table"
load_asset_markets_table = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag=dag_name,
        task_id=load_asset_markets_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws-credentials",
        table="asset_markets",
        query=SqlQueries.asset_markets_table_insert,
        start_date=start_date,
    ),
    task_id=load_asset_markets_task_id,
    dag=dag,
)

load_candlestick_task_id = "Load_candlestick_table"
load_candlestick_table = LoadFactOperator(
    task_id=load_candlestick_task_id,
    dag=dag,
    redshift_conn_id="redshift",
    query=SqlQueries.candlestick_table_insert,
)


run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["candlestick"],
    provide_context=True,
)


end_operator = DummyOperator(
    task_id="Stop_execution",
    dag=dag,
)

# setup DAG
start_operator >> [crypto_task, newsapi_task]
crypto_task >> crypto_bucket_task >> staging_crypto_task
staging_crypto_task >> [
    load_time_table,
    load_asset_base_table,
    load_asset_quote_table,
    load_asset_markets_table,
    load_candlestick_table,
]

newsapi_task >> news_bucket_task >> sentiment_task >> sent_bucket_task >> staging_news_task >> [
    load_articles_table,
    load_sources_table,
    load_candlestick_table,
]

[
    load_time_table,
    load_asset_base_table,
] >> run_quality_checks
[load_asset_quote_table, load_asset_markets_table] >> run_quality_checks
[load_articles_table, load_sources_table, load_candlestick_table] >> run_quality_checks
run_quality_checks >> end_operator
