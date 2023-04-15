# python3
from utils import MyConfigParser
from datetime import datetime
from io import StringIO
from typing import List
from utils import (
    get_json_objects,
    process_ds,
    flatten_json,
    request_api,
)
import pandas as pd
import boto3
import json


def create_ohlc_uri(pair: str, exchange: str, key: str, dt_format="%Y-%m-%d") -> str:
    """
    Create api uri given input parameters
    :param pair:        Cryptoasset pair (i.e., btc)
    :param exchange:    Cryptoasset exchange (i.e., kraken)
    :param key:         Cryptowatch access key
    :param dt_format:   Date format
    :return uri:        OHLC link
    """
    uri = "https://api.cryptowat.ch/markets/"
    uri += f"{exchange}/{pair}/ohlc?after={after}&before={before}&apikey={key}"
    return uri


def dump_to_s3(s3, bucket: str, series: dict, key: str) -> None:
    """
    Extract daily interval candlelist observations and dump to S3 bucket
    :param s3:          AWS S3 resource
    :param bucket:      AWS S3 bucket name
    :param series:      Data values as dict from `asset_pair_details`
    :param key:         Cryptowatch access key
    """
    pair = series["markets_pair"]
    exchange = series["markets_exchange"]
    uri = create_ohlc_uri(pair, exchange, key)
    res = request_api(uri)
    ohlc_data = json.loads(res)
    header = [
        "close_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "quote_volume",
    ]

    csv_buffer = StringIO()
    exchange = series["markets_exchange"]
    pair = series["markets_pair"]

    # filter daily
    daily_data = ohlc_data["result"]["86400"]
    if len(daily_data) > 0:
        df = pd.DataFrame(data=daily_data, columns=header)
        # update df to include series data
        for k, v in series.items():
            df[k] = v

        # convert close time to datetime
        df["close_time"] = df["close_time"].apply(
            lambda x: datetime.fromtimestamp(int(x))
        )
        df["close_date"] = df["close_time"].apply(lambda x: x.strftime("%Y-%m-%d"))
        df.to_csv(csv_buffer, index=False)

        try:
            s3.Object(
                bucket, f"ohlc-candlestick/{year}/{month}/{exchange}_{pair}.csv"
            ).put(Body=csv_buffer.getvalue())
        except Exception as e:
            msg = "ERROR: Could not dump market pairs data in S3."
            print(msg, e)
            return
    else:
        print(f"`{exchange}/{pair}/{year}/{month}` has no daily observations.")
        return


def query_pair_details(s3, bucket: str, pair_name: str, key: str) -> None:
    """
    Iterate over pair details and extract `pair_details` metadata
    :param s3:          AWS S3 resource
    :param bucket:      AWS S3 bucket name
    :param pair_name:   Cryptoasset name (i.e., btc)
    :param key:         Cryptowatch access key
    """
    asset_pair_url = f"https://api.cryptowat.ch/pairs/{pair_name}?apikey={key}"
    pair_details = get_json_objects(asset_pair_url)
    flat_pair = flatten_json(pair_details["result"])

    for row in flat_pair["markets"]:
        new_pair = {
            **{k: v for k, v in flat_pair.items() if k != "markets"},
            **{f"markets_{k}": v for k, v in row.items()},
        }

        dump_to_s3(s3, bucket, new_pair, key)


def CryptowatchToS3(
    ds: str, pair_base: List[str], pair_curr: List[str], **kwargs
) -> None:
    """
    Extract data using Cryptowatch REST API and dump to AWS S3
    :param ds:              Airflow macro reference `ds`
    :param pair_base:       List of cryptoasset names (i.e., [btc,eth,...])
    :param pair_curr:       List of cryptoasset currencies (i.e., [eur,usd,...])
    """
    my_config = MyConfigParser()

    global AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
    global CRYPTO_ACCESS_KEY_ID, S3_BUCKET

    AWS_SECRET_ACCESS_KEY = my_config.aws_secret_access_key()
    AWS_ACCESS_KEY_ID = my_config.aws_access_key_id()
    CRYPTO_ACCESS_KEY_ID = my_config.crypto_access_key_id()
    AWS_REGION = my_config.aws_region()
    S3_BUCKET = my_config.s3_bucket()

    global after, before, month, year
    after, before = process_ds(ds)
    month = datetime.strptime(ds, "%Y-%m-%d").month
    year = datetime.strptime(ds, "%Y-%m-%d").year

    pairs = [f"{base}{curr}" for base in pair_base for curr in pair_curr]

    s3 = boto3.resource(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    for pair in pairs:
        query_pair_details(s3, S3_BUCKET, pair, CRYPTO_ACCESS_KEY_ID)
    return
