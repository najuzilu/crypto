# python3
from datetime import datetime, timedelta
from typing import Tuple
from pathlib import Path
import configparser
import requests
import json


class MyConfigParser:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config_path = Path(__file__).parent.resolve().parents[1]
        self.config_name = "dwh.cfg"
        self.config.read(f"{self.config_path}/{self.config_name}")
        self.redshift_endpoint = ""

    def aws_access_key_id(self):
        return self.config.get("AWS", "AWS_ACCESS_KEY_ID")

    def aws_secret_access_key(self):
        return self.config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    def aws_region(self):
        return self.config.get("AWS", "AWS_REGION")

    def s3_bucket(self):
        return self.config.get("S3", "S3_BUCKET")

    def dwh_cluster_type(self):
        return self.config.get("DWH", "DWH_CLUSTER_TYPE")

    def dwh_num_nodes(self):
        return self.config.get("DWH", "DWH_NUM_NODES")

    def dwh_node_type(self):
        return self.config.get("DWH", "DWH_NODE_TYPE")

    def dwh_cluster_identifier(self):
        return self.config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

    def dwh_db(self):
        return self.config.get("DWH", "DWH_DB")

    def dwh_db_user(self):
        return self.config.get("DWH", "DWH_DB_USER")

    def dwh_db_password(self):
        return self.config.get("DWH", "DWH_DB_PASSWORD")

    def dwh_port(self):
        return self.config.get("DWH", "DWH_PORT")

    def dwh_iam_role_name(self):
        return self.config.get("DWH", "DWH_IAM_ROLE_NAME")

    def dwh_policy_arn(self):
        return self.config.get("DWH", "DWH_POLICY_ARN")

    def crypto_access_key_id(self):
        return self.config.get("CRYPTO", "CRYPTO_ACCESS_KEY_ID")

    def news_secret_access_key(self):
        return self.config.get("NEWSAPI", "NEWS_SECRET_ACCESS_KEY")

    def comprehend_access_role(self):
        return self.config.get("LAMBDA", "COMPREHEND_ACCESS_ROLE")

    def comprehend_access_policy(self):
        return self.config.get("LAMBDA", "COMPREHEND_ACCESS_POLICY")

    def comprehend_policy_arn(self):
        return self.config.get("LAMBDA", "COMPREHEND_POLICY_ARN")

    def update_redshift_endpoint(self, new_endpoint: str):
        self.redshift_endpoint = new_endpoint

    def get_redshift_endpoint(self):
        return self.redshift_endpoint


def request_api(uri: str) -> str:
    """
    Handle GET requests
    :param uri:             Api uri
    :return res.text:       If successful, response
    """
    try:
        res = requests.get(uri)
    except requests.exceptions as err:
        msg = f"ERROR: Could not GET uri: {uri}."
        print(msg, err)
        return

    if res.status_code == 200:
        return res.text
    else:
        msg = f"ERROR {res.status_code}: Could not GET uri: {uri}."
        print(msg)
        return


def get_json_objects(url: str) -> str:
    """
    Make Get request and if successful load data in json object
    :param url:             Api url
    :return
    """

    res = request_api(url)
    try:
        obs = json.loads(res)
    except Exception as e:
        msg = f"ERROR: Could not parse data as JSON."
        print(msg, e)
        return
    return obs


def flatten_json(obj: dict) -> dict:
    """
    Flatten json obj; doesn't handle lists
    """
    out = {}

    def flatten(obj, name=""):
        if type(obj) is dict:
            for key, value in obj.items():
                flatten(value, name + key + "_")
        else:
            out[name[:-1]] = obj

    flatten(obj)
    return out


def prev_month_date(ds: str) -> str:
    """
    Calculate previous month date
    :param ds:                      Airflow macro reference `ds`
    :return date_of_prev_month:     Date of previous month as string
    """
    # convert ds to datetime format
    ds_datetime = datetime.strptime(ds, "%Y-%m-%d")
    ds_day = ds_datetime.day
    # get last month date
    first_day_of_month = ds_datetime.replace(day=1)
    last_day_prev_month = first_day_of_month - timedelta(1)
    start_day_of_prev_month = first_day_of_month - timedelta(
        days=last_day_prev_month.day
    )
    # this handles day is out of range for prev month
    try:
        date_of_prev_month = start_day_of_prev_month.replace(day=ds_day)
    except ValueError:
        # if out of range, get last day of prev month
        date_of_prev_month = last_day_prev_month
    return date_of_prev_month  # .strftime("%Y-%m-%d")


def process_ds(ds: str) -> Tuple:
    """
    Calculate after and before `ds` timestemp
    :param ds:                      Airflow macro reference `ds`
    :return (after,before):         After/before date as timestamp
    """
    after = int(datetime.timestamp(prev_month_date(ds)))
    before = int(datetime.timestamp(datetime.strptime(ds, "%Y-%m-%d")))
    return after, before
