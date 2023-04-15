# python3
from utils import MyConfigParser
from pathlib import Path
from io import StringIO
import pandas as pd
import boto3
import json


def setup_aws_comprehend(iam) -> None:
    """
    Setup AWS Comprehend and run sentiment analysis on text data
    :param iam:     AWS IAM role
    """
    # path where json docs are located
    path = Path(__file__).parent.resolve()

    # trust policy file
    trust_policy = json.loads(open(f"{path}/comprehend-trust-policy.json").read())

    # create IAM role
    try:
        iam.create_role(
            Path="/",
            RoleName=COMPREHEND_ACCESS_ROLE,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
        )

        # # get policy arn to run Amazon Comprehend analysis jobs
        # trust_role_arn = iam.get_role(RoleName=COMPREHEND_ACCESS_ROLE)["Role"]["Arn"]
    except Exception as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            msg = f"IAM role `{COMPREHEND_ACCESS_ROLE}` exists."
            print(msg)
        else:
            msg = f"ERROR: Could not create IAM role `{COMPREHEND_ACCESS_ROLE}`."
            print(msg, e)
            return

    # create IAM policy to grant Amazon Comprehend access
    # to the specified S3 bucket
    access_policy = json.loads(open(f"{path}/comprehend-access-policy.json").read())

    try:
        access_res = iam.create_policy(
            Path="/",
            PolicyName=COMPREHEND_ACCESS_POLICY,
            PolicyDocument=json.dumps(access_policy),
        )

        policy_arn = access_res["Policy"]["Arn"]

        # attach IAM policy to IAM role.
        # now you have an IAM role that has
        # a trust policy for Amazon Comprehend
        # and an access policy that grants
        # Amazon Comprehend access to your S3 bucket
        iam.attach_role_policy(PolicyArn=policy_arn, RoleName=COMPREHEND_ACCESS_ROLE)
    except Exception as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            msg = f"IAM policy `{COMPREHEND_ACCESS_POLICY}` exists."
            print(msg)
            pass
        else:
            msg = f"ERROR: Could not create IAM policy `{COMPREHEND_ACCESS_POLICY}`."
            print(msg, e)
            return


def get_sentiment(df: pd.DataFrame, column_name: str) -> None:
    """
    Setup comprehend client, fix date (transfer!), read S3 object, run sentiment
    analysis and dump to new S3 bucket
    :param df:                  Data in dataframe format
    :param column_name:         Name of column on which sentiment should be applied
    """

    def _get_sentiment(client, text: str):
        """
        Helper method for `get_sentiment`
        """
        sentiment = client.detect_sentiment(Text=text, LanguageCode="en")
        sent_type = sentiment["Sentiment"]
        sent_score = sentiment["SentimentScore"]
        return (sent_type, sent_score)

    # prior to dump, add sentiment
    comprehend = boto3.client(
        "comprehend",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # move over to api >>>
    try:
        df["publishedAt"] = pd.to_datetime(
            df["publishedAt"], format="%Y-%m-%dT%H:%M:%SZ"
        )
    except Exception as e:
        pass

    try:
        df["publishedAt"] = pd.to_datetime(
            df["publishedAt"], format="%Y-%m-%d %H:%M:%S"
        )
    except Exception as e:
        msg = "ERROR: Could not format datetime."
        print(msg, e)
        return

    df["published_date"] = df["publishedAt"].apply(lambda x: x.strftime("%Y-%m-%d"))

    # create new sentiment result columns
    for idx, row in df.iterrows():
        sentiment, scores = _get_sentiment(comprehend, row[column_name])

        df.loc[idx, "sentiment"] = sentiment
        df.loc[idx, "positive_score"] = scores["Positive"]
        df.loc[idx, "negative_score"] = scores["Negative"]
        df.loc[idx, "mixed_score"] = scores["Mixed"]
        df.loc[idx, "neutral_score"] = scores["Neutral"]


def DetectNewsSentiment(column_name: str) -> None:
    """
    Detect sentiment on collection of text using AWS Comprehend
    :param column_name:         Name of column on which sentiment should be applied
    """
    my_config = MyConfigParser()

    global AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID, AWS_REGION
    global S3_BUCKET, COMPREHEND_ACCESS_ROLE, COMPREHEND_ACCESS_POLICY

    AWS_SECRET_ACCESS_KEY = my_config.aws_secret_access_key()
    AWS_ACCESS_KEY_ID = my_config.aws_access_key_id()
    AWS_REGION = my_config.aws_region()
    S3_BUCKET = my_config.s3_bucket()
    COMPREHEND_ACCESS_ROLE = my_config.comprehend_access_role()
    COMPREHEND_ACCESS_POLICY = my_config.comprehend_access_policy()

    iam = boto3.client(
        "iam",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # create iam role for amazon comprehend
    setup_aws_comprehend(iam)

    # Start sentiment analysis job
    s3 = boto3.resource(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    prefix = "news-articles"
    sent_prefix = f"{prefix}-sent"
    bucket = s3.Bucket(S3_BUCKET)
    objs = bucket.objects.filter(Prefix=prefix)

    # iterate over prefix objects
    for obj in objs:
        df = pd.read_csv(obj.get()["Body"])
        # detect sentiment on column_name
        get_sentiment(df, column_name)
        # dump file content to new s3 bucket prefix
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.Object(S3_BUCKET, obj.key.replace(prefix, sent_prefix)).put(
            Body=csv_buffer.getvalue()
        )

    print(f"{__file__} ran successfully.")
    return
