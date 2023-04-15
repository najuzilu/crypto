# python 3
from airflow.models import Connection
from utils import MyConfigParser
from utils import request_api
from airflow import settings
import botocore
import boto3
import json


def get_role_arn(
    iam: botocore.client, DWH_POLICY_ARN: str, DWH_IAM_ROLE_NAME: str
) -> str:
    """
    Create IAM role that make Redshift able to access S3 bucket in
    ReadOnly mode, attaches policy to IAM role and gets the IAM role ARN
    :param iam:                     AWS IAM client
    :param DWH_POLICY_ARN:          policy ARN
    :param DWH_IAM_ROLE_NAME:       name of IAM role
    :return role_arn:               string containing IAM role ARN
    """
    try:
        iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift cluster to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            pass
        else:
            msg = "ERROR: Could not create IAM Role."
            print(msg, e)
            return

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=DWH_POLICY_ARN,)[
        "ResponseMetadata"
    ]["HTTPStatusCode"]

    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    return role_arn


def setup_s3_bucket(s3, bucket_name: str, region: str) -> None:
    """
    Create/empty S3 bucket
    :param s3:                      S3 resource
    :param bucket_name:             S3 bucket name
    :param region:                  S3 region
    """
    my_bucket = s3.Bucket(bucket_name)

    if my_bucket in s3.buckets.all():
        print(f"S3 bucket `{bucket_name}` exists.")

        size = sum(1 for _ in my_bucket.objects.all())
        print(
            f"""{size} objects found in `{bucket_name}`.
            Emptying `{bucket_name}`..."""
        )

        my_bucket.objects.all().delete()
        size = sum(1 for _ in my_bucket.objects.all())
        if size == 0:
            print(f"`{bucket_name}` emptied successfully.")
        else:
            raise ValueError(
                f"""`{bucket_name}` **NOT** emptied successfully.
                {size} objects found in `{bucket_name}` ."""
            )
            return
    else:
        print(f"Creating `{bucket_name}` bucket...")
        try:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": region,
                },
            )
        except Exception as e:
            if e.response["Error"]["Code"] == "InvalidBucketName":
                msg = f"""ERROR: `{bucket_name}` bucket name is not valid.
                    Try a new bucket name."""
            else:
                msg = f"ERROR: Could not create S3 bucket `{bucket_name}`."
            print(msg, e)
            return
        print(f"`{bucket_name}` bucket created successfully.")


def setup_redshift_cluster(ec2, redshift, roleArn: str) -> str:
    """
    Setup Redshift on AWS cluster
    :param ec2:             AWS ec2 client
    :param redshift:        AWS redshift client
    :param roleArn:         IAM role arn
    :return endpoint:       AWS redshift endpoint arn
    """
    group_name = "redshift_security_group"
    response = ec2.describe_security_groups(
        Filters=[dict(Name="group-name", Values=[group_name])]
    )

    if len(response["SecurityGroups"]) > 0:
        group_id = response["SecurityGroups"][0]["GroupId"]
    else:
        msg = f"ERROR: Security Group with name `{group_name}` does not exist."
        print(msg)
        return

    # create Redshift cluster
    try:
        redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn],
            VpcSecurityGroupIds=[
                group_id,
            ],
        )
    except Exception as e:
        if e.response["Error"]["Code"] == "ClusterAlreadyExists":
            pass
        else:
            msg = "ERROR: Could not create a Redshift cluster."
            print(msg, e)
            return

    # get cluster status
    clusterProp = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
        "Clusters"
    ][0]

    clusterStatus = clusterProp["ClusterStatus"]

    while clusterStatus == "creating":
        clusterProp = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
        )["Clusters"][0]
        clusterStatus = clusterProp["ClusterStatus"]

    print(f"\nCluster created successfully. Cluster status='{clusterStatus}'")

    # try to get old Cidr if it exists
    response = ec2.describe_security_groups(
        GroupIds=[group_id],
        Filters=[
            {"Name": "ip-permission.from-port", "Values": [DWH_PORT]},
            {"Name": "ip-permission.protocol", "Values": ["tcp"]},
            {"Name": "ip-permission.to-port", "Values": [DWH_PORT]},
        ],
    )

    # if inbound rule exists, revoke all
    oldCidrs = []
    if len(response["SecurityGroups"]) > 0:
        ipPerms = response["SecurityGroups"][0]["IpPermissions"]
        for ip_perm in ipPerms:
            for cidr in ip_perm["IpRanges"]:
                oldCidrs.append(cidr["CidrIp"])

        # revoke old cidrs
        for old_cidr in oldCidrs:
            response = ec2.revoke_security_group_ingress(
                GroupId=group_id,
                IpProtocol="tcp",
                CidrIp=old_cidr,
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT),
            )

    # get my IP
    get_ip_url = "http://whatismyip.akamai.com"
    my_ip = f"{request_api(get_ip_url)}/32"

    # authorize inbound security rule from current IP address
    response = ec2.authorize_security_group_ingress(
        GroupId=group_id,
        IpProtocol="tcp",
        CidrIp=my_ip,
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT),
    )

    try:
        endpoint = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
            "Clusters"
        ][0]["Endpoint"]["Address"]
    except Exception as e:
        msg = "ERROR: Could not retrieve cluster endpoint."
        print(msg, e)
        return
    return endpoint


def SetupAWS() -> None:
    """
    Setup AWS resources such as S3 buckets, Redshift cluster, roles/permissions/policies
    """
    my_config = MyConfigParser()

    global AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID, AWS_REGION
    global S3_BUCKET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE
    global DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD
    global DWH_PORT, DWH_IAM_ROLE_NAME, DWH_POLICY_ARN

    AWS_SECRET_ACCESS_KEY = my_config.aws_secret_access_key()
    AWS_ACCESS_KEY_ID = my_config.aws_access_key_id()
    AWS_REGION = my_config.aws_region()
    S3_BUCKET = my_config.s3_bucket()

    DWH_CLUSTER_TYPE = my_config.dwh_cluster_type()
    DWH_NUM_NODES = my_config.dwh_num_nodes()
    DWH_NODE_TYPE = my_config.dwh_node_type()
    DWH_CLUSTER_IDENTIFIER = my_config.dwh_cluster_identifier()
    DWH_DB = my_config.dwh_db()
    DWH_DB_USER = my_config.dwh_db_user()
    DWH_DB_PASSWORD = my_config.dwh_db_password()
    DWH_PORT = my_config.dwh_port()
    DWH_IAM_ROLE_NAME = my_config.dwh_iam_role_name()
    DWH_POLICY_ARN = my_config.dwh_policy_arn()

    # setup iam, ec2, s3, redshift clients
    iam = boto3.client(
        "iam",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    role_arn = get_role_arn(iam, DWH_POLICY_ARN, DWH_IAM_ROLE_NAME)

    ec2 = boto3.client(
        "ec2",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    s3 = boto3.resource(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    redshift = boto3.client(
        "redshift",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    # setup s3 bucket
    setup_s3_bucket(s3, S3_BUCKET, AWS_REGION)

    # setup redshift cluster
    cluster_endpoint = setup_redshift_cluster(ec2, redshift, role_arn)
    if cluster_endpoint:
        my_config.update_redshift_endpoint(cluster_endpoint)

    # setup airflow connections
    session = settings.Session()

    # create aws connection obj
    aws_conn = Connection(
        conn_id="aws-credentials",
        conn_type="amazon web services",
        login=AWS_ACCESS_KEY_ID,
        password=AWS_SECRET_ACCESS_KEY,
    )

    # create redshift connection obj
    redshift_conn = Connection(
        conn_id="redshift",
        conn_type="postgres",
        host=my_config.get_redshift_endpoint(),
        schema=DWH_DB,
        login=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        port=DWH_PORT,
    )

    aws_conn_name = (
        session.query(Connection).filter(Connection.conn_id == aws_conn.conn_id).first()
    )

    redshift_conn_name = (
        session.query(Connection)
        .filter(Connection.conn_id == redshift_conn.conn_id)
        .first()
    )

    # add/update aws and redshift connections
    if str(aws_conn_name) == str(aws_conn.conn_id):
        session.query(Connection).filter(
            Connection.conn_id == aws_conn.conn_id
        ).first().set_password(aws_conn.password)
        print(f"Connection {aws_conn.conn_id} already exists")
    else:
        session.add(aws_conn)
        session.commit()

    if str(redshift_conn_name) == str(redshift_conn.conn_id):
        session.query(Connection).filter(
            Connection.conn_id == redshift_conn.conn_id
        ).first().set_password(redshift_conn.password)
        print(f"Connection {redshift_conn.conn_id} already exists")
    else:
        session.add(redshift_conn)
        session.commit()
