import configparser
import requests
import logging
from requests.exceptions import ConnectionError
import boto3
from botocore.exceptions import ClientError
import json
import time
import sys

FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)


def create_redshift_cluster(
    client, role_arn, user, password, cluster_id="sparkify-redshift-cluster-1"
):
    """
    Create a new Redshift cluster composed of four dc2.large nodes

    Parameters
    ----------
    client : boto3.session.Session.client
    role_arn : str
    user : str
    password : str
    cluster_id : str

    """
    try:
        client.create_cluster(
            # HW
            ClusterType="multi-node",
            NodeType="dc2.large",
            NumberOfNodes=4,
            # Identifiers & Credentials
            DBName="sparkifydb",
            ClusterIdentifier=cluster_id,
            MasterUsername=user,
            MasterUserPassword=password,
            # Roles for S3
            IamRoles=[role_arn],
        )

        logger.info(f"{cluster_id} initialization started...")

    except ClientError:
        logger.exception("Issue while creating the cluster (see below)")


def check_cluster(client, cluster_id="sparkify-redshift-cluster-1", deleted=False):
    """
    Check the status of a cluster given its cluster_id

    Parameters
    ----------
    client : boto3.session.Session.client
    cluster_id : str
    deleted : bool

    Returns
    -------
    (cluster_id, cluster_status, endpoint, vpcId) : tuple

    """
    try:
        cluster_status = None
        endpoint = None
        vpc_id = None
        response = client.describe_clusters(ClusterIdentifier=cluster_id)
        logger.debug(response)
        cluster_status = response["Clusters"][0]["ClusterStatus"]

        if cluster_status == "available":
            endpoint = response["Clusters"][0]["Endpoint"]
            vpc_id = response["Clusters"][0]["VpcId"]
        return (cluster_id, cluster_status, endpoint, vpc_id)

    except ClientError:
        if deleted:
            return (cluster_id, cluster_status, endpoint, vpc_id)
        else:
            logger.exception("Issue while describing cluster (see below)")


def create_security_group(client, vpc_id, ip, port_from, port_to):
    """
    Create a new security group for the redshift cluster

    Parameters
    -----------
    client : boto3.session.Session.client
    vpcId, i.e. vpcId of the cluster : str
    ip, i.e. inbound ip for IpPermissions : str
    port_from : int
    port_to : int

    Returns
    -------
    security_group_id : str

    """
    try:
        response = client.create_security_group(
            GroupName="sparkifydb_security_group",
            Description="Security group for Redshift",
            VpcId=vpc_id,
        )
        security_group_id = response["GroupId"]

        response = client.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": port_from,
                    "ToPort": port_to,
                    "IpRanges": [{"CidrIp": f"{ip}/32"}],
                },
            ],
        )

        logger.info(
            "Security Group Created %s in vpc %s." % (security_group_id, vpc_id)
        )
        logger.debug(response)
        return security_group_id

    except ClientError:
        logger.exception("Issue while creating security group (see below)")


def replace_security_group(client, cluster_id, security_group_id):
    """
    Replace security group of a redshift cluster

    Parameters
    -----------
    client : boto3.session.Session.client
    cluster_id : str
    security_group_id : str

    """
    try:
        client.modify_cluster(
            ClusterIdentifier=cluster_id, VpcSecurityGroupIds=[security_group_id,]
        )
        logger.info(
            f"Security group {security_group_id} attached to cluster {cluster_id}"
        )
    except ClientError:
        logger.exception(f"Issue while replacing security group {security_group_id}")


def create_iam_role(
    client,
    role_name="sparkifydbRole",
    policy_arn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
):
    """
    Create a new AWS Role for Redshift clusters and attach a policy to read from S3 Buckets
    
    Parameters
    ----------
    client : boto3.session.Session.client
    role_name : str
    policy_arn : str

    Returns
    -------
    role_arn : str

    """
    try:
        logger.info("Creating a new IAM Role...")
        response = client.create_role(
            Path="/",
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
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
        logger.debug("Response after creating sparkifyRole follows...")
        logger.debug(response)
        logger.info(f"Attaching Policy {policy_arn}")
        response = client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        role_arn = client.get_role(RoleName=role_name)["Role"]["Arn"]
        logger.debug(f"Response after attaching policy {policy_arn} follows...")
        logger.debug(response)

        return role_arn

    except ClientError:
        logger.exception("Issue while creating the Role (see below)")


def get_my_ip():
    """
    Get client's ip to create stricter security rules, i.e. avoiding 0.0.0.0/0
    in IpPermissions

    """
    try:
        r = requests.get("https://ifconfig.co/json")
        ip = r.json()["ip"]
        logger.info(f"Your IP is {ip}")
        return ip

    except ConnectionError:
        logger.exception("Unable to retrieve your IP (see below).")


def free_resources():
    """
    Delete all the resources created to run the Redshift cluster

    Parameters
    ----------
    redshift_client : boto3.session.Session.client
    iam_client : boto3.session.Session.client
    ec2_client : boto3.session.Session.client

    """

    iam = boto3.client("iam")
    redshift = boto3.client("redshift")
    ec2 = boto3.client("ec2")

    # get cluster_id, role_name, security_group_id from the config file
    try:
        resources_config = configparser.ConfigParser()
        resources_config.read("resources.cfg")
        role_name = resources_config["RESOURCES"]["ROLE_NAME"]
        cluster_id = resources_config["RESOURCES"]["ClusterIdentifier"]
        security_group_id = resources_config["RESOURCES"]["SecurityGroupId"]

        # delete the cluster and skip the final snapshot
        redshift.delete_cluster(
            ClusterIdentifier=cluster_id, SkipFinalClusterSnapshot=True
        )

        _, cluster_status, _, _ = check_cluster(redshift, deleted=True)

        while cluster_status != None:
            logger.info("Cluster is being deleted... Please, wait...")
            _, cluster_status, _, _ = check_cluster(redshift, deleted=True)
            time.sleep(50)

        logger.info("Cluster deleted successfully")

        # detach the policy first
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
        # then delete the role
        iam.delete_role(RoleName=role_name)

        # delete the security group by id
        ec2.delete_security_group(GroupId=security_group_id)

        logger.info(
            f"Resources {cluster_id}, {role_name}, {security_group_id} deleted successfully"
        )

    except ClientError:
        logger.exception("Issue while deleting the resources (see below)")

    except Exception:
        logger.exception("Unable to locate resources.cfg")


def create_resources():
    """
    Create a Redshift cluster with all the resources needed and write 
    cluster info and IAM Role to dwh.cfg file
    
    """

    # boto3 automatically looks for key, secret, and region in these vars
    # See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

    # ROLE_NAME AND DB_NAME to consider in the config file
    ROLE_NAME = "sparkifydbRole"
    DB_NAME = "sparkifydb"

    iam = boto3.client("iam")
    redshift = boto3.client("redshift")
    ec2 = boto3.client("ec2")

    dwh_config = configparser.ConfigParser()
    resources_config = configparser.ConfigParser()
    # preserve case, i.e. avoid default conversion to lowercase
    dwh_config.optionxform = str
    dwh_config.read("dwh.cfg")

    # preserve case, i.e. avoid default conversion to lowercase
    resources_config.optionxform = str
    # create a new section to keep track of the resources for later deletion
    resources_config["RESOURCES"] = {}

    role_arn = create_iam_role(iam, role_name=ROLE_NAME)
    dwh_config["IAM_ROLE"]["ARN"] = role_arn

    # create a redshift cluster using the default identifier
    create_redshift_cluster(
        redshift,
        role_arn,
        dwh_config["CLUSTER"]["DB_USER"],
        dwh_config["CLUSTER"]["DB_PASSWORD"],
    )

    cluster_id, cluster_status, endpoint, vpc_id = check_cluster(redshift)

    while cluster_status != "available":
        logger.info("Cluster is being created... Please, wait...")
        cluster_id, cluster_status, endpoint, vpc_id = check_cluster(redshift)
        time.sleep(60)

    logger.info("Cluster created successfully.")

    # set cluster info in the config
    dwh_config["CLUSTER"]["HOST"] = endpoint["Address"]
    dwh_config["CLUSTER"]["DB_PORT"] = str(endpoint["Port"])
    dwh_config["CLUSTER"]["DB_NAME"] = DB_NAME

    # create a security group for the cluster (using the default name)
    security_group_id = create_security_group(
        ec2, vpc_id, get_my_ip(), endpoint["Port"], endpoint["Port"]
    )

    # replace default security group attached to the cluster
    replace_security_group(redshift, cluster_id, security_group_id)

    # keep track of the resources created: ROLE_NAME, cluster_id, security_group_id
    resources_config["RESOURCES"]["ROLE_NAME"] = ROLE_NAME
    resources_config["RESOURCES"]["ClusterIdentifier"] = cluster_id
    resources_config["RESOURCES"]["SecurityGroupId"] = security_group_id

    with open("resources.cfg", "w") as res_file:
        resources_config.write(res_file)

    with open("dwh.cfg", "w") as dwh_file:
        dwh_config.write(dwh_file)

    logger.info(
        "Resources created successfully and information written to dwh.cfg and resources.cfg"
    )


if __name__ == "__main__":
    # only one argument, using argparse or click seems overkilling
    if len(sys.argv) < 2:
        print("Usage: manage_cluster.py action.")
        print("Where action may be either create or delete")
        sys.exit(1)

    if sys.argv[1] == "create":
        create_resources()

    elif sys.argv[1] == "delete":
        free_resources()

    else:
        print(f"Unrecognized argument: {sys.argv[1]}")
        sys.exit(1)
