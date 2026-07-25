"""
Run this ONCE before the first pipeline run. Safe to re-run any time -
it checks "does this already exist?" before creating anything.

Creates:
  1. The S3 bucket where CSV + Iceberg files will live
  2. The Glue database (the "catalog" that Athena/Iceberg uses)
  3. The Athena workgroup (needs engine v3 to support Iceberg)

Run it:
    python setup_aws_infra.py
"""
import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

REGION = os.environ["AWS_REGION"]
BUCKET = os.environ["S3_BUCKET"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]

session = boto3.Session(region_name=REGION)


def create_bucket_if_missing():
    s3 = session.client("s3")
    try:
        s3.head_bucket(Bucket=BUCKET)
        print(f"[ok] Bucket '{BUCKET}' already exists")
    except ClientError:
        print(f"[create] Creating bucket '{BUCKET}' in {REGION}")
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(
                Bucket=BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
    # basic safe defaults
    s3.put_bucket_versioning(Bucket=BUCKET, VersioningConfiguration={"Status": "Enabled"})
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def create_glue_database_if_missing():
    glue = session.client("glue")
    try:
        glue.get_database(Name=GLUE_DATABASE)
        print(f"[ok] Glue database '{GLUE_DATABASE}' already exists")
    except glue.exceptions.EntityNotFoundException:
        print(f"[create] Creating Glue database '{GLUE_DATABASE}'")
        glue.create_database(DatabaseInput={"Name": GLUE_DATABASE})


def create_athena_workgroup_if_missing():
    athena = session.client("athena")
    try:
        athena.get_work_group(WorkGroup=ATHENA_WORKGROUP)
        print(f"[ok] Athena workgroup '{ATHENA_WORKGROUP}' already exists")
    except athena.exceptions.InvalidRequestException:
        print(f"[create] Creating Athena workgroup '{ATHENA_WORKGROUP}'")
        athena.create_work_group(
            Name=ATHENA_WORKGROUP,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{BUCKET}/athena-query-results/"
                },
                "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"},
            },
        )


if __name__ == "__main__":
    create_bucket_if_missing()
    create_glue_database_if_missing()
    create_athena_workgroup_if_missing()
    print("\nDone. Your S3 bucket, Glue database, and Athena workgroup are ready.")
