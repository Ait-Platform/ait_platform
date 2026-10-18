"""Lease-only authenticated R2 transport; no shared credentials or public URLs."""
import os
from urllib.parse import urlsplit
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from flask import current_app
from app.utils import cloudflare_r2 as shared_r2

DEFAULT_BUCKET='ait-lease-controlled-private'
R2KeyCollision=shared_r2.R2KeyCollision
MAX_BYTES=10*1024*1024


def setting(name,default=None):
    return current_app.config.get(name) or os.getenv(name) or default


def settings():
    if current_app.config.get('PM_LEASE_PRIVATE_STORAGE_APPROVED') is not True:
        raise ValueError('Private Lease R2 storage has not been verified and approved. Uploads and retrieval remain disabled.')
    bucket=setting('PM_LEASE_R2_BUCKET',DEFAULT_BUCKET)
    public=setting('R2_BUCKET_NAME')
    home=setting('SACE_HOME_R2_BUCKET')
    if bucket in {name for name in (public,home) if name}:
        raise ValueError('Lease storage must use its dedicated bucket, separate from HOME and public platform storage.')
    endpoint=setting('PM_LEASE_R2_ENDPOINT_URL')
    access=setting('PM_LEASE_R2_ACCESS_KEY')
    secret=setting('PM_LEASE_R2_SECRET_KEY')
    if not all((endpoint,access,secret)):
        raise ValueError('Configure PM_LEASE_R2_ENDPOINT_URL, PM_LEASE_R2_ACCESS_KEY and PM_LEASE_R2_SECRET_KEY. Shared R2 credentials are not used.')
    parsed=urlsplit(endpoint)
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError('PM_LEASE_R2_ENDPOINT_URL must be an HTTPS endpoint without embedded credentials, query or fragment.')
    return {'bucket':bucket,'endpoint':endpoint,'access':access,'secret':secret}


def r2_client(*,bucket=None):
    config=settings()
    if bucket is not None and bucket!=config['bucket']:raise ValueError('Lease R2 bucket scope does not match its dedicated configuration.')
    return boto3.client('s3',endpoint_url=config['endpoint'],aws_access_key_id=config['access'],
        aws_secret_access_key=config['secret'],region_name='auto',verify=True,
        config=Config(connect_timeout=5,read_timeout=15,retries={'max_attempts':2})),config['bucket']


def read_file_from_r2(key,*,bucket=None):
    client,bucket=r2_client(bucket=bucket)
    response=client.get_object(Bucket=bucket,Key=key)
    try:
        content=response['Body'].read(MAX_BYTES+1)
        if len(content)>MAX_BYTES:raise ValueError('Stored Lease document exceeds the supported size.')
        return content
    finally:response['Body'].close()


def upload_bytes_to_r2(key,content,*,bucket=None,content_type='application/pdf'):
    if not isinstance(content,bytes) or not 0<len(content)<=MAX_BYTES:raise ValueError('Lease document size is invalid.')
    client,bucket=r2_client(bucket=bucket)
    try:
        # Reuse the platform's conditional, immutable PUT implementation without
        # using its public/HOME client or credential configuration.
        shared_r2._put_immutable(client,bucket,key,content,content_type)
    except ClientError as error:
        code=str(error.response.get('Error',{}).get('Code'))
        if code not in {'412','PreconditionFailed','409','ConditionalRequestConflict'}:raise
        if read_file_from_r2(key,bucket=bucket)!=content:raise R2KeyCollision('Immutable Lease key contains different content.') from None
    if read_file_from_r2(key,bucket=bucket)!=content:raise R2KeyCollision('Lease R2 read-back did not match uploaded bytes.')
    return key
