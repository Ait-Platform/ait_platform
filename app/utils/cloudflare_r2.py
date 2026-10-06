import os
import uuid
import boto3
from werkzeug.utils import secure_filename
from botocore.exceptions import ClientError

def upload_file_to_r2(file_storage, *, prefix="organogram", return_key=False):
    """
    Uploads a Werkzeug FileStorage object to R2. Existing callers receive a public URL.
    Internal callers may supply an object prefix and request the stored object key.
    Requires R2_ENDPOINT_URL, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET_NAME, R2_PUBLIC_DOMAIN in .env
    """
    endpoint = os.getenv("R2_ENDPOINT_URL")
    access_key = os.getenv("R2_ACCESS_KEY")
    secret_key = os.getenv("R2_SECRET_KEY")
    bucket = os.getenv("R2_BUCKET_NAME")
    domain = os.getenv("R2_PUBLIC_DOMAIN") # e.g. https://pub-xyz.r2.dev
    
    if not all([endpoint, access_key, secret_key, bucket, domain]):
        raise ValueError("Missing R2 configuration environment variables.")
        
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto'
    )
    
    original_filename = secure_filename(file_storage.filename)
    extension = original_filename.rsplit('.', 1)[1].lower() if '.' in original_filename else 'bin'
    
    # Generate unique filename to prevent collisions and cache issues
    unique_filename = f"{prefix}/{uuid.uuid4().hex}.{extension}"
    
    # Read file content
    file_content = file_storage.read()
    
    # Upload
    s3.put_object(
        Bucket=bucket,
        Key=unique_filename,
        Body=file_content,
        ContentType=file_storage.content_type
    )
    
    # Reset pointer if needed later
    file_storage.seek(0)
    
    return unique_filename if return_key else f"{domain.rstrip('/')}/{unique_filename}"


def r2_client(*, bucket=None):
    """Authenticated object API; public URL configuration is deliberately separate."""
    endpoint = os.getenv("R2_ENDPOINT_URL")
    access_key = os.getenv("R2_ACCESS_KEY")
    secret_key = os.getenv("R2_SECRET_KEY")
    bucket = bucket or os.getenv("R2_BUCKET_NAME")
    if not all([endpoint, access_key, secret_key, bucket]):
        raise ValueError("Missing R2 configuration environment variables.")
    from botocore.config import Config
    return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access_key,
        aws_secret_access_key=secret_key, region_name="auto",
        config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 2})), bucket


def head_file_from_r2(key, *, bucket=None):
    """Return authenticated metadata, or None only for an absent object."""
    client, bucket = r2_client(bucket=bucket)
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code")) in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def read_file_from_r2(key, *, bucket=None):
    """Read server-side bytes; callers enforce authorization and integrity."""
    client, bucket = r2_client(bucket=bucket)
    response = client.get_object(Bucket=bucket, Key=key)
    try:
        return response["Body"].read()
    finally:
        response["Body"].close()


class R2KeyCollision(ValueError):
    """An immutable object key already contains different bytes."""


def _put_immutable(client, bucket, key, content, content_type):
    values = dict(Bucket=bucket, Key=key, Body=content, ContentType=content_type)
    if not hasattr(client, "meta") or "IfNoneMatch" in (
            client.meta.service_model.operation_model("PutObject").input_shape.members):
        return client.put_object(**values, IfNoneMatch="*")
    # Older installed botocore models lack this parameter. Add the condition
    # before signing, so it is authenticated; never use an unconditional PUT.
    def condition(request, **kwargs):
        request.headers["If-None-Match"] = "*"
    event = "before-sign.s3.PutObject"
    client.meta.events.register(event, condition)
    try:
        return client.put_object(**values)
    finally:
        client.meta.events.unregister(event, condition)


def upload_bytes_to_r2(key, content, *, bucket=None, content_type="application/pdf"):
    """Create an exact immutable key, returning only its key, never a URL.

    Conditional creation also prevents concurrent writers from replacing an object.
    An existing object is accepted only when its actual bytes match.
    """
    client, bucket = r2_client(bucket=bucket)
    try:
        _put_immutable(client, bucket, key, content, content_type)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code"))
        if code not in {"412", "PreconditionFailed", "409", "ConditionalRequestConflict"}:
            raise
        if read_file_from_r2(key, bucket=bucket) != content:
            raise R2KeyCollision("Immutable R2 key contains different content.") from exc
    if read_file_from_r2(key, bucket=bucket) != content:
        raise R2KeyCollision("R2 content did not match the uploaded bytes.")
    return key
