import os
import uuid
import boto3
from werkzeug.utils import secure_filename
from botocore.exceptions import ClientError

def upload_file_to_r2(file_storage):
    """
    Uploads a Werkzeug FileStorage object to Cloudflare R2 and returns the public URL.
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
    unique_filename = f"organogram/{uuid.uuid4().hex}.{extension}"
    
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
    
    return f"{domain}/{unique_filename}"
