"""Tariff audit adapters over existing persistent disk/private PM R2 only."""
from importlib import import_module
from pathlib import Path,PurePosixPath
import os
from flask import current_app

PRIVATE_PREFIX='private/pm-tariff-audit/'
MAX_BYTES=512*1024


def persistent_mount(root):
    """Render: reject root/image/tmpfs paths masquerading as persistent disks."""
    root=Path(root).resolve()
    try:lines=Path('/proc/self/mountinfo').read_text(encoding='utf8').splitlines()
    except OSError:return False
    matches=[]
    for line in lines:
        try:
            left,right=line.split(' - ',1);fields=left.split()
            mount=Path(fields[4].replace('\\040',' ')).resolve();kind=right.split()[0]
            if root.is_relative_to(mount):matches.append((len(mount.parts),mount,kind,fields[5]))
        except (ValueError,IndexError):continue
    if not matches:return False
    _,mount,kind,options=max(matches,key=lambda item:item[0])
    return mount!=Path('/') and kind in ('ext4','xfs','btrfs','zfs','nfs','nfs4') and 'rw' in options.split(',')


class PrivateR2:
    """Same approved private PM bucket and immutable transport as Utilities evidence."""
    def __init__(self):
        self.api=import_module('app.program_billing.lease_r2')
        self.api.settings() # Existing private-storage approval and bucket isolation.

    def full_key(self,key):
        path=PurePosixPath(key)
        if (not key.startswith('municipal-tariff-review/') or path.is_absolute()
                or '..' in path.parts or '\\' in key):raise ValueError('Tariff audit storage scope is invalid.')
        return PRIVATE_PREFIX+key

    def failure(self,exc):
        code=getattr(exc,'response',{}).get('Error',{}).get('Code','')
        label=code if code in ('AccessDenied','NoSuchBucket','InvalidAccessKeyId','SignatureDoesNotMatch') else type(exc).__name__
        current_app.logger.warning('Private tariff audit storage failed (%s)',label)
        return ValueError('Private tariff approval storage is unavailable ('+label+'). Check the existing private PM bucket configuration and tariff-audit prefix permissions. No approval was confirmed.')

    def put(self,key,content,mime):
        full=self.full_key(key)
        if not isinstance(content,bytes) or not 0<len(content)<=MAX_BYTES:raise ValueError('Tariff audit size is invalid.')
        try:
            self.api.upload_bytes_to_r2(full,content,content_type=mime)
            if self.api.read_file_from_r2(full)!=content:raise ValueError('Audit read-back mismatch.')
        except Exception as exc:raise self.failure(exc) from None

    def get(self,key):
        full=self.full_key(key)
        try:
            content=self.api.read_file_from_r2(full)
            if not 0<len(content)<=MAX_BYTES:raise ValueError('Invalid audit size.')
            return content
        except Exception as exc:raise self.failure(exc) from None

    def keys(self,scope):
        prefix=self.full_key(scope);result=[];continuation=None
        try:
            client,bucket=self.api.r2_client()
            while True:
                values=dict(Bucket=bucket,Prefix=prefix,MaxKeys=1000)
                if continuation:values['ContinuationToken']=continuation
                response=client.list_objects_v2(**values)
                for row in response.get('Contents',[]):
                    key=row['Key']
                    if not key.startswith(prefix):raise ValueError('Audit listing escaped its scope.')
                    if key.endswith('.json'):result.append(key[len(PRIVATE_PREFIX):])
                if len(result)>10000:raise ValueError('Audit listing exceeds supported history; none was discarded.')
                if not response.get('IsTruncated'):return sorted(result)
                following=response.get('NextContinuationToken')
                if not following or following==continuation:raise ValueError('Audit listing pagination failed.')
                continuation=following
        except Exception as exc:raise self.failure(exc) from None


def backend(documents):
    root=current_app.config.get('PM_TARIFF_REVIEW_ROOT') or os.getenv('PM_TARIFF_REVIEW_ROOT')
    source_root=current_app.config.get('PM_TARIFF_DOCUMENT_ROOT') or os.getenv('PM_TARIFF_DOCUMENT_ROOT')
    if not root and source_root:root=Path(source_root)/'private-review'
    if root:
        if os.getenv('RENDER') and not persistent_mount(root):
            raise ValueError('Tariff approval audit directory is not on a verified persistent mounted disk. No approval can be saved to ephemeral Render storage.')
        return documents.Disk(root)
    local=Path(current_app.instance_path)/'pm_tariff_review'
    if not os.getenv('RENDER') and not current_app.testing and local.exists():
        return documents.Disk(local) # Never hide an existing local ledger by switching transport.
    if current_app.config.get('PM_LEASE_PRIVATE_STORAGE_APPROVED') is True:
        return PrivateR2()
    if os.getenv('RENDER'):
        raise ValueError('Persistent private tariff approval storage is unavailable. Configure a private path on the existing persistent disk or the existing approved private PM R2 transport. Public R2 and ephemeral storage are not permitted.')
    if current_app.testing:raise ValueError('Tests must configure isolated tariff approval storage.')
    return documents.Disk(Path(current_app.instance_path)/'pm_tariff_review')
