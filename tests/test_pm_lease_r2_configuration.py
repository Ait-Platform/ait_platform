"""Offline dedicated Lease R2 transport tests. No real credentials or network."""
import ast,io,os,types,sys,importlib.util,unittest
from pathlib import Path
from unittest.mock import patch,MagicMock
from flask import Flask
from botocore.exceptions import ClientError
ROOT=Path(__file__).resolve().parents[1]

class LeaseR2ConfigurationTests(unittest.TestCase):
    def setUp(self):
        modules={name:types.ModuleType(name) for name in ('app','app.utils','app.program_billing')}
        for module in modules.values():module.__path__=[]
        module_patch=patch.dict(sys.modules,modules);module_patch.start();self.addCleanup(module_patch.stop)
        for name,file in [('app.utils.cloudflare_r2','app/utils/cloudflare_r2.py'),('app.program_billing.lease_r2','app/program_billing/lease_r2.py')]:
            spec=importlib.util.spec_from_file_location(name,ROOT/file);module=importlib.util.module_from_spec(spec)
            sys.modules[name]=module;spec.loader.exec_module(module);self.addCleanup(sys.modules.pop,name,None)
            if name.endswith('cloudflare_r2'):modules['app.utils'].cloudflare_r2=module;self.shared=module
            else:self.r2=module
        self.environment={'PM_LEASE_R2_ENDPOINT_URL':'https://lease-test.r2.cloudflarestorage.com',
            'PM_LEASE_R2_ACCESS_KEY':'lease-synthetic-access','PM_LEASE_R2_SECRET_KEY':'lease-synthetic-secret',
            'R2_BUCKET_NAME':'public-platform','R2_ENDPOINT_URL':'https://public-test.r2.cloudflarestorage.com',
            'R2_ACCESS_KEY':'public-synthetic-access','R2_SECRET_KEY':'public-synthetic-secret','SACE_HOME_R2_BUCKET':'home-private'}
        env=patch.dict(os.environ,self.environment,clear=True);env.start();self.addCleanup(env.stop)
        self.app=Flask('lease-r2-offline');self.app.config['PM_LEASE_PRIVATE_STORAGE_APPROVED']=True
        context=self.app.app_context();context.push();self.addCleanup(context.pop)

    def test_default_bucket_and_separate_credentials_tls_timeouts(self):
        before=dict(os.environ)
        with patch.object(self.r2.boto3,'client',return_value=object()) as client:
            _,bucket=self.r2.r2_client()
        self.assertEqual(bucket,'ait-lease-controlled-private')
        values=client.call_args.kwargs
        self.assertEqual(values['endpoint_url'],self.environment['PM_LEASE_R2_ENDPOINT_URL'])
        self.assertEqual(values['aws_access_key_id'],'lease-synthetic-access')
        self.assertEqual(values['aws_secret_access_key'],'lease-synthetic-secret')
        self.assertTrue(values['verify']);self.assertEqual(values['config'].connect_timeout,5);self.assertEqual(values['config'].read_timeout,15)
        self.assertEqual(dict(os.environ),before)

    def test_unapproved_by_default_and_no_client_creation(self):
        self.app.config.pop('PM_LEASE_PRIVATE_STORAGE_APPROVED')
        with patch.object(self.r2.boto3,'client') as client:
            with self.assertRaisesRegex(ValueError,'not been verified and approved'):self.r2.r2_client()
            with self.assertRaises(ValueError):self.r2.upload_bytes_to_r2('private/test',b'test')
            client.assert_not_called()
        tree=ast.parse((ROOT/'config.py').read_text(encoding='utf-8-sig'))
        config=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Config')
        assignment=next(n for n in config.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='PM_LEASE_PRIVATE_STORAGE_APPROVED' for t in n.targets))
        self.assertTrue(any(k.arg=='default' and isinstance(k.value,ast.Constant) and k.value.value is False for k in assignment.value.keywords))

    def test_shared_credentials_are_never_used_as_fallback(self):
        for field in ('PM_LEASE_R2_ENDPOINT_URL','PM_LEASE_R2_ACCESS_KEY','PM_LEASE_R2_SECRET_KEY'):
            with patch.dict(os.environ,{field:''}),patch.object(self.r2.boto3,'client') as client:
                with self.assertRaisesRegex(ValueError,'Shared R2 credentials are not used'):self.r2.r2_client()
                client.assert_not_called()

    def test_bucket_collisions_https_and_wrong_bucket_fail_closed(self):
        for bucket in ('public-platform','home-private'):
            with patch.dict(os.environ,{'PM_LEASE_R2_BUCKET':bucket}):
                with self.assertRaisesRegex(ValueError,'dedicated bucket'):self.r2.r2_client()
        for endpoint in ('http://lease-test.r2.cloudflarestorage.com','https://user:secret@lease-test.example','https://lease-test.example?token=private'):
            with patch.dict(os.environ,{'PM_LEASE_R2_ENDPOINT_URL':endpoint}):
                with self.assertRaisesRegex(ValueError,'HTTPS endpoint'):self.r2.r2_client()
        with self.assertRaisesRegex(ValueError,'scope'):self.r2.r2_client(bucket='another-bucket')

    def test_authenticated_immutable_upload_and_retrieval_only_lease_bucket(self):
        store={};calls=[];bodies=[]
        class S3:
            def put_object(inner,**values):
                self.assertEqual(values['IfNoneMatch'],'*');calls.append(('put',values['Bucket']))
                if values['Key'] in store:raise ClientError({'Error':{'Code':'PreconditionFailed'}},'PutObject')
                store[values['Key']]=values['Body']
            def get_object(inner,**values):
                calls.append(('get',values['Bucket']));body=io.BytesIO(store[values['Key']]);bodies.append(body);return {'Body':body}
        with patch.object(self.r2.boto3,'client',return_value=S3()),patch.object(self.shared,'r2_client',side_effect=AssertionError('Shared credentials must not be used')):
            self.assertEqual(self.r2.upload_bytes_to_r2('private/test',b'exact bytes'),'private/test')
            self.assertEqual(self.r2.read_file_from_r2('private/test'),b'exact bytes')
            self.r2.upload_bytes_to_r2('private/test',b'exact bytes')
            with self.assertRaises(self.r2.R2KeyCollision):self.r2.upload_bytes_to_r2('private/test',b'changed bytes')
        self.assertEqual(store['private/test'],b'exact bytes');self.assertEqual({bucket for _,bucket in calls},{'ait-lease-controlled-private'})
        self.assertTrue(all(body.closed for body in bodies))

    def test_oversized_stored_content_and_access_denied_do_not_fallback(self):
        body=io.BytesIO(b'x'*(10*1024*1024+1));client=MagicMock();client.get_object.return_value={'Body':body}
        with patch.object(self.r2.boto3,'client',return_value=client):
            with self.assertRaisesRegex(ValueError,'exceeds'):self.r2.read_file_from_r2('private/oversized')
        self.assertTrue(body.closed)
        client.get_object.side_effect=ClientError({'Error':{'Code':'AccessDenied'}},'GetObject')
        with patch.object(self.r2.boto3,'client',return_value=client),patch.object(self.shared,'r2_client') as shared:
            with self.assertRaises(ClientError):self.r2.read_file_from_r2('private/denied')
            shared.assert_not_called()
