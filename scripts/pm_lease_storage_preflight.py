"""Read-only configuration/file checks. Never loads .env or starts the AIT factory."""
import argparse,hashlib,importlib.util,json,os
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--document-root',help='Explicit private root from the running service configuration')
    args=parser.parse_args()
    bucket=os.getenv('R2_BUCKET_NAME');home=os.getenv('SACE_HOME_R2_BUCKET')
    root=args.document_root or os.getenv('PM_LEASE_DOCUMENT_ROOT')
    report=dict(general_bucket=bucket,home_bucket=home,
        buckets_distinct=bool(bucket and home and bucket!=home),
        r2_credentials_present=all(bool(os.getenv(k)) for k in ('R2_ENDPOINT_URL','R2_ACCESS_KEY','R2_SECRET_KEY')),
        public_domain_configured=bool(os.getenv('R2_PUBLIC_DOMAIN')),
        public_prefix_privacy='NOT VERIFIED: audit every custom domain, r2.dev and Worker route',
        configured_document_root=root,document_files={},
        production_property_26='NOT VERIFIED: use authoritative database read-only preflight')
    if root:
        spec=importlib.util.spec_from_file_location('lease_document_constants',ROOT/'app/program_billing/lease_documents.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        for filename,expected in module.EXPECTED_SHA256.items():
            path=Path(root)/'R01'/filename
            try:valid=hashlib.sha256(path.read_bytes()).hexdigest()==expected
            except OSError:valid=False
            report['document_files'][filename]=dict(accessible=path.is_file(),approved_sha256_matches=valid)
    print(json.dumps(report,indent=2))
    return 0
if __name__=='__main__':raise SystemExit(main())
