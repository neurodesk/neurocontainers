"""Publish a staged Swift object without flattening its large-object manifest."""
from __future__ import annotations

import argparse
import os
import re
from urllib.parse import quote

import requests


def copy_object(session, storage_url: str, source: str, destination: str,
                *, require_slo: bool = False, expected_size: int | None = None) -> None:
    source_url = f"{storage_url.rstrip('/')}/neurodesk/{quote(source, safe='/')}"
    destination_url = f"{storage_url.rstrip('/')}/neurodesk/{quote(destination, safe='')}"
    response = session.head(source_url)
    response.raise_for_status()
    before = response.headers
    is_slo = before.get('X-Static-Large-Object', '').lower() == 'true'
    if require_slo and not is_slo:
        raise ValueError('Staged object is not a static large object')
    if expected_size is not None and int(before['Content-Length']) != expected_size:
        raise ValueError('Staged object size does not match the published S3 object')

    # Swift otherwise concatenates the segments and rejects objects over 5 GB.
    response = session.put(
        destination_url, params={'multipart-manifest': 'get'}, data=b'',
        headers={'X-Copy-From': f"/neurodesk/{quote(source, safe='/')}"},
    )
    response.raise_for_status()
    response = session.head(destination_url)
    response.raise_for_status()
    after = response.headers
    for header in ('Content-Length', 'ETag', 'X-Static-Large-Object', 'X-Object-Manifest'):
        if before.get(header) != after.get(header):
            raise ValueError(f'Copied object has a different {header}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-sha', required=True)
    parser.add_argument('--container', required=True)
    parser.add_argument('--sif', required=True)
    parser.add_argument('--verify-s3', action='store_true')
    parser.add_argument('--require-slo', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9a-f]{40}', args.source_sha):
        parser.error('--source-sha must be a full lowercase commit SHA')
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]*', args.container):
        parser.error('--container must be a recipe container name')
    if not re.fullmatch(re.escape(args.container) + r'_[A-Za-z0-9_.+-]+_[0-9]{8}\.simg', args.sif):
        parser.error('--sif must be a dated SIF filename for the selected container')
    expected_size = None
    if args.verify_s3:
        response = requests.head(
            f'https://neurocontainers.s3.us-east-2.amazonaws.com/{quote(args.sif, safe="")}',
            timeout=30,
        )
        response.raise_for_status()
        expected_size = int(response.headers['Content-Length'])

    from keystoneauth1 import session
    from keystoneauth1.identity import v3

    auth = v3.ApplicationCredential(
        auth_url=os.environ['OS_AUTH_URL'],
        application_credential_id=os.environ['OS_APPLICATION_CREDENTIAL_ID'],
        application_credential_secret=os.environ['OS_APPLICATION_CREDENTIAL_SECRET'],
    )
    client = session.Session(auth=auth, timeout=60)
    storage_url = client.get_endpoint(service_type='object-store', interface='public')
    source = f'promotion-staging/{args.source_sha}/{args.container}/{args.sif}'
    copy_object(client, storage_url, source, args.sif,
                require_slo=args.require_slo, expected_size=expected_size)
    print(f'Verified Nectar object: {args.sif}')


if __name__ == '__main__':
    main()
