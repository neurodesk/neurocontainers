from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from requests.adapters import BaseAdapter
from requests.structures import CaseInsensitiveDict

from tools.copy_swift_object import copy_object


class SwiftAdapter(BaseAdapter):
    """Model Swift's server-side copy limit at the HTTP request boundary."""

    def __init__(self, *, slo=True, corrupt_copy=False):
        self.source = {'Content-Length': '14651330560', 'ETag': 'segments-etag'}
        if slo:
            self.source['X-Static-Large-Object'] = 'True'
        self.destination = None
        self.corrupt_copy = corrupt_copy
        self.requests = []

    def send(self, request, **kwargs):
        self.requests.append(request)
        response = requests.Response()
        response.status_code = 200
        response._content = b''
        response.request = request
        path = urlsplit(request.url).path
        if request.method == 'PUT':
            if parse_qs(urlsplit(request.url).query) != {'multipart-manifest': ['get']}:
                response.status_code = 413
            else:
                assert request.headers['X-Copy-From'] == '/neurodesk/staged/image.simg'
                assert request.body in (None, b'')
                self.destination = dict(self.source)
                if self.corrupt_copy:
                    self.destination['ETag'] = 'wrong-segments'
                response.status_code = 201
        elif request.method == 'HEAD':
            response.headers = CaseInsensitiveDict(
                self.source if path.endswith('/staged/image.simg') else self.destination
            )
        else:
            pytest.fail('The copy must not download or upload object bodies')
        return response

    def close(self):
        pass


def connection(**kwargs):
    client = requests.Session()
    adapter = SwiftAdapter(**kwargs)
    client.mount('https://swift.test/', adapter)
    return client, adapter


def test_large_object_copy_preserves_manifest_without_transferring_segments():
    client, adapter = connection()
    copy_object(client, 'https://swift.test/v1/account', 'staged/image.simg',
                'image.simg', require_slo=True, expected_size=14651330560)
    assert adapter.destination == adapter.source
    assert [r.method for r in adapter.requests] == ['HEAD', 'PUT', 'HEAD']


def test_recovery_refuses_a_non_slo_source_before_writing():
    client, adapter = connection(slo=False)
    with pytest.raises(ValueError, match='not a static large object'):
        copy_object(client, 'https://swift.test/v1/account', 'staged/image.simg',
                    'image.simg', require_slo=True)
    assert adapter.destination is None


def test_recovery_refuses_source_size_disagreement_before_writing():
    client, adapter = connection()
    with pytest.raises(ValueError, match='does not match'):
        copy_object(client, 'https://swift.test/v1/account', 'staged/image.simg',
                    'image.simg', expected_size=123)
    assert adapter.destination is None


def test_copy_rejects_changed_segments_even_when_size_matches():
    client, _ = connection(corrupt_copy=True)
    with pytest.raises(ValueError, match='different ETag'):
        copy_object(client, 'https://swift.test/v1/account', 'staged/image.simg', 'image.simg')


def test_small_plain_object_is_supported():
    client, adapter = connection(slo=False)
    adapter.source['Content-Length'] = '128'
    copy_object(client, 'https://swift.test/v1/account', 'staged/image.simg', 'image.simg')
    assert adapter.destination == adapter.source


@pytest.mark.parametrize('field,value', [
    ('--source-sha', 'not-a-sha'),
    ('--container', '../other'),
    ('--sif', 'other_1.0_20261004.simg'),
    ('--sif', 'tool_../other_20261004.simg'),
])
def test_cli_rejects_invalid_repair_identity_before_network(monkeypatch, field, value):
    from tools.copy_swift_object import main

    arguments = ['copy_swift_object', '--source-sha', 'a' * 40,
                 '--container', 'tool', '--sif', 'tool_1.0_20261004.simg']
    arguments[arguments.index(field) + 1] = value
    monkeypatch.setattr('sys.argv', arguments)
    with pytest.raises(SystemExit) as result:
        main()
    assert result.value.code == 2
