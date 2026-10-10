"""Check the mask and pinned main.py dispatcher over actual MRD TCP."""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

import ismrmrd
import numpy as np

OPTIONS = {'mode': 'readout-edge-zero', 'input_domain': 'raw-uniform-cartesian-kspace',
           'measurement_role': 'imaging'}


def metadata():
    x = ismrmrd.xsd
    space = x.encodingSpaceType(matrixSize=x.matrixSizeType(x=8, y=8, z=1),
                               fieldOfView_mm=x.fieldOfViewMm(x=80, y=80, z=4))
    return x.ismrmrdHeader(
        acquisitionSystemInformation=x.acquisitionSystemInformationType(systemFieldStrength_T=3.0),
        experimentalConditions=x.experimentalConditionsType(H1resonanceFrequency_Hz=123000000),
        encoding=[x.encodingType(encodedSpace=space, reconSpace=space,
                                 trajectory=x.trajectoryType.CARTESIAN),
                  x.encodingType(encodedSpace=space, reconSpace=space,
                                 trajectory=x.trajectoryType.RADIAL)])


def acquisition(samples=100, pre=0, post=0, trajectory=0):
    a = ismrmrd.Acquisition()
    a.resize(samples, 3, trajectory)
    a.available_channels = 3
    a.discard_pre, a.discard_post = pre, post
    a.center_sample = pre + (samples - pre - post) // 2
    a.idx.kspace_encode_step_1, a.idx.kspace_encode_step_2 = 317, 91
    a.scan_counter = 123
    a.user_int[7] = 713
    a.data[:] = np.arange(3 * samples).reshape(3, samples) + 1 + 2j
    if trajectory:
        a.traj[:] = .25
    return a


def check(before, after, filtered=False):
    assert type(before) is type(after)
    if isinstance(before, ismrmrd.Acquisition):
        assert bytes(before.getHead()) == bytes(after.getHead())
        np.testing.assert_array_equal(before.traj, after.traj)
        expected = before.data.copy()
        if filtered:
            start, stop = before.discard_pre, before.number_of_samples - before.discard_post
            edge = (stop - start) // 20
            expected[:, start:start + edge] = 0
            expected[:, stop - edge:stop] = 0
        np.testing.assert_array_equal(after.data, expected)
    else:
        chunks_before, chunks_after = [], []
        before.serialize_into(chunks_before.append)
        after.serialize_into(chunks_after.append)
        assert b''.join(chunks_before) == b''.join(chunks_after)


def numerical(module):
    m = metadata()
    for samples in (19, 20, 99, 100, 101):
        for pre, post in ((0, 0), (7, 3)):
            a = acquisition(samples + pre + post, pre, post)
            edges = module.readout_edges(a, m)
            assert edges is not None
            before = deepcopy(a)
            a.data[:, edges.left] = 0
            a.data[:, edges.right] = 0
            check(before, a, True)
    for config in ([], {'kspacefilter': None}, {'kspacefilter': []}, {'kspacefilter': {'mode': 'wrong'}},
                   {'kspacefilter': {'measurement_role': None}},
                   {'kspacefilter': {'typo': 1}}):
        try:
            module.enabled(config)
        except ValueError:
            pass
        else:
            raise AssertionError(f'Malformed configuration accepted: {config}')
    a = acquisition()
    a.discard_pre = 101
    assert module.readout_edges(a, m) is None
    a = acquisition()
    a.resize(99, 3)
    a.getHead().number_of_samples = 100
    assert module.readout_edges(a, m) is None


def tcp(server):
    from connection import Connection
    with tempfile.TemporaryDirectory() as temporary:
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        with (Path(temporary) / 'server.log').open('w+') as log:
            child = subprocess.Popen([sys.executable, str(server / 'main.py'),
                                      '-H', '127.0.0.1', '-p', str(port), '-m'],
                                     stdout=log, stderr=subprocess.STDOUT,
                                     env=dict(os.environ, PYTHONUNBUFFERED='1'))
            try:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=1):
                            break
                    except OSError:
                        if child.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError('server did not start')
                        time.sleep(.05)

                def session(records, options=OPTIONS, filtered=True, fragmented=False, explicit=False, system_info=True, xml=None):
                    with socket.create_connection(('127.0.0.1', port), timeout=10) as sock:
                        client = Connection(sock, False)
                        client.send_config_file('openrecon')
                        header = metadata()
                        if not system_info:
                            header.acquisitionSystemInformation = None
                        client.send_metadata(ismrmrd.xsd.ToXML(header) if xml is None else xml)
                        if options is not None:
                            config = json.dumps({'parameters': {'config': 'kspacefilter'} if explicit else None, 'kspacefilter': options})
                            if fragmented:
                                from constants import MrdMessageIdentifier, MrdMessageLength, MRD_MESSAGE_TEXT
                                identifier = MrdMessageIdentifier.pack(MRD_MESSAGE_TEXT)
                                sock.sendall(identifier[:1])
                                time.sleep(.05)
                                sock.sendall(identifier[1:])
                                payload = (config + '\0').encode()
                                sock.sendall(MrdMessageLength.pack(len(payload)) + payload)
                            else:
                                client.send_text(config)
                        for record in records:
                            if isinstance(record, ismrmrd.Acquisition):
                                client.send_acquisition(record)
                            elif isinstance(record, ismrmrd.Image):
                                client.send_image(record)
                            else:
                                client.send_waveform(record)
                            check(record, next(client), filtered and isinstance(record, ismrmrd.Acquisition))
                        client.send_close()
                        assert next(client) is None

                session([acquisition(), acquisition(110, 7, 3)], fragmented=True)
                session([acquisition()], explicit=True)
                session([acquisition()], system_info=False)
                session([acquisition()], filtered=False, xml='<invalid>')
                for samples in (19, 20, 99, 101, 32768):
                    session([acquisition(samples)])
                for samples in (100, 101):
                    a = acquisition(samples)
                    a.set_flag(ismrmrd.ACQ_IS_REVERSE)
                    a.center_sample = samples - 1 - samples // 2
                    session([a])
                for options in (None, {'mode': 'passthrough'},
                                dict(OPTIONS, measurement_role='adjustment'),
                                dict(OPTIONS, measurement_role='unknown'),
                                dict(OPTIONS, input_domain='x-ky')):
                    session([acquisition()], options, False)
                for flag in (19, 20, 21, 23, 24, 26, 27, 28, 29, 30, 31, 57):
                    a = acquisition()
                    a.set_flag(flag)
                    session([a], filtered=False)
                for defect in ('trajectory', 'radial', 'encoding', 'center', 'discards'):
                    a = acquisition(trajectory=int(defect == 'trajectory'))
                    if defect == 'radial':
                        a.encoding_space_ref = 1
                    elif defect == 'encoding':
                        a.encoding_space_ref = 2
                    elif defect == 'center':
                        a.center_sample = 17
                    elif defect == 'discards':
                        a.discard_post = 101
                    session([a], filtered=False)
                image = ismrmrd.Image.from_array(np.ones((2, 2), np.float32), transpose=False)
                wave = ismrmrd.Waveform()
                wave.resize(3, 1)
                session([image, wave], filtered=False)
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                child.terminate()
                child.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server-dir', type=Path, default=Path('/opt/code/python-ismrmrd-server'))
    args = parser.parse_args()
    sys.path.insert(0, str(args.server_dir))
    import kspacefilter
    numerical(kspacefilter)
    tcp(args.server_dir)
    print('KSPACEFILTER numerical and MRD transport smoke tests passed')


if __name__ == '__main__':
    main()
