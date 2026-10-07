"""Exercise the numerical contract and the actual installed MRD TCP dispatcher."""
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
from concurrent.futures import ThreadPoolExecutor
import time

import ismrmrd
import numpy as np


def metadata() -> str:
    x = ismrmrd.xsd
    space = x.encodingSpaceType(matrixSize=x.matrixSizeType(x=8, y=8, z=1),
                               fieldOfView_mm=x.fieldOfViewMm(x=80, y=80, z=4))
    return x.ToXML(x.ismrmrdHeader(
        experimentalConditions=x.experimentalConditionsType(H1resonanceFrequency_Hz=123000000),
        encoding=[x.encodingType(encodedSpace=space, reconSpace=space,
                                 trajectory=x.trajectoryType.CARTESIAN)]))


AUX_FLAGS = (ismrmrd.ACQ_IS_NOISE_MEASUREMENT, ismrmrd.ACQ_IS_PARALLEL_CALIBRATION,
             ismrmrd.ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING,
             ismrmrd.ACQ_IS_NAVIGATION_DATA, ismrmrd.ACQ_IS_PHASECORR_DATA)


def fixtures(offset: complex = 0, samples: int = 8) -> list:
    records = []
    preamble = ismrmrd.Acquisition()
    preamble.resize(8, 2)
    preamble.data[:] = 13 - 2j
    preamble.set_flag(ismrmrd.ACQ_IS_NOISE_MEASUREMENT)
    records.append(preamble)
    for repetition in range(5):
        for line, ky in enumerate((1, 5)):
            a = ismrmrd.Acquisition()
            a.resize(samples, 2, 1)
            a.measurement_uid = 19
            a.available_channels = 2
            a.channel_mask[0] = 3
            a.idx.repetition = repetition
            a.idx.kspace_encode_step_1 = ky
            a.idx.user[5] = 71
            a.scan_counter = repetition * 2 + line
            a.acquisition_time_stamp = repetition * 100 + line
            a.sample_time_us = 2.5
            a.center_sample = samples // 2
            a.user_int[7] = 0x123456
            a.user_float[4] = .375
            a.position = (0, 0, 4)
            a.read_dir, a.phase_dir, a.slice_dir = (1, 0, 0), (0, 1, 0), (0, 0, 1)
            a.traj[:] = np.linspace(-.5, .5, samples)[:, None]
            signal = offset + (2 + 3j) + repetition * (.25 + .5j) + ky * (.1 + .2j)
            a.data[:] = signal + (-1) ** repetition * (1 - 2j)
            a.data[1] *= 2j
            if repetition % 2:
                a.set_flag(ismrmrd.ACQ_IS_REVERSE)
            if line == 0:
                a.set_flag(ismrmrd.ACQ_FIRST_IN_REPETITION)
            if line == 1:
                a.set_flag(ismrmrd.ACQ_LAST_IN_REPETITION)
            records.append(a)
            if line == 0:
                for flag in AUX_FLAGS:
                    if flag == ismrmrd.ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING:
                        continue
                    auxiliary = deepcopy(a)
                    auxiliary.set_flag(flag)
                    auxiliary.clear_flag(ismrmrd.ACQ_FIRST_IN_REPETITION)
                    auxiliary.data[:] = 99 + 7j
                    records.append(auxiliary)
                wave = ismrmrd.Waveform()
                wave.resize(3, 1)
                wave.data[:] = np.array([[7, 11, 19]], dtype=np.uint32)
                wave.time_stamp = repetition
                records.append(wave)
                image = ismrmrd.Image.from_array(np.arange(4, dtype=np.float32).reshape(2, 2), transpose=False)
                image.image_index = repetition
                image.attribute_string = '<ismrmrdMeta><meta><name>test</name><value>kept</value></meta></ismrmrdMeta>'
                records.append(image)
    return records


def serialized(record: object) -> bytes:
    chunks = []
    record.serialize_into(chunks.append)
    return b''.join(chunks)


def numerical(module: object) -> None:
    steady = np.array([[2 + 3j, -1 + 2j], [4 - 1j, 2j]], np.complex64)
    artifact = np.array([[1 - 2j, 3j], [-2 + 1j, 4]], np.complex64)
    slope = steady * np.complex64(.2 - .1j)
    inputs = [steady - slope + artifact, steady - artifact, steady + slope + artifact]
    saved = [value.copy() for value in inputs]
    np.testing.assert_allclose(module.centered(*inputs), steady, atol=1e-6)
    for actual, original in zip(inputs, saved):
        np.testing.assert_array_equal(actual, original)
    np.testing.assert_allclose(module.centered(steady, steady, steady), steady)
    omega = .7
    np.testing.assert_allclose(module.centered(steady * np.exp(-1j * omega), steady,
                                             steady * np.exp(1j * omega)),
                               steady * np.cos(omega / 2) ** 2, rtol=1e-6)
    phi = .4
    np.testing.assert_allclose(module.centered(steady * np.exp(1j * phi),
                                             steady * np.exp(-1j * phi),
                                             steady * np.exp(1j * phi)),
                               steady * np.cos(phi), rtol=1e-6)
    assert np.linalg.norm(steady * np.cos(phi)) < np.linalg.norm(steady)


OPTIONS = {'mode': 'centered-complex', 'input_domain': 'aligned-cartesian-kspace',
           'coil_basis': 'fixed-physical-channels', 'polarity': 'per-line-reverse-flag',
           'measurement_role': 'imaging'}


def verify(original: list, received: list, correction: bool) -> None:
    received = [record for record in received if record is not None]
    assert len(original) == len(received), (len(original), len(received))
    for before, after in zip(original, received):
        assert type(before) is type(after)
        if isinstance(before, ismrmrd.Acquisition):
            assert bytes(before.getHead()) == bytes(after.getHead())
            np.testing.assert_array_equal(before.traj, after.traj)
            rep = before.idx.repetition
            if correction and rep in (1, 2, 3) and not any(before.isFlagSet(flag) for flag in AUX_FLAGS):
                expected = before.data.copy()
                expected[0] -= (-1) ** rep * (1 - 2j)
                expected[1] -= (-1) ** rep * (1 - 2j) * 2j
                np.testing.assert_allclose(after.data, expected, atol=2e-6)
            else:
                assert serialized(before) == serialized(after)
        else:
            assert serialized(before) == serialized(after)


def tcp(server: Path) -> None:
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

                def connect(options: dict | None, explicit_dispatch: bool = False,
                            fragmented: bool = False):
                    sock = socket.create_connection(('127.0.0.1', port), timeout=20)
                    client = Connection(sock, False)
                    client.send_config_file('openrecon')
                    client.send_metadata(metadata())
                    if options is not None:
                        configuration = json.dumps({'parameters': {'config': 'frisgokspace'} if explicit_dispatch else None,
                                                    'frisgokspace': options})
                        if fragmented:
                            from constants import MrdMessageIdentifier, MrdMessageLength, MRD_MESSAGE_TEXT
                            identifier = MrdMessageIdentifier.pack(MRD_MESSAGE_TEXT)
                            payload = (configuration + '\0').encode()
                            sock.sendall(identifier[:1])
                            time.sleep(.1)
                            sock.sendall(identifier[1:])
                            sock.sendall(MrdMessageLength.pack(len(payload)))
                            sock.sendall(payload)
                        else:
                            client.send_text(configuration)
                    return sock, client

                def session(records: list, options: dict | None, correct: bool = False,
                            eof: bool = False, explicit_dispatch: bool = False, threaded: bool = False,
                            fragmented: bool = False):
                    sock, client = connect(options, explicit_dispatch, fragmented)
                    def send():
                        for record in records:
                            if isinstance(record, ismrmrd.Acquisition):
                                client.send_acquisition(record)
                            elif isinstance(record, ismrmrd.Image):
                                client.send_image(record)
                            else:
                                client.send_waveform(record)
                        if eof:
                            sock.shutdown(socket.SHUT_WR)
                        else:
                            client.send_close()
                    try:
                        if threaded:
                            with ThreadPoolExecutor(max_workers=1) as pool:
                                sent = pool.submit(send)
                                receiver = Connection(sock, False)
                                verify(records, list(receiver), correct)
                                sent.result()
                        else:
                            send()
                            verify(records, list(client), correct)
                    finally:
                        sock.close()

                session(fixtures(), None)
                session(fixtures(), {'mode': 'passthrough'})
                session(fixtures(), OPTIONS, True)
                session(fixtures(), OPTIONS, True, explicit_dispatch=True)
                session(fixtures(), OPTIONS, True, fragmented=True)
                session(fixtures(), dict(OPTIONS, measurement_role='adjustment'))
                session(fixtures(), dict(OPTIONS, max_buffer_records=3))
                session(fixtures(), dict(OPTIONS, max_buffer_mib=.001))
                session(fixtures()[:4], OPTIONS, eof=True)
                for flag in (ismrmrd.ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING,
                             ismrmrd.ACQ_USER1):
                    records = fixtures()
                    for record in records:
                        if (isinstance(record, ismrmrd.Acquisition)
                                and record.isFlagSet(ismrmrd.ACQ_IS_NAVIGATION_DATA)):
                            record.clear_flag(ismrmrd.ACQ_IS_NAVIGATION_DATA)
                            record.set_flag(flag)
                    session(records, OPTIONS)
                for defect in ('dwell', 'mask', 'polarity', 'geometry', 'nonfinite', 'time', 'channels', 'trajectory'):
                    records = fixtures()
                    for record in records:
                        if (isinstance(record, ismrmrd.Acquisition)
                                and not any(record.isFlagSet(flag) for flag in AUX_FLAGS)
                                and record.idx.kspace_encode_step_1 == 5):
                            if defect == 'dwell':
                                record.sample_time_us += record.idx.repetition
                            elif defect == 'mask':
                                record.idx.user[5] += record.idx.repetition
                            elif defect == 'polarity':
                                record.clear_flag(ismrmrd.ACQ_IS_REVERSE)
                            elif defect == 'geometry':
                                record.position = (0, 0, record.idx.repetition)
                            elif defect == 'time':
                                record.acquisition_time_stamp += record.idx.repetition ** 2
                            elif defect == 'channels':
                                record.channel_mask[0] = 3 + record.idx.repetition
                            elif defect == 'trajectory':
                                record.traj[-1] += record.idx.repetition
                            else:
                                record.data[0, -1] = np.nan
                    session(records, OPTIONS)
                for defect in ('missing-first', 'missing-last', 'repeated-counter', 'duplicate-key'):
                    records = fixtures()
                    for record in records:
                        if isinstance(record, ismrmrd.Acquisition) and not any(record.isFlagSet(flag) for flag in AUX_FLAGS):
                            if defect == 'missing-first':
                                record.clear_flag(ismrmrd.ACQ_FIRST_IN_REPETITION)
                            elif defect == 'missing-last':
                                record.clear_flag(ismrmrd.ACQ_LAST_IN_REPETITION)
                            elif defect == 'repeated-counter':
                                record.idx.repetition = 0
                            else:
                                record.idx.kspace_encode_step_1 = 1
                    session(records, OPTIONS)
                pairs = [connect(OPTIONS), connect(OPTIONS)]
                streams = [fixtures(17), fixtures(41)]
                try:
                    for records, (_, client) in zip(streams, pairs):
                        for record in records:
                            if isinstance(record, ismrmrd.Acquisition):
                                client.send_acquisition(record)
                            elif isinstance(record, ismrmrd.Image):
                                client.send_image(record)
                            else:
                                client.send_waveform(record)
                        client.send_close()
                    for records, (_, client) in zip(streams, pairs):
                        verify(records, list(client), True)
                finally:
                    for sock, _ in pairs:
                        sock.close()
                session(fixtures(samples=32768)[1:2], None, threaded=True)
                log.flush()
                log.seek(0)
                output = log.read()
                assert 'frisgokspace session failed' not in output, output
                assert 'Failed to parse as JSON' not in output, output
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                child.terminate()
                child.wait(timeout=10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server-dir', type=Path, default=Path('/opt/code/python-ismrmrd-server'))
    parser.add_argument('--numerical-only', action='store_true')
    args = parser.parse_args()
    sys.path.insert(0, str(args.server_dir))
    import frisgokspace
    numerical(frisgokspace)
    if not args.numerical_only:
        tcp(args.server_dir)
    print('FRISGO numerical and MRD transport smoke tests passed')


if __name__ == '__main__':
    main()
