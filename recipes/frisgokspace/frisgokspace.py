from __future__ import annotations

from dataclasses import dataclass, field
import logging

import ismrmrd
import numpy as np


@dataclass
class Record:
    message: object
    size: int
    header: bytes = b""
    samples: np.ndarray | None = None


@dataclass
class Frame:
    records: list[Record] = field(default_factory=list)
    repetition: int | None = None
    size: int = 0
    emitted: bool = False


_EXCLUDED = (
    "IS_NOISE_MEASUREMENT", "IS_PARALLEL_CALIBRATION",
    "IS_PARALLEL_CALIBRATION_AND_IMAGING", "IS_PHASECORR_DATA",
    "IS_NAVIGATION_DATA", "IS_DUMMYSCAN_DATA", "IS_RTFEEDBACK_DATA",
    "IS_HPFEEDBACK_DATA", "IS_SURFACECOILCORRECTIONSCAN_DATA",
    "IS_PHASE_STABILIZATION_REFERENCE", "IS_PHASE_STABILIZATION",
)
_ALLOWED_FLAGS = sum(1 << (getattr(ismrmrd, name) - 1) for name in dir(ismrmrd)
                     if name.startswith("ACQ_FIRST_") or name.startswith("ACQ_LAST_"))
_ALLOWED_FLAGS |= 1 << (ismrmrd.ACQ_IS_REVERSE - 1)


def centered(before: np.ndarray, center: np.ndarray, after: np.ndarray) -> np.ndarray:
    """Center the [1, 2, 1]/4 filter without changing its input arrays."""
    return (before * np.float32(.25) + center * np.float32(.5)
            + after * np.float32(.25)).astype(np.complex64)


def _key(acquisition: ismrmrd.Acquisition) -> tuple:
    idx = acquisition.idx
    return (acquisition.measurement_uid, acquisition.encoding_space_ref,
            idx.average, idx.slice, idx.contrast, idx.phase, idx.set, idx.segment,
            idx.kspace_encode_step_1, idx.kspace_encode_step_2, tuple(idx.user))


def _eligible(acquisition: ismrmrd.Acquisition) -> bool:
    return (not any(acquisition.isFlagSet(getattr(ismrmrd, 'ACQ_' + name))
                    for name in _EXCLUDED)
            and not acquisition.flags & ~_ALLOWED_FLAGS)


def _signature(acquisition: ismrmrd.Acquisition) -> tuple:
    return (acquisition.number_of_samples, acquisition.active_channels,
            acquisition.available_channels, tuple(acquisition.channel_mask),
            acquisition.sample_time_us, acquisition.discard_pre,
            acquisition.discard_post, acquisition.center_sample,
            acquisition.trajectory_dimensions, acquisition.traj.tobytes(),
            tuple(acquisition.position), tuple(acquisition.read_dir),
            tuple(acquisition.phase_dir), tuple(acquisition.slice_dir),
            tuple(acquisition.patient_table_position))


def _slots(frame: Frame) -> dict[tuple, Record]:
    slots = {}
    for record in frame.records:
        acq = record.message
        if isinstance(acq, ismrmrd.Acquisition):
            if acq.isFlagSet(ismrmrd.ACQ_IS_PARALLEL_CALIBRATION_AND_IMAGING):
                raise ValueError('integrated calibration imaging requires unchanged frame')
            if (not any(acq.isFlagSet(getattr(ismrmrd, 'ACQ_' + name))
                        for name in _EXCLUDED)
                    and acq.flags & ~_ALLOWED_FLAGS):
                raise ValueError('unknown imaging flags require unchanged frame')
        if isinstance(acq, ismrmrd.Acquisition) and _eligible(acq):
            key = _key(acq)
            if key in slots:
                raise ValueError('duplicate acquisition key')
            slots[key] = record
    return slots


def _correct(before: Frame, center: Frame, after: Frame) -> bool:
    try:
        if not (before.repetition + 1 == center.repetition
                and center.repetition + 1 == after.repetition):
            raise ValueError('nonconsecutive repetition counters')
        maps = [_slots(frame) for frame in (before, center, after)]
        if not maps[1] or not maps[0].keys() == maps[1].keys() == maps[2].keys():
            raise ValueError('different acquired sampling masks')
        output = []
        for key, middle in maps[1].items():
            records = [mapping[key] for mapping in maps]
            acqs = [record.message for record in records]
            if not _signature(acqs[0]) == _signature(acqs[1]) == _signature(acqs[2]):
                raise ValueError('inconsistent geometry, channels or sampling')
            if any(acq.sample_time_us <= 0 or not np.isfinite(acq.sample_time_us)
                   or not acq.number_of_samples or not acq.active_channels
                   or not np.isfinite(acq.traj).all() for acq in acqs):
                raise ValueError('invalid sampling metadata')
            if any(not np.isfinite(record.samples).all() for record in records):
                raise ValueError('nonfinite samples')
            polarity = [acq.isFlagSet(ismrmrd.ACQ_IS_REVERSE) for acq in acqs]
            if polarity[0] == polarity[1] or polarity[1] == polarity[2]:
                raise ValueError('readout polarity did not alternate')
            times = [acq.acquisition_time_stamp for acq in acqs]
            if not 0 < times[1] - times[0] == times[2] - times[1]:
                raise ValueError('nonuniform acquisition time gaps')
            output.append((middle, centered(*(record.samples for record in records))))
        for record, samples in output:
            record.message.data[:] = samples
            if bytes(record.message.getHead()) != record.header:
                raise RuntimeError('sample substitution changed acquisition header')
        return True
    except ValueError as error:
        logging.warning('frisgokspace bypassed center frame: %s', error)
        return False


def _policy(config: object) -> tuple[bool, int, int]:
    options = config.get('frisgokspace', {}) if isinstance(config, dict) else {}
    if not isinstance(options, dict):
        raise ValueError('frisgokspace configuration must be an object')
    known = {'mode', 'input_domain', 'coil_basis', 'polarity', 'measurement_role',
             'max_buffer_mib', 'max_buffer_records'}
    if options.keys() - known:
        raise ValueError('unknown frisgokspace configuration fields')
    mode = options.get('mode', 'passthrough')
    if mode not in ('passthrough', 'centered-complex'):
        raise ValueError('unsupported frisgokspace mode')
    byte_cap = options.get('max_buffer_mib', 512)
    record_cap = options.get('max_buffer_records', 65536)
    if (isinstance(byte_cap, bool) or not isinstance(byte_cap, (int, float))
            or not np.isfinite(byte_cap) or byte_cap <= 0
            or isinstance(record_cap, bool) or not isinstance(record_cap, int)
            or record_cap <= 0):
        raise ValueError('buffer limits must be positive finite numbers')
    if mode == 'centered-complex':
        if (options.get('input_domain') not in ('aligned-cartesian-kspace', 'aligned-x-ky')
                or options.get('coil_basis') != 'fixed-physical-channels'
                or options.get('polarity') != 'per-line-reverse-flag'):
            raise ValueError('correction requires aligned domain, fixed coils and verified polarity')
        if options.get('measurement_role') != 'imaging':
            logging.warning('frisgokspace bypass: adapter did not classify a main imaging measurement')
            mode = 'passthrough'
    return mode == 'centered-complex', int(byte_cap * 1024 * 1024), record_cap


def _snapshot(message: object, retain: bool = True) -> Record:
    if isinstance(message, ismrmrd.Acquisition):
        samples = message.data.copy() if retain and _eligible(message) else None
        return Record(message, 4096 + message.data.nbytes + message.traj.nbytes
                      + (3 * samples.nbytes if samples is not None else 0),
                      bytes(message.getHead()), samples)
    if isinstance(message, ismrmrd.Image):
        return Record(message, 4096 + message.data.nbytes
                      + len(message.attribute_string.encode('utf-8')))
    if isinstance(message, ismrmrd.Waveform):
        return Record(message, 4096 + message.data.nbytes)
    if isinstance(message, str):
        return Record(message, 4096 + len(message.encode('utf-8')))
    raise TypeError(f'Unsupported received MRD record: {type(message).__name__}')


def _send(connection: object, record: Record) -> None:
    message = record.message
    if isinstance(message, ismrmrd.Acquisition):
        if bytes(message.getHead()) != record.header:
            raise RuntimeError('acquisition header changed')
        connection.send_acquisition(message)
    elif isinstance(message, ismrmrd.Image):
        connection.send_image(message)
    elif isinstance(message, ismrmrd.Waveform):
        connection.send_waveform(message)
    else:
        connection.send_text(message)


def process(connection: object, config: object, metadata: object) -> None:
    """Own all buffering in one connection; default to an unchanged ordered stream."""
    enabled, byte_cap, record_cap = _policy(config)
    frames: list[Frame] = []
    current = Frame()
    last_repetition = None
    corrected = 0

    def emit(frame: Frame) -> None:
        if not frame.emitted:
            for record in frame.records:
                _send(connection, record)
            frame.emitted = True

    def flush() -> None:
        for frame in frames:
            emit(frame)
        emit(current)

    try:
        for message in connection:
            if message is None:
                break
            record = _snapshot(message, enabled)
            if not enabled:
                _send(connection, record)
                continue
            current.records.append(record)
            current.size += record.size
            retained_frames = frames + [current]
            if (sum(len(frame.records) for frame in retained_frames) > record_cap
                    or sum(frame.size for frame in retained_frames) > byte_cap):
                logging.warning('frisgokspace buffer cap reached; flushing and latching passthrough')
                flush()
                frames.clear()
                current = Frame()
                enabled = False
                continue
            if not isinstance(message, ismrmrd.Acquisition) or not _eligible(message):
                continue
            first = message.isFlagSet(ismrmrd.ACQ_FIRST_IN_REPETITION)
            last = message.isFlagSet(ismrmrd.ACQ_LAST_IN_REPETITION)
            repetition = message.idx.repetition
            valid = True
            if current.repetition is None:
                valid = first and (last_repetition is None or repetition == last_repetition + 1)
                current.repetition = repetition
            elif first or repetition != current.repetition:
                valid = False
            if not valid:
                logging.warning('frisgokspace ambiguous repetition boundary; latching passthrough')
                flush()
                frames.clear()
                current = Frame()
                enabled = False
                continue
            if last:
                last_repetition = repetition
                frames.append(current)
                current = Frame()
                if len(frames) == 3:
                    corrected += int(_correct(*frames))
                    emit(frames[0])
                    emit(frames[1])
                    frames.pop(0)
        flush()
        logging.info('frisgokspace corrected %d interior frames; EOF flushed unchanged boundaries', corrected)
        connection.send_close()
    except Exception:
        logging.exception('frisgokspace session failed')
        raise
