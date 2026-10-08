from __future__ import annotations

from dataclasses import dataclass

import ismrmrd


@dataclass(frozen=True)
class ReadoutEdges:
    left: slice
    right: slice


_ALLOWED_FLAGS = sum(1 << (getattr(ismrmrd, name) - 1) for name in dir(ismrmrd)
                     if name.startswith(('ACQ_FIRST_', 'ACQ_LAST_')))
_ALLOWED_FLAGS |= 1 << (ismrmrd.ACQ_IS_REVERSE - 1)


def enabled(config: object) -> bool:
    if config is None or isinstance(config, str):
        return False
    if not isinstance(config, dict):
        raise ValueError('Configuration must be an object')
    if 'kspacefilter' not in config:
        return False
    options = config['kspacefilter']
    if not isinstance(options, dict):
        raise ValueError('kspacefilter configuration must be an object')
    if set(options) - {'mode', 'input_domain', 'measurement_role'}:
        raise ValueError('Unknown kspacefilter option')
    mode = options.get('mode', 'passthrough')
    if mode not in ('passthrough', 'readout-edge-zero'):
        raise ValueError('Unsupported kspacefilter mode')
    for key in ('input_domain', 'measurement_role'):
        if key in options and not isinstance(options[key], str):
            raise ValueError(f'{key} must be a string')
    return (mode == 'readout-edge-zero'
            and options.get('input_domain') == 'raw-uniform-cartesian-kspace'
            and options.get('measurement_role') == 'imaging')


def readout_edges(acquisition: ismrmrd.Acquisition, metadata: object) -> ReadoutEdges | None:
    if acquisition.flags & ~_ALLOWED_FLAGS:
        return None
    if acquisition.trajectory_dimensions or acquisition.traj.size:
        return None
    encodings = getattr(metadata, 'encoding', ())
    if acquisition.encoding_space_ref >= len(encodings):
        return None
    if encodings[acquisition.encoding_space_ref].trajectory != ismrmrd.xsd.trajectoryType.CARTESIAN:
        return None
    samples = acquisition.number_of_samples
    if acquisition.data.shape != (acquisition.active_channels, samples) or not acquisition.active_channels:
        return None
    start, stop = acquisition.discard_pre, samples - acquisition.discard_post
    width = stop - start
    if start < 0 or stop > samples or width <= 0:
        return None
    centers = {width // 2}
    if acquisition.isFlagSet(ismrmrd.ACQ_IS_REVERSE):
        centers.add(width - 1 - width // 2)
    if acquisition.center_sample - start not in centers:
        return None
    edge = width // 20
    return ReadoutEdges(slice(start, start + edge), slice(stop - edge, stop))


def process(connection: object, config: object, metadata: object) -> None:
    try:
        apply_mask = enabled(config)
        if apply_mask and isinstance(metadata, (str, bytes)):
            try:
                metadata = ismrmrd.xsd.CreateFromDocument(metadata)
            except Exception:
                metadata = None
        for message in connection:
            if message is None:
                break
            if isinstance(message, ismrmrd.Acquisition):
                edges = readout_edges(message, metadata) if apply_mask else None
                if edges is not None:
                    message.data[:, edges.left] = 0
                    message.data[:, edges.right] = 0
                connection.send_acquisition(message)
            elif isinstance(message, ismrmrd.Image):
                connection.send_image(message)
            elif isinstance(message, ismrmrd.Waveform):
                connection.send_waveform(message)
            elif isinstance(message, str):
                connection.send_text(message)
            else:
                raise TypeError(f'Unsupported MRD record: {type(message).__name__}')
    finally:
        connection.send_close()
