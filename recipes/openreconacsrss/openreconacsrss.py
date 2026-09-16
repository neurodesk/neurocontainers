"""Capture ACS data and reconstruct Fourier/RSS preview images."""

from fire_poc import process as process_session


def process(connection, config, metadata):
    process_session(connection, config, metadata, "openreconacsrss")
