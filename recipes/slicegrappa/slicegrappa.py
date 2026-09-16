"""Capture single-band references and SMS data for slice-GRAPPA experiments."""

from fire_poc import process as process_session


def process(connection, config, metadata):
    process_session(connection, config, metadata, "slicegrappa")
