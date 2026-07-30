#!/usr/bin/env python3

import argparse
from pathlib import Path
import socket
import subprocess
import sys
import tempfile


SCRIPT_DIR = Path(__file__).resolve().parent
PYTHON_ISMRMRD_SERVER_DIR = Path("/opt/code/python-ismrmrd-server")
for candidate in (SCRIPT_DIR, PYTHON_ISMRMRD_SERVER_DIR):
    if candidate.exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

import connection


class FileSocket:
    def __init__(self, path: Path):
        self._file = path.open("rb")

    def recv(self, nbytes: int, flags: int = 0) -> bytes:
        if flags == socket.MSG_PEEK:
            position = self._file.tell()
            data = self._file.read(nbytes)
            self._file.seek(position)
            return data
        return self._file.read(nbytes)

    def shutdown(self, how):
        del how

    def close(self):
        self._file.close()


def _stream_to_hdf5(stream_path: Path, output_path: Path, group_name: str):
    file_socket = FileSocket(stream_path)
    conn = connection.Connection(
        file_socket,
        savedata=True,
        savedataFile=str(output_path),
        savedataFolder="",
        savedataGroup=group_name,
    )

    try:
        for _ in conn:
            pass
    finally:
        if conn.dset is not None:
            conn.dset.close()
        file_socket.close()


def convert_twix_to_mrd(input_file: Path, output_file: Path, group_name: str, attach_trajectory: bool):
    with tempfile.TemporaryDirectory(prefix="siemens_twix2mrd_") as temp_dir:
        stream_path = Path(temp_dir) / "converter.stream"
        command = [
            "siemens_to_ismrmrd",
            "--skipSyncData",
            "-f",
            str(input_file),
            "-o",
            str(stream_path),
        ]
        if attach_trajectory:
            command.insert(1, "--attachTrajectory")

        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "siemens_to_ismrmrd failed with exit code "
                f"{result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        if not stream_path.exists() or stream_path.stat().st_size == 0:
            raise RuntimeError(
                "siemens_to_ismrmrd did not produce a stream file.\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )

        _stream_to_hdf5(stream_path, output_file, group_name)

        print(result.stderr.strip())
        print(
            "Converted Siemens Twix data to MRD via siemens_to_ismrmrd stream:",
            output_file,
        )


def main():
    parser = argparse.ArgumentParser(
        description="Convert Siemens Twix data to ISMRMRD HDF5 using siemens_to_ismrmrd."
    )
    parser.add_argument("-f", "--file", required=True, help="Input Siemens .dat file")
    parser.add_argument("-o", "--output", required=True, help="Output ISMRMRD HDF5 file")
    parser.add_argument("-g", "--group", default="dataset", help="ISMRMRD group name")
    parser.add_argument(
        "--attach-trajectory",
        action="store_true",
        help="Request trajectory attachment from siemens_to_ismrmrd before materializing the stream.",
    )
    args = parser.parse_args()

    convert_twix_to_mrd(
        input_file=Path(args.file),
        output_file=Path(args.output),
        group_name=args.group,
        attach_trajectory=bool(args.attach_trajectory),
    )


if __name__ == "__main__":
    main()
