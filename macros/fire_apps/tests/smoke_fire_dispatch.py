"""Replay the scanner's adjustment handshake against the packaged server entrypoint."""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import ismrmrd
import numpy as np

SERVER = Path("/opt/code/python-ismrmrd-server")
sys.path.insert(0, str(SERVER))
from connection import Connection  # noqa: E402
from smoke_fire_poc import acquisitions, metadata  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        env = dict(os.environ, FIRE_POC_CAPTURE_ROOT=str(root / "captures"))
        with (root / "server.log").open("w+") as log:
            # OpenRecon packaging starts main.py directly, without launcher -d.
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(SERVER / "main.py"),
                    "-H",
                    "127.0.0.1",
                    "-p",
                    str(port),
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 15
                while True:
                    try:
                        client_socket = socket.create_connection(
                            ("127.0.0.1", port), timeout=5
                        )
                        break
                    except OSError:
                        if process.poll() is not None or time.monotonic() > deadline:
                            raise RuntimeError("Server failed to start")
                        time.sleep(0.05)
                client_socket.close()
                configurations = [
                    {"version": "1.1.0", "parameters": None},
                    {
                        "version": "1.1.0",
                        "parameters": {"config": args.app, "mode": "capture"},
                    },
                ]
                for config in configurations:
                    with socket.create_connection(
                        ("127.0.0.1", port), timeout=5
                    ) as sock:
                        client = Connection(sock, False)
                        client.send_config_file("openrecon")
                        client.send_metadata(ismrmrd.xsd.ToXML(metadata()))
                        client.send_text(json.dumps(config))
                        samples = np.ones((2, 12, 12), np.complex64) * (1 + 2j)
                        for acquisition in acquisitions(
                            samples, flag=ismrmrd.ACQ_IS_PARALLEL_CALIBRATION
                        ):
                            client.send_acquisition(acquisition)
                        client.send_close()
                        responses = list(client)
                        images = [x for x in responses if isinstance(x, ismrmrd.Image)]
                        if args.app == "acsrss":
                            assert len(images) == 1, responses
                            expected = np.zeros((12, 12), np.float32)
                            expected[6, 6] = 12 * np.sqrt(10)
                            np.testing.assert_allclose(
                                images[0].data[0, 0], expected, atol=1e-5
                            )
                            assert images[0].image_series_index == 60000
                summaries = list((root / "captures").glob("*/summary.json"))
                assert len(summaries) == (0 if args.app == "acsrss" else 2), (
                    f"Expected two captured sessions, got {len(summaries)}"
                )
                for path in summaries:
                    summary = json.loads(path.read_text())
                    assert summary["application"] == args.app
                    assert summary["status"] == "captured", summary
                    assert summary["counts"]["Acquisition"] == 12, summary
                    saved = ismrmrd.Dataset(
                        str(path.with_name("input.h5")), create_if_needed=False
                    )
                    try:
                        np.testing.assert_array_equal(
                            saved.read_acquisition(0).data, acquisition.data
                        )
                    finally:
                        saved.close()
                if args.app == "acsrss":
                    assert not (root / "captures").exists()
                log.flush()
                log.seek(0)
                assert "Failed to parse as JSON" not in log.read()
            except Exception:
                log.flush()
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                process.terminate()
                process.wait(timeout=10)
    print("FIRE adjustment dispatch smoke tests passed")


if __name__ == "__main__":
    main()
