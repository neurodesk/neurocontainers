"""Configure the pinned MRD server for parameterless OpenRecon adjustment streams."""

from __future__ import annotations

import argparse
from pathlib import Path


def configure(server_dir: Path, app: str) -> None:
    if app not in ("frisgokspace",):
        raise ValueError("Unsupported FIRE POC application")
    main = server_dir / "main.py"
    server = server_dir / "server.py"
    connection = server_dir / "connection.py"
    connection_text = connection.read_text()
    if (connection_text.count("self.socket.send(") != 19
            or connection_text.count("serialize_into(self.socket.send)") != 3):
        raise ValueError("Pinned MRD transport changed; review full-write handling")
    original_read = "    def read(self, nbytes):\n        return self.socket.recv(nbytes, socket.MSG_WAITALL)"
    original_peek = "    def peek(self, nbytes):\n        return self.socket.recv(nbytes, socket.MSG_PEEK)"
    buffer_init = "        self.is_exhausted   = False"
    if (connection_text.count(original_read) != 1
            or connection_text.count(original_peek) != 1
            or connection_text.count(buffer_init) != 1):
        raise ValueError("Pinned MRD transport changed; review exact-read handling")
    connection_text = connection_text.replace(
        buffer_init, buffer_init + "\n        self._read_buffer = b''"
    )
    connection_text = connection_text.replace(original_read, """    def read(self, nbytes):
        buffered = self._read_buffer[:nbytes]
        self._read_buffer = self._read_buffer[nbytes:]
        chunks = [buffered]
        remaining = nbytes - len(buffered)
        while remaining:
            chunk = self.socket.recv(remaining)
            if not chunk:
                if remaining == nbytes:
                    return b''
                raise EOFError('Socket closed inside an MRD payload')
            chunks.append(chunk)
            remaining -= len(chunk)
        return b''.join(chunks)""")
    connection_text = connection_text.replace(original_peek, """    def peek(self, nbytes):
        data = self.read(nbytes)
        self._read_buffer = data + self._read_buffer
        return data""")
    main_text = main.read_text()
    server_text = server.read_text()
    placeholder = "'defaultConfig':  'default_replace_with_valid_name'"
    parameter_check = "if ('parameters' in configAdditional):"
    if main_text.count(placeholder) != 1 or server_text.count(parameter_check) != 1:
        raise ValueError(
            "Pinned MRD server changed; review adjustment dispatch before building"
        )
    main.write_text(main_text.replace(placeholder, f"'defaultConfig':  '{app}'"))
    server.write_text(
        server_text.replace(
            parameter_check,
            "if isinstance(configAdditional, dict) and "
            "isinstance(configAdditional.get('parameters'), dict):",
        )
    )

    connection.write_text(connection_text.replace("self.socket.send(", "self.socket.sendall(")
                          .replace("serialize_into(self.socket.send)",
                                   "serialize_into(self.socket.sendall)"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("server_dir", type=Path)
    parser.add_argument("app")
    args = parser.parse_args()
    configure(args.server_dir, args.app)
