#!/usr/bin/env python
#
# Copyright 2010-2011 University of Chicago
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import platform
import sys
from socket import socket, AF_UNIX, SOCK_STREAM
from select import select
from signal import signal, SIGINT, SIGKILL
from time import sleep
import re
import getpass


PY2 = sys.version_info < (3,)
PY3 = not PY2


def _get_linux_distro_name():
    """
    Because `platform.linux_distribution` is deprecated and will be removed in
    python3.8+ , we do not rely on it. Instead, we will parse the contents of
    `/etc/os-release` to get linux distro information
    If something goes wrong, use dummy version info -- never error
    """
    dist_id = "unknown"
    dist_version = "0"
    try:
        with open("/etc/os-release") as f:
            for line in f:
                line = line.strip()
                if line.startswith("ID="):
                    dist_id = line.split("=")[1].strip('"')
                if line.startswith("VERSION_ID="):
                    dist_version = line.split("=")[1].strip('"')
    except Exception:
        pass
    return dist_id + "-" + dist_version


class _BinaryStdioWrapper(object):
    """
    A wrapper for writing to stdout, stderr which
    - knows about PY2 vs PY3
    - takes binary data and writes it out
    - handles errors
    """
    def _write(self, stream, data):
        if PY2:
            stream.write(data)
        else:
            stream.buffer.write(data)

    def _write_with_handler(self, stream, data, flush):
        try:
            self._write(stream, data)
            if flush:
                stream.flush()
        except Exception:
            # if this fails, it's a hard error
            self._write(sys.stderr, b"ERROR: failed to write data to stdio")

    def out(self, data, flush=True):
        self._write_with_handler(sys.stdout, data, flush)

    def err(self, data, flush=True):
        self._write_with_handler(sys.stderr, data, flush)


STDIO = _BinaryStdioWrapper()


GCP_VERSION = "3.2.8"
if len(sys.argv) > 1 and sys.argv[1] == "-version":
    print(GCP_VERSION)
    sys.exit(0)

arch = platform.architecture()[0]
dist_string = _get_linux_distro_name()
if arch != "":
    dist_string += "-" + arch

LINUX_VER = dist_string

try:
    GLOBUS_LOCATION = os.environ['GLOBUS_LOCATION']
except KeyError:
    raise Exception("Need GLOBUS_LOCATION defined")
EXE_DIR = os.path.dirname(sys.argv[0])

os.environ['LD_LIBRARY_PATH'] = "%s/lib:%s" % (
        GLOBUS_LOCATION, os.getenv('LD_LIBRARY_PATH', ""))

os.environ['GCP_OS'] = "linux"
os.environ['GCP_OS_VERSION'] = LINUX_VER
os.environ['GCP_APP_VERSION'] = GCP_VERSION
os.environ['GCP_PROTOCOL_VERSION'] = "3"
# getuser() checks environment variables in this order:
# LOGNAME, USER, LNAME, USERNAME
# it then failsover to using pwd
os.environ['GCP_USER'] = getpass.getuser()
os.environ['GCP_GLOBAL_ETC_DIR'] = os.path.join(EXE_DIR, "etc")
os.environ['GCP_SSH_PATH'] = os.path.join(GLOBUS_LOCATION, "bin", "ssh")
os.environ['GCP_PDEATH_PATH'] = os.path.join(GLOBUS_LOCATION, "bin", "pdeath")
os.environ['GCP_RELAYTOOL_PATH'] = os.path.join(GLOBUS_LOCATION, "bin", "relaytool")
os.environ['GCP_REGISTER_PATH'] = os.path.join(GLOBUS_LOCATION, "bin", "register")
os.environ['GCP_GRIDFTP_PATH'] = os.path.join(GLOBUS_LOCATION, "sbin", "globus-gridftp-server")


GC_CONTROL_ADDRESS = ""


def status_to_rc(status):
    if os.WIFSIGNALED(status):
        return -os.WTERMSIG(status)
    elif os.WIFEXITED(status):
        return os.WEXITSTATUS(status)
    else:
        assert 0


def ctrlc(sig, frame):
    sys.exit(1)


def send2clients(fds, data):
    for i in range(len(fds)):
        try:
            fds[i].send(data)
        except Exception:
            fds.pop(i)

def start(debug):
    restart = True
    while restart:
        restart = False
        s = socket(AF_UNIX, SOCK_STREAM, 0)
        try:
            try:
                os.unlink(GC_CONTROL_ADDRESS)
            except OSError:
                pass
            s.bind(GC_CONTROL_ADDRESS)
        except Exception as e:
            if 'Address already in use' in str(e):
                STDIO.err(b"Another Globus Connect Personal is currently running\n")
                sys.exit(1)
            else:
                raise
        s.listen(5)

        piread, piwrite = os.pipe()
        pread, pwrite = os.pipe()
        peread, pewrite = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(piwrite)
            os.close(pread)
            os.close(peread)
            os.dup2(piread, 0)
            os.dup2(pwrite, 1)
            os.dup2(pewrite, 2)
            os.execl(sys.executable, sys.executable, "./gc.py", args[3], args[1], args[2], args[4])
        else:
            os.close(piread)
            os.close(pwrite)
            os.close(pewrite)
            fds = [pread, s]
            while True:
                rfds, _, _ = select(fds, [], [])
                # next line from gc.py through pipe
                if rfds[0] == pread:
                    data = os.read(pread, 1024)
                    if not data:
                        error = os.read(peread, 1024)
                        pid, status = os.waitpid(pid, 0)
                        rc = status_to_rc(status)
                        mesg = (b"%s\nSubprocess pid %d exited, rc=%d\n" %
                                (error, pid, rc))
                        STDIO.err(mesg)
                        send2clients(fds[2:], mesg.encode('utf-8'))
                        sys.exit(rc)
                    if debug:
                        STDIO.out(data)
                    send2clients(fds[2:], data)
                # control socket accepting a new client
                elif rfds[0] == s:
                    conn, addr = s.accept()
                    fds.append(conn)
                # command from a client on a control socket
                else:
                    try:
                        cmd = rfds[0].recv(16).decode('utf-8')
                    except Exception:
                        fds.remove(rfds[0])
                        continue
                    if not cmd:
                        fds.remove(rfds[0])
                        continue

                    if cmd == "pause":
                        if args[4] == "forward":
                            args[4] = "pause"
                            restart = True

                    elif cmd == "unpause":
                        if args[4] == "pause":
                            args[4] = "forward"
                            restart = True

                    elif cmd == "stop":
                        sys.exit(0)
                    elif cmd == "status" or cmd == "trace":
                        pass
                    else:
                        try:
                            rfds[0].send(b"Error: unrecognized command")
                        except Exception:
                            fds.remove(rfds[0])

                if restart:
                    sleep(1)
                    fds.remove(rfds[0])
                    s.close()
                    os.kill(pid, SIGKILL)
                    os.waitpid(pid, 0)
                    os.close(piwrite)
                    os.close(pread)
                    os.close(peread)
                    sleep(1)
                    break


def stop():
    s = socket(AF_UNIX, SOCK_STREAM, 0)
    try:
        s.bind(GC_CONTROL_ADDRESS)
    except Exception as e:
        if 'Address already in use' in str(e):
            STDIO.out(
                b"Globus Connect Personal is currently running and"
                b" connected to Globus Online\n"
                b"Sending stop signal... "
            )
            s.connect(GC_CONTROL_ADDRESS)
            s.send(b"stop")
            s.close()
            STDIO.out(b"Done\n")
            sys.exit(0)
        else:
            raise
    STDIO.out(b"No Globus Connect Personal connected to Globus Online Service\n")
    sys.exit(1)


def pause():
    s = socket(AF_UNIX, SOCK_STREAM, 0)
    try:
        s.bind(GC_CONTROL_ADDRESS)
    except Exception as e:
        if 'Address already in use' in str(e):
            STDIO.out(
                b"Globus Connect Personal is currently running and"
                b" connected to Globus Online\n"
                b"Sending pause signal... "
            )
            s.connect(GC_CONTROL_ADDRESS)
            s.send(b"pause")
            s.close()
            STDIO.out(b"Done\n")
            sys.exit(0)
        else:
            raise
    STDIO.out(b"No Globus Connect Personal connected to Globus Online Service\n")
    sys.exit(1)


def unpause():
    s = socket(AF_UNIX, SOCK_STREAM, 0)
    try:
        s.bind(GC_CONTROL_ADDRESS)
    except Exception as e:
        if 'Address already in use' in str(e):
            STDIO.out(
                b"Globus Connect Personal is currently running and"
                b" connected to Globus Online\n"
                b"Sending unpause signal... "
            )
            s.connect(GC_CONTROL_ADDRESS)
            s.send(b"unpause")
            s.close()
            STDIO.out(b"Done\n")
            sys.exit(0)
        else:
            raise
    STDIO.out(b"No Globus Connect Personal connected to Globus Online Service\n")
    sys.exit(1)


def status():
    s = socket(AF_UNIX, SOCK_STREAM, 0)
    try:
        s.bind(GC_CONTROL_ADDRESS)
    except Exception as e:
        if 'Address already in use' in str(e):
            s.connect(GC_CONTROL_ADDRESS)
            s.send(b"status")
            data = ""
            while True:
                data = data + s.recv(1024).decode('utf-8')
                ftp = re.search(r'^#gridftp (\w+)$', data, re.MULTILINE)
                relay = re.search(r'^#relaytool ([\w/]+)$', data, re.MULTILINE)
                if ftp and relay:
                    if relay.group(1) == "n/a":
                        print("Globus Online:   disconnected")
                    else:
                        print("Globus Online:   " + relay.group(1))
                    if ftp.group(1) == "0":
                        print("Transfer Status: idle")
                    else:
                        print("Transfer Status: active")
                    break
            s.close()
            sys.exit(0)
        else:
            raise
    STDIO.out(b"No Globus Connect Personal connected to Globus Online Service\n")
    sys.exit(1)


def trace():
    s = socket(AF_UNIX, SOCK_STREAM, 0)
    try:
        s.bind(GC_CONTROL_ADDRESS)
    except Exception as e:
        if 'Address already in use' in str(e):
            s.connect(GC_CONTROL_ADDRESS)
            s.send(b"trace")
            while True:
                try:
                    data = s.recv(1024).decode('utf-8')
                except Exception:
                    STDIO.out(b"Connection reset by peer")
                    sys.exit(0)
                STDIO.out(data.encode('utf-8'))
            s.close()
            sys.exit(0)
        else:
            raise
    STDIO.out(b"No Globus Connect Personal connected to Globus Online Service\n")
    sys.exit(1)


if __name__ == "__main__":
    signal(SIGINT, ctrlc)
    args = sys.argv[1:]
    GC_CONTROL_ADDRESS = "/tmp/globusconnect_%d.sock" % os.getuid()
    if args[0] == "-start":
        start(debug=False)
    elif args[0] == "-debug":
        start(debug=True)
    elif args[0] == "-stop":
        stop()
    elif args[0] == "-pause":
        pause()
    elif args[0] == "-unpause":
        unpause()
    elif args[0] == "-status":
        status()
    elif args[0] == "-trace":
        trace()
