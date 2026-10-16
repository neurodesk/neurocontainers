"""Inventory scientific software and system runtime files (Python 2/3)."""

from __future__ import print_function
import hashlib
import json
import os
import stat
import sys

roots = [
    "/opt/freesurfer-6.0.0",
    "/usr",
    "/lib",
    "/lib64",
    "/bin",
    "/sbin",
    "/etc/ld.so.conf",
    "/etc/ld.so.conf.d",
    "/etc/ld.so.cache",
]
records = {}


def record(path):
    info = os.lstat(path)
    item = {"mode": stat.S_IMODE(info.st_mode)}
    if stat.S_ISLNK(info.st_mode):
        item.update(kind="symlink", target=os.readlink(path))
    elif stat.S_ISREG(info.st_mode):
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            while True:
                block = stream.read(8 * 1024 * 1024)
                if not block:
                    break
                digest.update(block)
        item.update(kind="file", bytes=info.st_size, sha256=digest.hexdigest())
    elif stat.S_ISDIR(info.st_mode):
        item.update(kind="directory")
    else:
        item.update(kind="other")
    records[path] = item


for root in roots:
    if not os.path.lexists(root):
        continue
    record(root)
    if os.path.isdir(root):
        for parent, directories, files in os.walk(root):
            for name in directories + files:
                record(os.path.join(parent, name))
json.dump(records, sys.stdout, sort_keys=True, indent=2)
print()
