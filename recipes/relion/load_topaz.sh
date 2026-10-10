#!/bin/bash
set -e
module load topaz/latest
exec topaz "$@"
