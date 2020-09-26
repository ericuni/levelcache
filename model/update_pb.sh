#!/bin/bash
set -x

protoc --go_out=./ ./cache.proto

exit 0

