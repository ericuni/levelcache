#!/bin/bash
set -x

protoc --go_out=./model ./cache.proto

exit 0

