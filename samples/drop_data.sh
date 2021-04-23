#!/bin/bash

# This drops all data and schema from your locally running DGraph
# instance. It's intended to be a conveience script to run
# between trying samples.

curl -sX POST localhost:8080/alter -d '
{
    "drop_all": true
}' | jq .
