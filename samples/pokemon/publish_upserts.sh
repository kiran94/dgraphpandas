#!/bin/bash
set -e

DGRAPH_TEMP_BUFFERS=samples/pokemon/dgraph_temp_buffers
UPSERT_PREDICATE=xid
XIDMAP_LOCATION=samples/pokemon/xidmap
OUTPUT_DIR=samples/pokemon/output

echo Ensuring xid has index
curl -sX POST localhost:8080/alter -d 'xid: string @index(exact) .' | jq .

echo Applying Data Files
for f in $OUTPUT_DIR/*.gz; do
    echo "$f"
    dgraph live --tmp $DGRAPH_TEMP_BUFFERS --files "$f" --upsertPredicate UPSERT_PREDICATE --xidmap $XIDMAP_LOCATION --format rdf
done
