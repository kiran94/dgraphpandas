#!/bin/bash
set -e

DGRAPH_TEMP_BUFFERS=samples/netflix/dgraph_temp_buffers
UPSERT_PREDICATE=xid
XIDMAP_LOCATION=samples/netflix/xidmap
OUTPUT_DIR=samples/netflix/output

echo Ensuring xid has index
curl -sX POST localhost:8080/alter -d 'xid: string @index(exact) .' | jq .

echo Applying Data Files
for f in $OUTPUT_DIR/*.gz; do
    echo $f
    dgraph live --tmp $DGRAPH_TEMP_BUFFERS --files $f --upsertPredicate xid --xidmap $XIDMAP_LOCATION --format rdf --batch 500
done


echo Applying Reverse Edges
curl -sX POST localhost:8080/alter -d 'cast: [uid] @reverse .' | jq .
curl -sX POST localhost:8080/alter -d 'genre: [uid] @reverse .' | jq .
curl -sX POST localhost:8080/alter -d 'director: [uid] @reverse .' | jq .
curl -sX POST localhost:8080/alter -d 'rating: [uid] @reverse .' | jq .
curl -sX POST localhost:8080/alter -d 'description: string @index(term) .' | jq .
