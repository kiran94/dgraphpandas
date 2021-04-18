#!/bin/bash

# Assuming run from the samples/planets directory.

echo 'Creating Schema'
dgraphpandas -c dgraphpandas.json -x schema

echo 'Creating Upserts'
dgraphpandas -c dgraphpandas.json -f solar_system.csv -ck planet

echo 'Applying Exports'
dgraph live --upsertPredicate xid --xidmap xidmap -f solar_system_intrinsic.gz --format rdf -s schema.txt
dgraph live --upsertPredicate xid --xidmap xidmap -f solar_system_edges.gz --format rdf -s schema.txt

echo 'Cleaning Up'
rm *.gz
rm schema.txt

# Note: You don't want to delete these for a real app
# they are just removed here for keeping the sample folder clean
rm -rf t
rm -rf xidmap
