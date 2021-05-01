# Getting Started

Fire up a Terminal (Assuming not Windows but WSL is okay).

```sh
# Create a Configuration
echo '{
    "transform": "horizontal",
    "files": {
       "animal": {
            "subject_fields": ["species_id"],
            "edge_fields": ["habitat_id"],
            "type_overrides": {
              "number_of_legs": "int32"
            }
        }
    }
}' > dgraphpandas.json

# Create a Data file
echo "species_id,name,number_of_legs,habitat_id,
1,Elephant,4,10,
2,Lion,4,7,
3,Flamingo,2,78,
" > animals.csv

# Run dgraphpandas
dgraphpandas -f animals.csv -c dgraphpandas.json -ck animal

# Unzip output
gzip -d animals_intrinsic.gz
gzip -d animals_edges.gz

# Verify Output
❯ cat animals_intrinsic
<animal_1> <name> "Elephant"^^<xs:string> .
<animal_2> <name> "Lion"^^<xs:string> .
<animal_3> <name> "Flamingo"^^<xs:string> .
<animal_1> <number_of_legs> "4"^^<xs:int> .
<animal_2> <number_of_legs> "4"^^<xs:int> .
<animal_3> <number_of_legs> "2"^^<xs:int> .
<animal_1> <dgraph.type> "animal"^^<xs:string> .
<animal_2> <dgraph.type> "animal"^^<xs:string> .
<animal_3> <dgraph.type> "animal"^^<xs:string> .

❯ cat animals_edges
<animal_1> <habitat> <habitat_10> .
<animal_2> <habitat> <habitat_7> .
<animal_3> <habitat> <habitat_78> .
```