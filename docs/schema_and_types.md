## Generating a Schema

DGraph allows you to define a [schema](https://dgraph.io/docs/query-language/schema/#sidebar). This can be generated using the same configuration used above but there are also additional options you can add such as `options` and `list_edges` which are exclusively used for schema generation.

```sh
# Model the data, define types, edges and any options
> echo '
{
  "transform": "horizontal",
  "files": {
    "animal": {
      "subject_fields": ["species_id"],
      "type_overrides": {
        "name": "string",
        "legs": "int",
        "weight": "float",
        "height": "float",
        "discovered": "datetime64",
        "aquatic": "bool"
      },
      "edge_fields": ["class_id", "found_in"],
      "options": {
        "name": ["@index(hash)"],
        "discovered": ["@index(year)"],
        "class": ["@reverse"],
        "found_in": ["@reverse", "@count"]
      },
      "list_edges": ["found_in"]
    }
  }
}
' > dgraphpandas.json

# Apply the config to the schema generation logic
> dgraphpandas -c dgraphpandas.json -x schema

# Inspect Schema
> cat schema.txt

found_in: [uid] @reverse @count .
aquatic: bool .
discovered: dateTime @index(year) .
weight: float .
height: float .
legs: int .
name: string @index(hash) .
species: string .
class: uid @reverse .

# Apply to DGraph
dgraph live -s schema.txt
```

## Generating Types

DGraph also allows you to define [types](https://dgraph.io/docs/query-language/type-system/#sidebar) that can be used to categorize nodes. This can also be generated from the same configuration as data loading.


```sh
# Model the data, define types, edges and any options
echo '
{
  "transform": "horizontal",
  "files": {
    "animal": {
      "subject_fields": ["species_id"],
      "type_overrides": {
        "name": "string",
        "legs": "int",
        "weight": "float",
        "height": "float",
        "discovered": "datetime64",
        "aquatic": "bool"
      },
      "edge_fields": ["class_id", "found_in"],
      "class": ["@reverse"],
      "found_in": ["@reverse", "@count"],
      "list_edges": ["found_in"]
    },
    "habitat": {
      "subject_fields": ["id"],
      "type_overrides": {
        "name": "string"
      }
    }
  }
}' > dgraphpandas.json

# Apply the config to the schema generation logic
> dgraphpandas -c dgraphpandas.json -x types -v DEBUG

# Inspect Types
❯ cat types.txt
type animal {
found_in
aquatic
discovered
height
weight
legs
species
name
class
 }

type habitat {
id
name
}


# Apply to DGraph
# NOTE: you should always apply the schema
# before applying types else dgraph
# won't know what the predicates are
dgraph live -s types.txt
```
