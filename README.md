# dgraphpandas

[![Build](https://github.com/kiran94/dgraphpandas/actions/workflows/python-package.yml/badge.svg)](https://github.com/kiran94/dgraphpandas/actions/workflows/python-package.yml) ![PyPI](https://img.shields.io/pypi/v/dgraphpandas?color=blue&style=flat-square) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Coverage Status](https://coveralls.io/repos/github/kiran94/dgraphpandas/badge.svg)](https://coveralls.io/github/kiran94/dgraphpandas) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/3484574402e0408c97849301b354be8d)](https://www.codacy.com/gh/kiran94/dgraphpandas/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=kiran94/dgraphpandas&amp;utm_campaign=Badge_Grade)



A Library (with accompanying cli tool) to transform [Pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/index.html#user-guide) DataFrames into Exports ([RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework)) to be sent to [DGraph Live Loader](https://dgraph.io/docs/deploy/fast-data-loading/live-loader/)

- [dgraphpandas](#dgraphpandas)
  - [Usage](#usage)
    - [Command Line](#command-line)
    - [Module](#module)
  - [Getting Started](#getting-started)
  - [Horizontal and Vertical Formats](#horizontal-and-vertical-formats)
    - [Horizontal](#horizontal)
    - [Vertical](#vertical)
    - [Edges](#edges)
  - [Configuration](#configuration)
    - [Additional Configuration](#additional-configuration)
  - [Schema](#schema)
  - [Samples](#samples)
  - [Working with Larger Files](#working-with-larger-files)
    - [Command Line](#command-line-1)
    - [Module](#module-1)
  - [Local Setup](#local-setup)

## Usage

```sh
python -m pip install dgraphpandas
```

### Command Line

```sh
❯ dgraphpandas --help
usage: dgraphpandas [-h] -f FILE -c CONFIG -ck CONFIG_FILE_KEY [-o OUTPUT_DIR]
                    [--console] [--export_csv] [--encoding ENCODING]
                    [--chunk_size CHUNK_SIZE]
                    [--gz_compression_level GZ_COMPRESSION_LEVEL]
                    [--key_separator KEY_SEPARATOR]
                    [--add_dgraph_type_records ADD_DGRAPH_TYPE_RECORDS]
                    [--drop_na_intrinsic_objects DROP_NA_INTRINSIC_OBJECTS]
                    [--drop_na_edge_objects DROP_NA_EDGE_OBJECTS]
                    [--illegal_characters ILLEGAL_CHARACTERS]
                    [--illegal_characters_intrinsic_object ILLEGAL_CHARACTERS_INTRINSIC_OBJECT]
                    [--version] [-v {DEBUG,INFO,WARNING,ERROR,NOTSET}]
```

This is a real example which you can find in the [samples folder](https://github.com/kiran94/dgraphpandas/tree/main/samples) and run from the root of this repository:

```sh
dgraphpandas \
  --config samples/planets/dgraphpandas.json \
  --config_file_key planet \
  --file samples/planets/solar_system.csv \
  --output samples/planets/output
```

### Module

This example can also be found in [Notebook](https://github.com/kiran94/dgraphpandas/blob/main/samples/notebooks/PlanetSample.ipynb) form.

```py
import dgraphpandas as dpd

# Define a Configuration for your data files(s). Explained further in the Configuration section.
config = {
  "transform": "horizontal",
  "files": {
    "planet": {
      "subject_fields": ["id"],
      "edge_fields": ["type"],
      "type_overrides": {
        "order_from_sun": "int32",
        "diameter_earth_relative": "float32",
        "diameter_km": "float32",
        "mass_earth_relative": "float32",
        "mean_distance_from_sun_au": "float32",
        "orbital_period_years": "float32",
        "orbital_eccentricity": "float32",
        "mean_orbital_velocity_km_sec": "float32",
        "rotation_period_days": "float32",
        "inclination_axis_degrees": "float32",
        "mean_temperature_surface_c": "float32",
        "gravity_equator_earth_relative": "float32",
        "escape_velocity_km_sec": "float32",
        "mean_density": "float32",
        "number_moons": "int32",
        "rings": "bool"
      },
      "ignore_fields": ["image", "parent"]
    }
  }
}

# Perform a Horizontal Transform on the passed file using the config/key
# Generate RDF Upsert statements
intrinsic, edges = dpd.to_rdf('solar_system.csv', config, 'planet', output_dir='.', export_rdf=True)

# Do something with these statements e.g write to zip and ship to DGraph
# The cli will zip this output automatically
# In module mode when you provide output_dir and export_rdf it will automatically zip and write to disk
print(intrinsic)
print(edges)
```

Alternatively, you could call the underlying methods

```py
# Perform a Horizontal Transform on the passed file using the config/key
intrinsic, edges = horizontal_transform('solar_system.csv', config, "planet")
# Generate RDF Upsert statements
intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)
```

## Getting Started

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

## Horizontal and Vertical Formats

dgraphpandas takes two kinds of input; vertical and horizontal. In both instances they are expected to be in csv format.

### Horizontal

Horizontal follows a tabular structure and is probably the more likely format found out in the wild. It might look like this:

```txt
customer_id    weight    height
1              90        190
2              23        120
3              100       56
```

When you provide the subject fields as `['customer_id']` then dgraphpandas will be treat the rest of the columns as data values. It will be pivoted like this:

```txt
customer_id    predicate    object
1              weight       90
1              height       190
2              weight       23
2              height       120
3              weight       100
3              height       56
```

Then along with the options provided within the passed configuration then the output RDF might look like this:

```xml
<customer_1>     <weight>       "90"^^<xs:int> .
<customer_1>     <height>       "190"^^<xs:int> .
<customer_2>     <weight>       "23"^^<xs:int> .
<customer_2>     <height>       "120"^^<xs:int> .
<customer_3>     <weight>       "100"^^<xs:int> .
<customer_3>     <height>       "56"^^<xs:int> .
```

Where `customer_` was appended as it was defined as the `type` for this export and types were associated because it was defined inside `type_overrides`.

### Vertical

Vertical transformation is very similar to the above Horizontal explanation but we skip the initial pivoting step as the data is already looks like `customer_id`, `predicate`, `object`.

### Edges

Edges are derived from the `edge_fields` defined inside the file level configuration and they are sent just like data fields from the input file.

As they are defined in `edge_fields`, dgraphpandas will split these out and treat them slightly differently during transformation and generation of the RDF output.

For example if we had an E-Commerce Orders table:

```txt
order_id    customer_id    store_id
5           1              1
9           2              2
2           3              1
```

And we had a configuration like this:

```json
{
    "transform": "horizontal",
    "files": {
       "order": {
            "subject_fields": ["order_id"],
            "edge_fields": ["customer_id", "store_id"]
        }
    }
}
```

Then the output RDF would look like this:

```xml
<order_5> <customer> <customer_1> .
<order_9> <customer> <customer_2> .
<order_2> <customer> <customer_3> .
<order_5> <store> <store_1> .
<order_9> <store> <store_2> .
<order_2> <store> <store_1> .
```

Where each of the orders has been associated with it's customer and store.

## Configuration

A Configuration file influences how we transform a DataFrame. It consists of:

-   Global configuration options
    -   Options which will be applied to all files
    -   These can either be defined in the configuration or as `kwargs` in the transform method or both where the `kwargs` takes priority.
    -   A collection of `files`

-   File configuration options
    -   Options which will be applied only to this entry
    -   `subject_fields` is required so the unique identifier for a row in the DataFrame can be found
    -   `edge_fields` are optional and if provided will generate edge output
    -   `type_overrides` are optional but recommended to ensure the correct type is attached in RDF

If you are running this with the module and passing via `kwargs` then the above options may also be lambda callable that takes the DataFrame as an input. For example if you didn't want to hard code all your edge fields and were following a convention that all edge fields have suffix `_id` then you could set the edge_fields to `lambda frame: frame.loc[frame['predicate'].str.endswith('_id'), 'predicate'].unique().tolist()`. For this specific convention, it's common enough to have it's own built in option. See `edge_id_convention`

An example of the configuration for the [planets sample](https://github.com/kiran94/dgraphpandas/blob/main/samples/planets/solar_system.csv) might look like this:

```py
config = {
  "transform": "horizontal",
  "files": {
    "planet": {
      "subject_fields": ["id"],
      "edge_fields": ["type"],
      "type_overrides": {
        "order_from_sun": "int32",
        "diameter_earth_relative": "float32",
        "diameter_km": "float32",
        "mass_earth_relative": "float32",
        "mean_distance_from_sun_au": "float32",
        "orbital_period_years": "float32",
        "orbital_eccentricity": "float32",
        "mean_orbital_velocity_km_sec": "float32",
        "rotation_period_days": "float32",
        "inclination_axis_degrees": "float32",
        "mean_temperature_surface_c": "float32",
        "gravity_equator_earth_relative": "float32",
        "escape_velocity_km_sec": "float32",
        "mean_density": "float32",
        "number_moons": "int32",
        "rings": "bool"
      },
      "ignore_fields": ["image", "parent"]
    }
  }
}
```

### Additional Configuration

**Global Level**

These options can be placed on the root of the config or passed as `kwargs` directly.

-   `add_dgraph_type_records`
    -   DGraph has a special predicate called [`dgraph.type`](https://dgraph.io/docs/query-language/type-system/#setting-the-type-of-a-node), this can be used to query via the `type()`
        function. If `add_dgraph_type_records` is enabled, then we add `dgraph.type` fields
        to the current export.

-   `strip_id_from_edge_names`
    -   Its common for a data set to have a reference to another 'table' using `_id` convention
    -   You may not want the `_id` in your predicate name so this options strips it away
    -   For example if you had a Student & School then the student might more sense to have `(Student) - school -> (School)` rather then `(Student) - school_id -> (School)`.

-   `drop_na_intrinsic_objects`
    -   Automatically drop intrinsic records where the object is NA. In a relational model, you might have a column with a `null` entry however in a graph model you may want to omit the attribute altogether.

-   `drop_na_edge_objects`
    -   Same as `drop_na_intrinsic_objects` but for edges.

-   `key_separator`
    -   Separator used to combine key fields. For example if the key separator was `_` and we were operating on an intrinsic attribute for a customer with id 1 then the `xid` would be `customer_1` but if our seperator was `$` then it would be `customer$1`.

-   `illegal_characters`
    -   Characters to strip from intrinsic and edge subjects. if the unique identifier has a character not supported by RDF/DGraph then strip them away or they will not be accepted by live loading.

-   `illegal_characters_intrinsic_object`
    -   Same as `illegal_characters` but for the subject on intrinsic fields. These have a different set of illegal characters because subjects on intrinsic records are actual data values and are quoted. They therefore can accept many more characters then the subject.

- `ensure_xid_predicate`
  - Schema generation option to ensure that the `xid` predicate is applied to the schema. If you use the `--upsertPredicate xid` then this must be set so that the predicate is created and indexed.

**File Level**

-   `type_overrides`
    -   Recommended. This ensures that data types are being treated as a type and the output RDF has the correct type mapped into it. Without this fields will go under the default rdf type `<xs:string>` but you may want a field to be a true int in RDF.
    -   Additionally certain data types such as `datetime64` will activate special handling to ensure the output in RDF is within the correct format to be ingested into DGraph.
    -   Supported Types can be found [here](https://github.com/kiran94/dgraphpandas/blob/main/dgraphpandas/types.py)

-   `csv_edges`
    -   Sometimes a vendor will provide a data file where a single column is actually a csv list and each csv value should be broken into multiple RDF statements (because they relate to independent entities). Adding that column into this list will do that.
    -   For example in the [netflix sample / title file](https://github.com/kiran94/dgraphpandas/blob/e5b2864eeb285bcf4d41215f70c4675a0bc95075/samples/netflix/dgraphpandas.json#L43) we have a `cast` column where the values are `actor_1, actor2`. Enabling `csv_edges` will ensure that the movie has 2 different relationships for each cast member.

-   `csv_edges_separator`
    -   Alternative separator for `csv_edges`

-   `ignore_fields`
    -   Add fields in the input that we don't care about to this list so they aren't present in the output

-   `override_edge_name`
    -   Ensure that the edge name as a different predicate and/or target_node_type to what is defined in the file.
    -   For example in the [pokemon sample / pokemon_species](https://github.com/kiran94/dgraphpandas/blob/e5b2864eeb285bcf4d41215f70c4675a0bc95075/samples/pokemon/dgraphpandas.json#L99-L103) file you will see a column called `evolves_from_species` which tells us for a given pokemon which other pokemon does it evolve from. If we were to use the raw data here we would get a `evolves_from_species` edge with an incorrect target xid. Instead we want to override the `target_node_type` to `pokemon` so the edge correctly loops back to a node of the same type.

-   `pre_rename`
    -   Rename intrinsic predicates or edge names to something else

-   `read_csv_options`
    -   Applied to the [`pd.read_csv`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) call when a file is passed to a transform
    -   For example if the vendor file was tab separated then this could be `{'sep': '\t'}`

-   `date_fields`
    -   Apply datetime options to a field. This option can be useful when the input file has a date column with an unsual format. For each field, this object is passed into [`pd.to_datetime`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html). For example if you had a column called `dob` then you could set this object to `{ "dob": {"format": "%Y-%m-%d"} }`. All the [standard](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) format codes are supported.

-   `edge_id_convention`
    -   Applies `_id` convention to find edges when set to `true`
    -   Same as providing the edge_field `lambda frame: frame.loc[frame['predicate'].str.endswith('_id'), 'predicate'].unique().tolist()`.

-   `predicate_field`
    -   Only applicable for vertical transforms
    -   Allows you to define your own predicate field name if not the default `predicate`

-   `object_field`
    -   Only applicable to vertical transforms
    -   Allows you to define your own object field name if not the default `object`

- `options`
    - Additional Options for Schema generation such as indexes or other directives.
    - This is a key value pair between a intrinsic/edge to list of directives to apply
    - e.g `"title": ["@index(exact, fulltext)", "@count"]`

- `list_edges`
    - Schema option to define an edge as a list. This will ensure the type is `[uid]` rather then just `uid`


## Schema

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

## Samples

Samples can be found [here](https://github.com/kiran94/dgraphpandas/tree/main/samples). They follow a convention where the download script can be found within the `input` directory and the config, generate_upsert, publish scripts can be found root of each respective sample.

There are also [Jupyter Notebooks](https://github.com/kiran94/dgraphpandas/tree/main/samples/notebooks) which should show step by step examples.

## Working with Larger Files

If you have very large input files, it may make sense to break up your files into smaller ones to reduce the likely hood of memory issues.

dgraphpandas provides facilities to break up exports via the cli tool into chunks or if you are using the module directly then you can find an example below on how to use pandas to break up your file.

### Command Line

In the CLI you have the `chunk_size` parameter to determine an upper limit for your files.

```sh
python -m dgraphpandas \
  -c samples/netflix/dgraphpandas.json \
  -ck title -f samples/netflix/input/netflix_titles.csv \
  -o samples/netflix/output \
  --chunk_size 1000
```

When you pass this, only `chunk_size` lines will be pushed through the RDF generation logic at a time and the output will be indexed per chunk. For example:

```sh
❯ ls -la samples/netflix/output/
total 12M
drwxr-xr-x 2 kiran kiran 4.0K Apr  4 18:13 .
drwxr-xr-x 6 kiran kiran 4.0K Apr  4 16:45 ..
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_2.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_3.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_4.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_5.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_6.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_7.gz
-rw-r--r-- 1 kiran kiran 706K Apr  4 18:13 netflix_titles_edges_8.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_2.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_3.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_4.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_5.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_6.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_7.gz
-rw-r--r-- 1 kiran kiran 701K Apr  4 18:13 netflix_titles_intrinsic_8.gz
```

You can then take these exports and live load them as normal.

### Module

The `chunk_size` method is also available on `to_rdf`. If you provide an `output_dir` & `export_rdf` this will automatically be written out to an export file on disk.

For Example:

```py
import dgraphpandas as dpd

dpd.to_rdf('your_input.csv', config, 'your_input_key', output_dir='.', export_rdf=True, chunk_size=1000)
```

If you wanted more control, then you could also call the underlying methods to leverage the fact that the transform methods can take a `DataFrame` directly and you can pre-chunk before you enter.

```py
from dgraphpandas.strategies.horizontal import horizontal_transform
from dgraphpandas.writers.upserts import generate_upserts

# Each Chunk won't be loaded into memory until it hits that particular loop.
for index, frame in enumerate(pd.read_csv('your_input.csv', chunksize=1000)):

  # Generate for this Chunk
  intrinsic, edges = horizontal_transform(frame, dgraphpandas_config, 'your_input_key')

  # Generate Rdf Upserts for this Chunk
  intrinsic_upserts, edges_upserts = generate_upserts(intrinsic, edges)

  # Then you can do whatever you want with these before the next iteration
```

## Local Setup

Assuming you have already cloned the repo and have a terminal in the root of the project.

```sh
# Create Virtual Environment and Activate it
conda create -n dgraphpandas python=3.6 # or venv
conda activate dgraphpandas

# Restore packages
python -m pip install -r requirements-dev.txt
python -m pip install -r requirements.txt

# Run Flake
flake8 --count .

# Run Tests
python -m unittest

# Create & Run DGraph
docker-compose up

# Try a Sample
# See Sample section for more details
# It should help getting some data,
# generating rdf and publishing to your
# local DGraph

# If you are making changes then
# Install a Local Copy of the Library
python -m pip install -e .

# Remember to uninstall once done
python -m pip uninstall dgraphpandas -y
```
