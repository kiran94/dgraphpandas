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

## Additional Configuration

### Global Level

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

### File Level

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
