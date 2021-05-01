If you have very large input files, it may make sense to break up your files into smaller ones to reduce the likely hood of memory issues.

dgraphpandas provides facilities to break up exports via the cli tool into chunks or if you are using the module directly then you can find an example below on how to use pandas to break up your file.

## Command Line

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

## Module

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
