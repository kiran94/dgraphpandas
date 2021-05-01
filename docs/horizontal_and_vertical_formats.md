dgraphpandas takes two kinds of input; vertical and horizontal. In both instances they are expected to be in csv format.

## Horizontal

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

## Vertical

Vertical transformation is very similar to the above Horizontal explanation but we skip the initial pivoting step as the data is already looks like `customer_id`, `predicate`, `object`.

## Edges

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