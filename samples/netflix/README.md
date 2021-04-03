# Netflix Example

- [Netflix Example](#netflix-example)
  - [Setup](#setup)
  - [Apply to DGraph](#apply-to-dgraph)
  - [Example Queries](#example-queries)
    - [Query 1: Find all Actors related to Horror Movies in 2018](#query-1-find-all-actors-related-to-horror-movies-in-2018)
    - [Query 2: Find all the Movies that Robert Downey Jr is in and the associated Rating](#query-2-find-all-the-movies-that-robert-downey-jr-is-in-and-the-associated-rating)
    - [Query 3: Find all the Titles that both Mike Myers and Seth Green are appear in](#query-3-find-all-the-titles-that-both-mike-myers-and-seth-green-are-appear-in)
    - [Query 4: Find all Titles where the term 'Autobots' appears in the description](#query-4-find-all-titles-where-the-term-autobots-appears-in-the-description)
    - [Query 5: Find the total counts of titles per Genre](#query-5-find-the-total-counts-of-titles-per-genre)


## Setup

*This dataset requires access to [Kaggle](https://www.kaggle.com/docs/api)*.

From the root of the repository

```sh
cd samples/netflix/input/
sh download_data.sh

python download_cleanup.py
```

## Apply to DGraph

From the root of the repository

```sh
sh samples/netflix/generate_upserts.sh
sh samples/netflix/publish_upserts.sh
```

## Example Queries

### Query 1: Find all Actors related to Horror Movies in 2018

```sh
{
  q(func: type(genre)) @filter(eq(identifier, "Horror Movies"))
    {
  		name: identifier
  		titles: ~genre @filter(type(titles) AND eq(release_year, 2018)) {
        name: title
        description

        cast {
          name: identifier
        }
      }
	}
}
```

### Query 2: Find all the Movies that Robert Downey Jr is in and the associated Rating

```sh
{
  q(func: type(cast)) @filter(eq(identifier, "Robert Downey Jr."))
    {
      name: identifier
      titles: ~cast @filter(type(titles)) @normalize {
        name: title
        rating {
          rating: identifier
        }
      }
	}
}
```

### Query 3: Find all the Titles that both Mike Myers and Seth Green are appear in

```sh
{
   mike as var(func: type(cast)) @filter(eq(identifier, "Mike Myers")) { uid }
   seth as var(func: type(cast)) @filter(eq(identifier, "Seth Green")) { uid }

	title(func: type(titles), orderdesc: release_year)
     @filter(uid_in(cast, uid(mike)) AND uid_in(cast, uid(seth)))
   {
        title
        description
        release_year
   }
}
```

### Query 4: Find all Titles where the term 'Autobots' appears in the description

```sh
{
  q(func: type(titles)) @filter(anyofterms(description, "Autobots"))
	{
      identifier
    	title
    	description
    }
}
```

### Query 5: Find the total counts of titles per Genre

```sh
{
  var(func: type(titles)) @groupby(genre)
  {
  	c as count(uid)
  }

  genre_counts(func: uid(c), orderdesc: val(c))
  {
    identifier # from the genre node (grouped)
    count: val(c)
  }
}
```