'''
Break up the single data file into different types depending on the nodes we want
'''
import pandas as pd

source = pd.read_csv('./netflix_titles.csv')

print('Generating title types')
title_type: pd.DataFrame = source[['type']].copy()
title_type['id'] = title_type['type']
title_type.drop_duplicates(subset=['id'], inplace=True)
title_type = title_type[['id', 'type']]
title_type.to_csv('show_types.csv', index=False)

print('Generating directors')
directors = source['director'].dropna().str.split(',').explode().str.strip()
directors = pd.DataFrame(directors)
directors['id'] = directors['director']
directors.to_csv('directors.csv', index=False)

print('Generating cast')
cast = source['cast'].dropna().str.split(',').explode().str.strip()
cast = pd.DataFrame(cast)
cast['id'] = cast['cast']
cast.to_csv('cast.csv', index=False)

print('Generating rating')
rating: pd.DataFrame = source[['rating']].copy()
rating['id'] = rating['rating']
rating.drop_duplicates(subset=['rating'], inplace=True)
rating.dropna(inplace=True)
rating = rating[['id', 'rating']]
rating.to_csv('rating.csv', index=False)

print('Generating genre')
genre = source['listed_in'].dropna().str.split(',').explode().str.strip()
genre = pd.DataFrame(genre)
genre.rename(columns={'listed_in': 'genre'}, inplace=True)
genre.dropna(inplace=True)
genre['id'] = genre['genre']
genre.to_csv('genre.csv', index=False)
