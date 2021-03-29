!#/bin/bash

# Requires Kaggle - https://www.kaggle.com/docs/api
kaggle datasets download shivamb/netflix-shows

unzip netflix-shows.zip
rm netflix-shows.zip

cat netflix_titles.csv | awk 'NR!=1' | awk -F, '{ print $2","$2 }' | sort | uniq | awk 'BEGIN {print "id,desc"}; {print $0}' | awk 'NR!=2' > show_types.csv