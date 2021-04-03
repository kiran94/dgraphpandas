#!/bin/bash

# Requires Kaggle - https://www.kaggle.com/docs/api
echo Downloading DataSet
kaggle datasets download shivamb/netflix-shows

unzip netflix-shows.zip
rm netflix-shows.zip

