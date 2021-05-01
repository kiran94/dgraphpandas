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