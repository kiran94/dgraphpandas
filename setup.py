from setuptools import setup, find_packages
from dgraphpandas import __name__, __version__, __description__

with open('README.md', 'r') as desc:
    long_description = desc.read()

with open('requirements.txt', 'r') as f:
    requirements = f.read()
    requirements = requirements.split('\n')

setup(
    name=__name__,
    version=__version__,
    description=__description__,
    packages=find_packages(),
    install_requires=requirements,
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/kiran94/dgraphpandas',
    entry_points={
        'console_scripts': [
            'dgraphpandas = dgraphpandas.__main__:main'
        ]
    },
    keywords=['dgraph', 'pandas', 'rdf', 'graph', 'database'],
    python_requires='>=3.6',
    license='MIT'
)
