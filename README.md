# gtfspy  

[![Gtfspy-master Travis CI build passing](https://api.travis-ci.org/CxAalto/gtfspy.svg?branch=master)](https://travis-ci.org/CxAalto/gtfspy)
[![PyPI badge](https://badge.fury.io/py/gtfspy.svg)](https://pypi.python.org/pypi/gtfspy/)


``gtfspy`` is a Python package for working with public transport timetable data provided in the [General Transit Feeds Specification](https://developers.google.com/transit/gtfs/) (GTFS) format.

## Core features:
* Import one or multiple GTFS feeds into a [SQLite](https://www.sqlite.org/) database.
* Update this sqlite database using Open Street Map (OSM) data.
* Compute summary statistics
* Filter databases spatially and temporally
* Perform accessibility analyses using a routing/profiling engine
    - Routing implementation [Connection Scan Algorithm](http://i11www.iti.uni-karlsruhe.de/extra/publications/dpsw-isftr-13.pdf) (CSA).
    - Used for computing travel times and transfers between origin-destination pairs

## Prerequisites
* [Python 3.5 (or above)](https://www.python.org/)
* Supported platforms: Linux + OSX (Windows is currently not supported, please creat an issue in Github if you would be interested in running ``gtfspy`` on Windows)
* Optional: [git](https://git-scm.com/) is used for development.


## Install

```
pip install gtfspy
```

## Development quickstart

Only use this if you want to be able to edit ``gtfspy``'s source code.

```
git clone git@github.com:CxAalto/gtfspy.git
cd gtfspy/
pip install -r requirements.txt # install any requirements
nosetests . # run tests
```

Remember to also add the ``gtfspy`` directory to your ``PYTHONPATH`` environment variable.

## Examples
- [Importing a GTFS feed into a sqlite database](examples/example_import.py)
- [Validating an imported feed TODO!](examples/example_validation.py)
- [Computing and plotting temporal distance profiles between an origin--destination pair](examples/example_temporal_distance_profile.py)
- [Visualizing the public transport network on map](examples/example_map_visualization.py)
- [TODO! Filtering GTFS feed spatially and in time-domain](examples/example_filter.py)
- [TODO! Extracting a temporal network (and other formats)](examples/example_export.py)
- [TODO! Running a simple accessibility analysis pipeline!](examples/example_accessibility_analysis.py)


## Contributing

We welcome contributions as GitHub pull requests.
In your pull request, please also add yourself as a contributor in the list below.

## Versioning


## Authors

### Package maintainers
* Rainer Kujala (Rainer.Kujala@gmail.com, rmkujala)
* Richard Darst
* Christoffer Weckström
* Nils Haglund

### Other contributors
* You?


## Licenses

### Code
This source code of this project licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

### Data
The OpenStreetMap data (.osm.pbf file(s) under examples/data) is licenced under the [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/) (ODbL) by the [OpenStreetMap Foundation](http://osmfoundation.org/) (OSMF).

The GTFS data used for the examples is provided by the City of Kuopio (Finland), and have been downloaded from [http://bussit.kuopio.fi/gtfs/gtfs.zip](http://bussit.kuopio.fi/gtfs/gtfs.zip) [data licensed [under CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/deed)].


## Usage for scientific purposes

If you use this code for scientific purposes, please cite our paper [TO BE ANNOUNCED].


## Acknowledgments

* The development of this Python package has benefited from the support by Academy of Finland through the DeCoNet project.


## See also

Code using (an older version of) ``gtfspy``: (https://github.com/rmkujala/ptn_temporal_distances)
