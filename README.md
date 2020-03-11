# gtfspy  

[![Gtfspy-master Travis CI build passing](https://api.travis-ci.org/CxAalto/gtfspy.svg?branch=master)](https://travis-ci.org/CxAalto/gtfspy)
[![PyPI badge](https://badge.fury.io/py/gtfspy.svg)](https://pypi.python.org/pypi/gtfspy/)


``gtfspy`` is a Python package for analyzing public transport timetable data provided in the [General Transit Feed Specification](https://developers.google.com/transit/gtfs/), GTFS, -format.

## Core features:
* Import one or multiple GTFS feeds into one [SQLite](https://www.sqlite.org/) database for efficient querying of the data.
* Augment the sqlite with real walking distances between PT stops using Open Street Map (OSM) data.
* Compute simple statistics for the public transport networks such as number of stops, routes, network length.
* Filter databases spatially and temporally to match your area and time region of interest.
* Perform accessibility analyses using a routing/profiling engine
    - Adapted from the [Connection Scan Algorithm](http://i11www.iti.uni-karlsruhe.de/extra/publications/dpsw-isftr-13.pdf) (CSA).
    - Compute all Pareto-optimal journey alternatives between an origin-destination pair, and summarize connectivity with measures on travel time and number of transfers.
* Produce data extracts in various formats (network edge lists, geojson). 


## Prerequisites
* [Python 3.8](https://www.python.org/)
* Supported platforms: Linux, OSX & Windows
* Optional: [git](https://git-scm.com/) is used for development.


## Install
### Linux and Mac OS
```
pip install gtfspy
```

### Windows
Windows should work, but has not been tested or and may not be
supported as much.  Please report problems.

Windows users may need to install Shapely library first. [Download Shapely wheel](https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely) and then run:
```
pip install wheel
pip install {path to the Shapely wheel file on your PC}
```

If you come across the `Microsoft Visual C++ 14.0 is required` error, you may need to download the latest Microsoft Visual C++ Build Tools.
You can download it [from here](https://visualstudio.microsoft.com/cs/downloads/).

After that, continue with:
```
pip install gtfspy
```



## Development quickstart

Use this if you want to be able to edit ``gtfspy``'s source code.

```
git clone git@github.com:CxAalto/gtfspy.git
cd gtfspy/
pip install -r requirements.txt # install any requirements
nosetests . # run tests
```

Remember to also add the ``gtfspy`` directory to your ``PYTHONPATH`` environment variable.

## Examples
- [Importing a GTFS feed into a sqlite database](examples/example_import.py)
- [TODO: Validate an imported feed](examples/example_validation.py)
- [Compute and plot temporal distance profiles between an origin--destination pair](examples/example_temporal_distance_profile.py)
- [Visualizing the public transport network on map](examples/example_map_visualization.py)
- [Filter GTFS feed spatially and temporally](examples/example_filter.py)
- [Extract a network / a temporal network from the GTFS database](examples/example_export.py)
- [TODO! Run a simple accessibility analysis pipeline!](examples/example_accessibility_analysis.py)


## Contributing

We welcome contributions as GitHub pull requests.
In your pull request, please also add yourself as a contributor in the list below.

## Versioning

This library is not yet stabilised, and new features are being developed. 
Thus code organization and interfaces may change at a fast pace. 
More precise versioning scheme will be decided upon later.

## Changelog

View the [changelog](CHANGELOG.md).

## Authors

### Package maintainers
* Rainer Kujala
* Richard Darst
* Christoffer Weckström

### Other contributors

* Manuel Rios ([marz7002](https://github.com/marz7002))
* Nils Haglund
* Michaela Ockova ([evelyn9191](https://github.com/evelyn9191))

* You?

## Licensing

### Code
This source code of this project licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

### Example data

The OpenStreetMap data (.osm.pbf file(s) under examples/data) is licenced under the [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/) (ODbL) by the [OpenStreetMap Foundation](http://osmfoundation.org/) (OSMF).

The GTFS data used for the examples is provided by the City of Kuopio (Finland), and have been downloaded from [http://bussit.kuopio.fi/gtfs/gtfs.zip](http://bussit.kuopio.fi/gtfs/gtfs.zip) [data licensed [under CC-BY 4.0](https://creativecommons.org/licenses/by/4.0/deed)].


## Usage for scientific purposes

If you use this Python package for scientific purposes, please cite our paper

Rainer Kujala, Christoffer Weckström, Miloš N. Mladenović, Jari Saramäki, Travel times and transfers in public transport: Comprehensive accessibility analysis based on Pareto-optimal journeys, In Computers, Environment and Urban Systems, Volume 67, 2018, Pages 41-54, ISSN 0198-9715, [https://doi.org/10.1016/j.compenvurbsys.2017.08.012](https://doi.org/10.1016/j.compenvurbsys.2017.08.012).


## Acknowledgments

* The development of this Python package has benefited from the support by Academy of Finland through the DeCoNet project.
* For running the Java routing, we use the [Graphhopper routing library](https://github.com/graphhopper/graphhopper).


## Bugs

If you have any problems using ``gtfspy`` please create an issue in GitHub.  

## Other questions on 

If you have any questions regarding ``gtfspy``, feel free to send the package maintainers (see above) an e-mail!

## See also

[Code for a research project using ``gtfspy``](https://github.com/rmkujala/ptn_temporal_distances)

[Web-visualization tool utilizing gtfspy (gtfspy-webviz)](https://github.com/CxAalto/gtfspy-webviz)
