# gtfspy

The gtfspy python package is used for routing, transforming, filtering and validating public transport timetable data in the General Transit Feeds Specification (GTFS) format.
For more information about the GTFS standard: https://developers.google.com/transit/gtfs/

Current core features:
* Importing one or multiple GTFS feeds into a sqlite database.
* Update this sqlite database using Open Street Map (OSM) data.
* A routing/profiling engine using the Connection Scan Algorithm (CSA).
    - Used for computing travel times and transfers between origin-destination pairs

Support features:
* Validation of GTFS feeds
* Extraction of basic summary statics about the feed

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites
* Support for Python 3.4
* Currently supported platforms: Linux + OSX (Windows support is only partial at the moment)

What things you need to install the software and how to install them

```
Give examples
```

### Installing

A step by step series of examples that tell you have to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```


### Recommended set up for storing GTFS feeds

## Simple use case examples
- Importing gtfs feed (`examples/example_import.py`)
- Generation of shortest path walking distances using OSM  (`examples/example_add_footpaths.py`)
- Filtering (`examples/example_filter.py`)
- Validation (`examples/example_validation.py`)
- Extracting a temporal network (and other formats) (`examples/`)
- Running an analysis pipeline(`examples/example_analysis.py`)

## Contributing

## Versioning

## Authors

* **Rainer Kujala** (Rainer.Kujala@gmail.com, @rmkujala) - *Initial work*
* **Richard Darst** - *Initial work*
* **Christoffer Weckström** - *Work*
* **Nils Haglund**

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Usage for scientific purposes
If you use this code for scientific purposes, please cite our paper ("Travel times and transfers in public transport networks: comprehensive accessibility analysis using Pareto-optimal journeys").

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc

## To run the unit tests, run nosetests in the root directory:
```
nosetests .
```

