# gtfspy

The gtfspy python package is used for routing, transforming, filtering and validating public transport timetable data in the General Transit Feeds Specification (GTFS) format.
For more information about the GTFS standard: https://developers.google.com/transit/gtfs/

Current core features:
* Transformation of one GTFS feed or merging several GTFS feeds into a sqlite database
* Importing of Open Street Map (OSM) street layer for pedestrian routing
* Routing engine using the Connection Scan Algorithm (CSA)

Support features:
* Validation of GTFS feeds
* Extraction of basic summary statics about the feed

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites
* Support for Python 3.4

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
- Importing gtfs feed
- Generation of shortest path walking distances using OSM
- Filtering
- (Validation)
- Extracting a temporal network
- Running routing engine

## Contributing

## Versioning

## Authors

* **Rainer Kujala** - *Initial work*
* **Richard Darst** - *Initial work*
* **Christoffer Weckström** - *Work*

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone who's code was used
* Inspiration
* etc


## To test the code, run nosetests in the base directory
nosetests .

