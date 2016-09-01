# Setting up travel modes, directly extending from GTFS specification, see:
# https://developers.google.com/transit/gtfs/reference#routestxt

TRAVEL_MODE_WALK = -1
TRAVEL_MODE_TRAM = 0
TRAVEL_MODE_SUBWAY = 1
TRAVEL_MODE_RAIL = 2
TRAVEL_MODE_BUS = 3
TRAVEL_MODE_FERRY = 4
TRAVEL_MODE_CABLE_CAR = 5
TRAVEL_MODE_GONDOLA = 6
TRAVEL_MODE_FUNICULAR = 7

TRAVEL_MODE_TO_DESCRIPTION = {
    TRAVEL_MODE_WALK: "Walking layer",
    TRAVEL_MODE_TRAM: "Tram, Streetcar, Light rail. Any light rail "
                      "or street level system within a metropolitan area.",
    TRAVEL_MODE_SUBWAY: "Subway, Metro. Any underground rail system within a metropolitan area.",
    TRAVEL_MODE_RAIL: "Rail. Used for intercity or long - distance travel.",
    TRAVEL_MODE_BUS: "Bus. Used for short- and long-distance bus routes.",
    TRAVEL_MODE_FERRY: "Ferry. Used for short- and long-distance boat service.",
    TRAVEL_MODE_CABLE_CAR: "Cable car. Used for street-level cable cars "
                           "where the cable runs beneath the car.",
    TRAVEL_MODE_GONDOLA: "Gondola, Suspended cable car. "
                         "Typically used for aerial cable cars where "
                         "the car is suspended from the cable.",
    TRAVEL_MODE_FUNICULAR: "Funicular. Any rail system designed for steep inclines."
}

TRAVEL_MODE_TO_SHORT_DESCRIPTION = {
    TRAVEL_MODE_WALK: "Walk",
    TRAVEL_MODE_TRAM: "Tram",
    TRAVEL_MODE_SUBWAY: "Subway",
    TRAVEL_MODE_RAIL: "Rail",
    TRAVEL_MODE_BUS: "Bus",
    TRAVEL_MODE_FERRY: "Ferry",
    TRAVEL_MODE_CABLE_CAR: "Cable car",
    TRAVEL_MODE_GONDOLA: "Gondola",
    TRAVEL_MODE_FUNICULAR: "Funicular"
}