# Route types, as specified in the GTFS reference:
# https://developers.google.com/transit/gtfs/reference#routestxt

WALK = -1
TRAM = 0
SUBWAY = 1
RAIL = 2
BUS = 3
FERRY = 4
CABLE_CAR = 5
GONDOLA = 6
FUNICULAR = 7
TROLLEYBUS = 11
MONORAIL = 12
AIRCRAFT = 99

ALL_ROUTE_TYPES = {WALK, TRAM, SUBWAY, RAIL, BUS, FERRY, CABLE_CAR, GONDOLA, FUNICULAR}
TRANSIT_ROUTE_TYPES = ALL_ROUTE_TYPES.difference({WALK})

ROUTE_TYPE_TO_ZORDER = {
    WALK: 2,
    BUS: 3,
    RAIL: 4,
    SUBWAY: 5,
    TRAM: 6,
    FERRY: 7,
    CABLE_CAR: 8,
    GONDOLA: 9,
    FUNICULAR: 10,
    TROLLEYBUS: 11,
    MONORAIL: 12,
    AIRCRAFT: 13
}

ROUTE_TYPE_TO_DESCRIPTION = {
    WALK: "Walk, pedestrian travel",
    SUBWAY: "Subway, Metro. Any underground rail system within a metropolitan area.",
    RAIL: "Rail. Used for intercity or long - distance travel.",
    BUS: "Bus. Used for short- and long-distance bus routes.",
    FERRY: "Ferry. Used for short- and long-distance boat service.",
    CABLE_CAR: "Cable car. Used for street-level cable cars "
                           "where the cable runs beneath the car.",
    GONDOLA: "Gondola, Suspended cable car. "
                         "Typically used for aerial cable cars where "
                         "the car is suspended from the cable.",
    FUNICULAR: "Funicular. Any rail system designed for steep inclines."
}

ROUTE_TYPE_TO_SHORT_DESCRIPTION = {
    WALK: "Walk",
    TRAM: "Tram",
    SUBWAY: "Subway",
    RAIL: "Rail",
    BUS: "Bus",
    FERRY: "Ferry",
    CABLE_CAR: "Cable car",
    GONDOLA: "Gondola",
    FUNICULAR: "Funicular",
    TROLLEYBUS: "Trolleybus",
    MONORAIL: "Monorail",
    AIRCRAFT: "Aircraft"
}

ROUTE_TYPE_TO_LOWERCASE_TAG = {
    WALK: "walk",
    TRAM: "tram",
    SUBWAY: "subway",
    RAIL: "rail",
    BUS: "bus",
    FERRY: "ferry",
    CABLE_CAR: "cablecar",
    GONDOLA: "gondola",
    FUNICULAR: "funicular",
    TROLLEYBUS: "trolleybus",
    MONORAIL: "monorail"
}

# Use these on your own risk!
ROUTE_TYPE_TO_APPROXIMATE_CAPACITY = {
    WALK: None,
    TRAM: 200,
    SUBWAY: 600,
    RAIL: 600,
    BUS: 80,
    FERRY: 200,
    CABLE_CAR: 40,
    GONDOLA: 20,
    FUNICULAR: 20,
    TROLLEYBUS: 80,
    MONORAIL: 80
}

ROUTE_TYPE_TO_COLOR = {
    WALK: "#bdbdbd",
    TRAM: '#33a02c',
    SUBWAY: "#ff7f00",
    RAIL: '#e31a1c',
    BUS: '#1f78b4',
    FERRY: "#ffff99",
    CABLE_CAR: "#6a3d9a",
    GONDOLA: "#b15928",
    FUNICULAR: "#fb9a99",
    TROLLEYBUS: "#114569",
    MONORAIL: "#ba5d00",
    AIRCRAFT: "#fb9a99"
}


def route_type_to_color_iterable(route_type):
    return [ROUTE_TYPE_TO_COLOR[x] for x in route_type]


def route_type_to_zorder(route_type):
    return [ROUTE_TYPE_TO_ZORDER[x] for x in route_type]
