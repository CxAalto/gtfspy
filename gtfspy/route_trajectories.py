from geopandas import sjoin, GeoDataFrame
from pandas import DataFrame

from gtfspy.util import df_to_utm_gdf


def calculate_terminus_coords(self, trips):
    query = """
                WITH
                small_seq AS 
                (SELECT trip_I, min(seq) AS minseq from stop_times
                WHERE trip_I IN ({trips})
                GROUP BY trip_I),

                stop_coords AS
                (SELECT stop_times.trip_I, lat, lon FROM stop_times, stops, small_seq
                WHERE small_seq.trip_I = stop_times.trip_I AND minseq = stop_times.seq AND stop_times.stop_I = stops.stop_I),

                get_route AS
                (SELECT trip_I, name FROM trips, routes
                WHERE trips.route_I = routes.route_I),

                routecoords AS
                (SELECT name, lat, lon FROM stop_coords, get_route
                WHERE stop_coords.trip_I = get_route.trip_I
                GROUP BY name, lat, lon)

                SELECT group_concat(name) AS name, lat, lon FROM routecoords
                GROUP BY lat, lon
                """.format(trips=','.join([str(i) for i in trips]))
    df = self.execute_custom_query_pandas(query)
    gdf, _ = df_to_utm_gdf(df)

    return gdf


def calculate_trajectory_segments(self, day_start, start_time, end_time, trips=None, ignore_order_of_stop_ids=False):
    """
    returns the trajectory segments grouped either
    :param end_time:
    :param start_time:
    :param day_start:
    :param self:
    :param trips: list
    :param ignore_order_of_stop_ids: bool
    :return:
    """
    if isinstance(trips, GeoDataFrame) or isinstance(trips, DataFrame):
        trips = trips.tolist()
    if trips:
        trip_string = "WHERE trip_I in ({trips})".format(trips=",".join(trips))
    else:
        trip_string = ""
    if not trips:
        query = """SELECT trips.trip_I FROM days, trips
                    WHERE trips.trip_I = days.trip_I 
                    AND days.day_start_ut = {day_start} 
                    AND trips.start_time_ds >= {start_time} 
                    AND trips.start_time_ds < {end_time}""".format(day_start=day_start,
                                                                   start_time=start_time,
                                                                   end_time=end_time)
        trip_Is = self.execute_custom_query_pandas(query)
        trip_string = "WHERE trip_I in ({trips})".format(trips=",".join([str(x) for x in trip_Is["trip_I"].tolist()]))

    if ignore_order_of_stop_ids:
        query = """WITH 
                    stops_sections AS (SELECT start_stop.trip_I AS trip_I, 
                    min(start_stop.stop_I, end_stop.stop_I) AS min_stop, 
                    max(start_stop.stop_I, end_stop.stop_I) AS max_stop 
                    FROM
                    (SELECT trip_I, stop_I, seq  FROM stop_times) start_stop,
                    (SELECT trip_I, stop_I, seq  FROM stop_times) end_stop
                    WHERE start_stop.seq = end_stop.seq - 1 AND start_stop.trip_I = end_stop.trip_I)

                SELECT count(*) AS n_trips, min_stop, max_stop FROM stops_sections 
                {trip_string}
                GROUP BY min_stop, max_stop
                """.format(trip_string=trip_string)
    else:
        query = """WITH 
                    stops_sections AS (SELECT start_stop.trip_I AS trip_I, 
                    start_stop.stop_I AS min_stop, 
                    end_stop.stop_I AS max_stop 
                    FROM
                    (SELECT trip_I, stop_I, seq  FROM stop_times) start_stop,
                    (SELECT trip_I, stop_I, seq  FROM stop_times) end_stop
                    WHERE start_stop.seq = end_stop.seq - 1 AND start_stop.trip_I = end_stop.trip_I)


                SELECT count(*) AS n_trips, min_stop, max_stop FROM stops_sections 
                {trip_string}
                GROUP BY min_stop, max_stop
                """.format(trip_string=trip_string)

    df = self.execute_custom_query_pandas(query)

    df = self.add_coordinates_to_df(df, stop_id_column='min_stop', lat_name="from_lat", lon_name="from_lon")
    df = self.add_coordinates_to_df(df, stop_id_column='max_stop', lat_name="to_lat", lon_name="to_lon")

    gdf, crs = df_to_utm_gdf(df)

    gdf["coord_seq"] = gdf.apply(lambda row: [(row.from_lon, row.from_lat), (row.to_lon, row.to_lat)], axis=1)

    gdf["segment_kmt"] = gdf.apply(lambda row: row.geometry.length * row.n_trips, axis=1)

    return gdf


def calculate_route_trajectories(self, day_start, start_time, end_time):
    """

    :return: GeoDataFrame
    """
    query = """WITH 

                a AS (SELECT stop_I, stop_times.trip_I FROM stop_times
                WHERE stop_times.trip_I IN trips_of_the_day
                ORDER BY stop_times.trip_I, seq),

                q2 AS (SELECT name, trips.trip_I, group_concat(stop_I) AS trajectory FROM
                a, trips, routes
                WHERE trips.trip_I=a.trip_I AND trips.route_I=routes.route_I
                GROUP BY a.trip_I),

                stop_coords AS 
                (SELECT trip_I, lon ||' '|| lat AS coords FROM stop_times, stops
                WHERE stops.stop_I = stop_times.stop_I 
                ORDER BY trip_I, seq),

                linestring AS
                (SELECT trip_I, 'LINESTRING(' ||group_concat(coords)||')' AS wkt FROM stop_coords
                GROUP BY trip_I),

                trips_of_the_day AS 
                (SELECT trips.trip_I FROM days, trips
                WHERE trips.trip_I = days.trip_I 
                AND days.day_start_ut = {day_start} 
                AND trips.start_time_ds >= {start_time} 
                AND trips.start_time_ds < {end_time})

                SELECT group_concat(q2.trip_I) AS trip_Is, q2.trip_I AS trip_I, name, count(*) AS n_trips, wkt 
                FROM q2, linestring
                WHERE q2.trip_I = linestring.trip_I
                GROUP BY q2.trajectory 
                ORDER BY count(*) DESC""".format(day_start=day_start,
                                                 start_time=start_time,
                                                 end_time=end_time)
    df = self.execute_custom_query_pandas(query)
    if df.empty:
        raise ValueError
    route_trajectories, _ = df_to_utm_gdf(df)
    route_trajectories["segment_length"] = route_trajectories.apply(lambda row: row.geometry.length,
                                                                    axis=1)

    return route_trajectories


def calculate_hubs(self, hub_merge_distance, day_start, start_time, end_time):
    """
    get stop list,
    get list of stops with trip_I's
    get trip_I's within buffer
    group by trip_I's
    calculate n_trip_I's
    :param end_time:
    :param start_time:
    :param day_start:
    :param hub_merge_distance:
    :param self:
    :return:
    """

    # TODO: make the joins in the opposite direction so that buffers can be set based on mode
    stop_buffer_gdf, crs_utm = df_to_utm_gdf(self.stops())

    query = "SELECT stops.lat AS lat, stops.lon AS lon, stop_times.trip_I " \
            "FROM stops, stop_times, days " \
            "WHERE stops.stop_I = stop_times.stop_I " \
            "AND stop_times.trip_I = days.trip_I " \
            "AND days.day_start_ut = {day_start} " \
            "AND stop_times.dep_time_ds >= {start_time} " \
            "AND stop_times.dep_time_ds < {end_time}".format(day_start=day_start,
                                                             start_time=start_time,
                                                             end_time=end_time)
    trips_gdf, crs_utm = df_to_utm_gdf(self.execute_custom_query_pandas(query))
    trips_gdf = trips_gdf[["trip_I", "geometry"]]

    stop_buffer_gdf["buffer"] = stop_buffer_gdf["geometry"].buffer(hub_merge_distance / 2)
    stop_buffer_gdf = stop_buffer_gdf.set_geometry(stop_buffer_gdf["buffer"])

    gdf_joined = sjoin(trips_gdf, stop_buffer_gdf, how="left", op='within')
    # removes duplicate trip_I's
    gdf_grouped = gdf_joined.groupby(["stop_I", 'trip_I'])
    gdf_grouped = gdf_grouped.agg({'geometry': lambda x: x.iloc[0],
                                   'lat': lambda x: x.iloc[0],
                                   'lon': lambda x: x.iloc[0]}, axis=1)
    gdf_joined = gdf_grouped.reset_index()

    # calculate trip frequency within buffer
    gdf_grouped = gdf_joined.groupby(["stop_I"])
    gdf_grouped = gdf_grouped.agg({'trip_I': 'count',
                                   'geometry': lambda x: x.iloc[0],
                                   'lat': lambda x: x.iloc[0],
                                   'lon': lambda x: x.iloc[0]}, axis=1)

    gdf_joined = gdf_grouped.reset_index()
    gdf_joined.columns = ['stop_I', 'trips_in_area', 'geometry', 'lat', 'lon']

    hubs = GeoDataFrame(gdf_joined, crs=crs_utm, geometry=gdf_joined["geometry"])
    return hubs
