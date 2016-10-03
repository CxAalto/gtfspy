from gtfspy.routing.plots import plot_temporal_distance_variation
from gtfspy.routing.node_profile import NodeProfile
from gtfspy.routing.models import ParetoTuple

if __name__ == "__main__":
    profile = NodeProfile()
    profile.update_pareto_optimal_tuples(ParetoTuple(departure_time=2, arrival_time_target=11))
    profile.update_pareto_optimal_tuples(ParetoTuple(departure_time=20, arrival_time_target=25))
    profile.update_pareto_optimal_tuples(ParetoTuple(departure_time=40, arrival_time_target=45))
    assert(len(profile.get_pareto_tuples()) == 3)
    plot_temporal_distance_variation(profile, start_time=0, end_time=60, show=True)


