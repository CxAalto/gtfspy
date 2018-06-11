from example_import import load_or_import_example_gtfs
from matplotlib import pyplot as plt
from gtfspy.path_animator import PathAnimator
from gtfspy.smopy_plot_helper import using_smopy_map_style, MAP_STYLE_DARK_ALL

g = load_or_import_example_gtfs()
# g is now a gtfspy.gtfs.GTFS object

day_start_ut = g.get_weekly_extract_start_date(ut=True)
start_ut = day_start_ut + 8 * 3600
end_ut = day_start_ut + 10 * 3600

with using_smopy_map_style(MAP_STYLE_DARK_ALL):
    animator = PathAnimator.from_gtfs_db_trips(g, start_ut, end_ut)
    ani = animator.get_animation()
    ani.save("trips_anim_test_kuopio.mp4")
    plt.show()

