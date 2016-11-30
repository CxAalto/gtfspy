import com.graphhopper.reader.OSMWay;
import com.graphhopper.routing.util.FootFlagEncoder;

/**
 * Created by rmkujala on 4/20/16.
 */
public class NoFerriesFootFlagEncoder extends FootFlagEncoder {

    // Quick (and dirty?) way to ignore ferries:
    @Override
    public long acceptWay(OSMWay way) {
        String highwayValue = way.getTag("highway");
        if ((highwayValue == null) && (way.hasTag("route", ferries))) {
            return 0;
        }

        // If the way is not a ferry, use the default by GraphHopper
        return super.acceptWay(way);
    }
}