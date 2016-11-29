import com.graphhopper.util.shapes.GHPlace;
import com.graphhopper.util.shapes.GHPoint;

/**
 * Created by rmkujala on 22.4.2016.
 */
public class FromToCoordinates {
    private double lat1;
    private double lon1;
    private double lat2;
    private double lon2;

    public enum PointTypeRouting {
        FROM, TO
    }


    public FromToCoordinates(double lat1, double lon1, double lat2, double lon2) {
        this.lat1 = lat1;
        this.lon1 = lon1;
        this.lat2 = lat2;
        this.lon2 = lon2;
    }

    public GHPoint getPoint(PointTypeRouting ptr) {
        switch (ptr) {
            case FROM:
                return getFromPoint();
            case TO:
                return getToPoint();
            default:
                return null;
        }
    }

    public GHPoint getFromPoint() {
        return new GHPoint(lat1, lon1);
    }

    public GHPoint getToPoint() {
        return new GHPoint(lat2, lon2);
    }
}
