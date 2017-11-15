import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.util.PointList;
import com.graphhopper.util.shapes.GHPoint;
import com.graphhopper.util.shapes.GHPoint3D;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.SystemUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.apache.commons.io.FilenameUtils.getBaseName;

/**
 * Created by rmkujala on 22.4.2016.
 */
public class Router {
    private String osmFileName = null;
    private GraphHopper hopper;
    private boolean isSetUp;
    private long setUpDuration;
    private String ghDir;

    public Router(String osmFileName, String ghDir) {
        this.isSetUp = false;
        this.osmFileName = osmFileName;
        this.setUpDuration = 0;
        this.ghDir = ghDir;
    }

    public long getSetupDuration() {
        return setUpDuration;
    }

    public void setUp() {
        if (this.isSetUp) {
            return;
        }

        System.out.println("Setting up Graphhopper");
        this.hopper = new GraphHopper().forServer();
        this.hopper.setOSMFile(osmFileName);
        // getBaseName is applied twice to remove both .osm and the .pbf endings

        // where to store graphhopper files?
        System.out.println("Setting up GraphHopper to " + this.ghDir + " ...");
        this.hopper.setEncodingManager(new EncodingManager(new NoFerriesFootFlagEncoder()));
        this.hopper.setGraphHopperLocation(this.ghDir);

        // now this can take minutes if it imports or a few seconds for loading
        // of course this is dependent on the area you import
        long startTime = System.currentTimeMillis();
        // disable fast mode, as CH does not bring benefits on short distances.
        this.hopper.setCHEnable(false);
        // this.hopper.clean(); // clean previous imports
        this.hopper.setEnableInstructions(false);
        this.hopper.setEnableCalcPoints(false);
        System.out.println("Importing or loading");
        this.hopper.importOrLoad();
        long endTime = System.currentTimeMillis();
        setUpDuration = endTime-startTime;
        this.isSetUp = true;
        System.out.println("Importing took " + String.valueOf(setUpDuration) + " milliseconds");
    }

    public ArrayList<Integer> route(List<FromToCoordinates> fromToCoordinatesList) {
        System.out.println("Making tons of requests...");
        ArrayList<Integer> routedDistances = new ArrayList<Integer>(fromToCoordinatesList.size());
        for (FromToCoordinates ftc : fromToCoordinatesList) {
            int distance = route(ftc);
            routedDistances.add(distance);
        }
        return routedDistances;
    }

    public Integer route(FromToCoordinates fromToCoordinates) {
        if (!this.isSetUp) {
            throw new IllegalStateException("Graphhopper should be set up before usage!");
        }
        GHPoint fromPoint = fromToCoordinates.getFromPoint();
        GHPoint toPoint = fromToCoordinates.getToPoint();
        GHRequest req = new GHRequest(fromPoint, toPoint).
                setWeighting("shortest").
                setVehicle("foot").
                setLocale(Locale.US).
                setAlgorithm(AlgorithmOptions.ASTAR);
        GHResponse rsp = hopper.route(req);
        // first check for errors
        if (rsp.hasErrors()) {
            // handle them!
            for (Throwable t : rsp.getErrors()) {
                System.out.println(t.getMessage());
            }
            return -1;
        }
        // use the best path, see the GHResponse class for more possibilities.
        PathWrapper path = rsp.getBest();
        // PointList pointList = path.getPoints();
        PointList points = path.getPoints();
        GHPoint3D pathStartPoint = points.toGHPoint(0);
        GHPoint3D pathEndPoint = points.toGHPoint(points.size() - 1);

        int distance = (new Double(path.getDistance())).intValue();
        distance += this.distance(fromPoint, pathStartPoint);
        distance += this.distance(pathEndPoint, toPoint);
        return distance;
    }

    private int distance(GHPoint start, GHPoint end) {
        double EARTH_RADIUS = 6378137.;
        double dLat = Math.toRadians(end.lat - start.lat);
        double dLon = Math.toRadians(end.lon- start.lon);
        double a = (Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(start.lat)) * Math.cos(Math.toRadians(end.lat)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2));
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double d = EARTH_RADIUS * c;
        return (new Double(d).intValue());
    }


}
