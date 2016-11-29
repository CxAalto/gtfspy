import com.graphhopper.GHRequest;
import com.graphhopper.GHResponse;
import com.graphhopper.GraphHopper;
import com.graphhopper.PathWrapper;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.util.shapes.GHPoint;
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
        int distance = (new Double(path.getDistance())).intValue();
        return distance;
    }

}
