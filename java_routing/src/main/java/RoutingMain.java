import java.io.File;
import java.util.*;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import static org.apache.commons.io.FilenameUtils.getBaseName;

/**
 * Created by rmkujala on 4/20/16.
 */
public class RoutingMain {

    @Parameter(names={"--updateDb", "-u"}, description = "Path to the *.sqlite file to be updated")
    String dbToUpdatePath = "";

    @Parameter(names={"--osmFile", "-osm"}, description = "Path to the *.osm.pbf file")
    String osmFile = "";

    @Parameter(names={"--cache", "-c"}, description = "Path to the cache")
    String cachePath = null;

    @Parameter(names={"--tempDir", "-t"}, description = "Path to the directory where to store GraphHopper's internal databases")
    String tempDir = "/l/";

    @Parameter(names={"-n"}, description="Maximum number of routes to compute.")
    Integer nPairsToCompute = null;

    @Parameter(names = "--help", help=true)
    private boolean help;



    public static void main(String[] args) {
        RoutingMain main = new RoutingMain();
        JCommander jCommander = new JCommander(main, args);
        jCommander.setProgramName("RoutingMain");
        if (main.help) {
            jCommander.usage();
            return;
        }
        boolean printUsage = main.run();
        if (printUsage) {
            jCommander.usage();
        }
    }

    public RoutingMain() {
    }

    private boolean run() {
        File f = new File(dbToUpdatePath);
        GtfsSqliteDistancesReaderWriter toUpdateDb;

        if (f.exists() && !f.isDirectory()) {
            toUpdateDb = new GtfsSqliteDistancesReaderWriter(dbToUpdatePath);
        } else {
            System.out.println("'" +dbToUpdatePath +"'" + "is not valid path to a sqlite db");
            return true;
        }

        long startTime = System.currentTimeMillis();

        toUpdateDb.readData();
        List<FromToCoordinates> fromToCoordinatesList = toUpdateDb.getFromToCoords();
        int size = fromToCoordinatesList.size();
        if (nPairsToCompute != null && nPairsToCompute < size && nPairsToCompute >= 0) {
            fromToCoordinatesList = fromToCoordinatesList.subList(0, nPairsToCompute);
        }

        Router router;
        f = new File(osmFile);
        if (f.exists() && !f.isDirectory()) {
            File tempDir = new File(this.tempDir);
            if (tempDir.isDirectory()) {
                String osmBaseName = getBaseName(getBaseName(this.osmFile));
                String ghDir = this.tempDir + osmBaseName +"/";
                router = new Router(this.osmFile, ghDir);
                router.setUp();
            } else {
                System.out.println("Path to the OSM file was given but ...");
                System.out.println(this.tempDir + " is not a valid directory for storing GraphHopper data.");
                System.out.println("Specify a proper directory by the --tempdir or -t option.");
                return true;
            }
        } else {
            System.out.println("Warning: File " + osmFile + " was not found! \n" +
                    "No router in use -> routing may fail if results not found in cache!");
            router = null;
        }

        SqliteDistanceCache cache = null;
        if (cachePath != null) {
            try {
                cache = new SqliteDistanceCache(cachePath);
                System.out.println("Cache initialized:");
            } catch (Exception e) {
                System.out.println("Cache could not be initialized:");
                System.out.println(e.getMessage());
                cache = null;
            }

            if (cache == null && router == null) {
                System.out.println("Neither the path to the cache or the path to the osm file were valid");
                System.out.println("Exiting..");
                return false;
            }
        }

        CacheRouter cacheRouter = new CacheRouter(router, cache);

        long routingStartTime = System.currentTimeMillis();
        List<Integer> routedDistances = cacheRouter.route(fromToCoordinatesList);
        long routingEndTime = System.currentTimeMillis();
        long routingDuration = routingEndTime-routingStartTime;

        long writeOutStartTime = System.currentTimeMillis();
        toUpdateDb.writeRoutedDistances(routedDistances);
        long writeOutEndTime = System.currentTimeMillis();
        long writeOutDuration =  writeOutEndTime-writeOutStartTime;

        long endTime = System.currentTimeMillis();
        System.out.println(
                "The whole process took " + String.valueOf((endTime-startTime)/1000.) + " seconds");
        System.out.println("Setting up Graphhopper took " + String.valueOf(router.getSetupDuration()/1000.) +
                " seconds");
        System.out.println("Routing/fetching data from cache took " + String.valueOf(routingDuration/1000.) +
                " seconds");
        System.out.println("Writing out results took " + String.valueOf(writeOutDuration/1000.) + " seconds");
        System.out.println(String.format("In total there were %d results fetched from cache.", cacheRouter.nCacheHits));
        return false;
    }

}