import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by rmkujala on 5/3/16.
 */
public class CacheRouter {
    private SqliteDistanceCache cacheDB;
    private Router router;
    private boolean updateCache = true;
    public AtomicInteger nCacheHits = new AtomicInteger();

    public CacheRouter(Router router, SqliteDistanceCache cacheDB)
    {
        this.cacheDB = cacheDB;
        this.router = router;
        if (cacheDB == null && router == null) {
            throw new IllegalArgumentException("router and cacheDB can not both be null");
        }
    }

    public void setEnableUpdateCache(boolean updateCache) {
        this.updateCache = updateCache;
    }

    public ArrayList<Integer> route(List<FromToCoordinates> fromToCoordinatesList) {
        // try to find out if the result is found in cache:
        ArrayList<Integer> distances = new ArrayList<Integer>(fromToCoordinatesList.size());
        int i= 0;
        int onePercentOfsize = Math.max(1, fromToCoordinatesList.size()/100);
        boolean cacheDbPrepared = false;
        try {
            if (this.cacheDB != null) {
                this.cacheDB.prepareForUpdates();
                cacheDbPrepared = true;
            }
        } catch (SQLException e) {
            System.out.println("Commits to cache may not have succeeded");
        }
        System.out.println("Computing distances for " + String.valueOf(fromToCoordinatesList.size()) + " coordinate pairs");


        List<IntFtcTuple> intToFtc = new ArrayList<>(fromToCoordinatesList.size());
        for (int k = 0; k < fromToCoordinatesList.size(); k++) {
            intToFtc.add(new IntFtcTuple(k, fromToCoordinatesList.get(k)));
            distances.add(k, null);
        }
        assert distances.size() == fromToCoordinatesList.size();
        AtomicInteger progress = new AtomicInteger();
        intToFtc.parallelStream().forEach(
                itf -> {
                    distances.set(itf.getInt(), this.route(itf.getFromToCoordinates()));
                    int index = progress.incrementAndGet();
                    /* Some stuff for logging: */
                    if ((index % onePercentOfsize) == 0) {
                        if (index / onePercentOfsize == 0) {
                            System.out.println("Completed: ");
                        }
                        System.out.print(String.valueOf((index / onePercentOfsize)) + "% done, ");
                        System.out.println( this.nCacheHits.get() / (1.0 * index) + " of distances found from cache");
                        System.out.flush();
                    }
                }
        );

        try {
            if (this.cacheDB != null && cacheDbPrepared) {
                this.cacheDB.commitUpdates();
            }
        } catch (SQLException e) {
            System.out.println("Commits to cache may not have succeeded");
        }
        return distances;
    }

    public int route(FromToCoordinates fromToCoordinates) {
        Integer distance = null;
        if (cacheDB != null) {
            distance = cacheDB.getCachedDistance(fromToCoordinates);
            if (distance != null) {
                this.nCacheHits.incrementAndGet();
                return distance;
            }
        }

        if (distance == null && router != null) {
            distance = router.route(fromToCoordinates);
            if (updateCache && (cacheDB != null)) {
                cacheDB.updateCache(fromToCoordinates, distance);
            }
        }

        if (distance == null) {
            throw new RuntimeException("No value found for the given fromToCoordinates");
        }
        return distance;
    }



}
