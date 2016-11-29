/**
 * Created by rmkujala on 5/3/16.
 */

import static org.junit.Assert.assertEquals;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;

public class SqliteDistanceCacheTest extends TestCase {
    private SqliteDistanceCache cache;
    private String cachePath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.cachePath = "/tmp/java_test_cache.sqlite";
        this.cache = new SqliteDistanceCache(cachePath);
    }

    @Override
    protected void tearDown() throws Exception {
        this.cache.close();
        File file = new File(this.cachePath);
        file.delete();
    }

    @Test
    public void testCache() {
        FromToCoordinates ftc = new FromToCoordinates(1.000, 1.000, 2.000, 2.000);
        int firstDistance = 1234;
        int secondDistance = 4321;

        Integer noDistance = cache.getCachedDistance(ftc);
        assertEquals("an empty cache should yield a null value", null, noDistance);

        boolean updated = cache.updateCache(ftc, firstDistance);
        assertEquals("cache should return true for a successful update", true, updated);

        int distance = cache.getCachedDistance(ftc);
        assertEquals("cache should return correct value after one update", firstDistance, distance);

        updated = cache.updateCache(ftc, secondDistance);
        assertEquals("cache should return true for a successful update even if a value existed", true, updated);

        distance = cache.getCachedDistance(ftc);
        assertEquals("cache should return correct value after two updates", secondDistance, distance);
    }

}
