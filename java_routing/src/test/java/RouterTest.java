import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RouterTest {

    private Router router;
    private FromToCoordinates ftc;

    @Before
    public void before() throws Exception {
        router = new Router("../../gtfspy/examples/data/kuopio_extract_mapzen_2017_03_15.osm.pbf", "/tmp/graphhopper_test/");
        router.setUp();
        ftc = new FromToCoordinates(62.895169, 27.682910, 62.895512, 27.676259);
    }

    @After
    public void after() throws Exception {

    }

    @Test
    public void testRouter() {
        int distance = router.route(ftc);
    }

}
