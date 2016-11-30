import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by rmkujala on 5/4/16.
 */

public class CacheRouterTest {
    private SqliteDistanceCache mockCache;
    private Router mockRouter;
    private FromToCoordinates ftc;
    private CacheRouter cacheRouter;

    @Before
    public void before() throws Exception {
        mockCache = mock(SqliteDistanceCache.class);
        mockRouter = mock(Router.class);
        ftc = new FromToCoordinates(1.0, 1.0, 1.00001, 1.00001);
    }

    @After
    public void after() throws Exception {
        if (mockRouter != null) {
            verifyNoMoreInteractions(mockRouter);
        }
        if (mockCache != null) {
            verifyNoMoreInteractions(mockCache);
        }

    }

    @Test
    public void testCacheRouterNoCacheAvail() {
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);

        when(mockRouter.route(ftc)).thenReturn(2);
        when(mockCache.getCachedDistance(ftc)).thenReturn(null);

        cacheRouter.setEnableUpdateCache(false);
        int distance = cacheRouter.route(ftc);

        assertEquals(distance, 2);

        verify(mockRouter).route(ftc);
        verify(mockCache).getCachedDistance(ftc);
    }

    @Test
    public void testCacheRouterCacheAvail() {
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);

        when(mockRouter.route(ftc)).thenReturn(2);
        when(mockCache.getCachedDistance(ftc)).thenReturn(3);

        boolean updateCache = false;
        cacheRouter.setEnableUpdateCache(updateCache);

        int distance = cacheRouter.route(ftc);
        assertEquals(distance, 3);

        verify(mockCache).getCachedDistance(ftc);
    }

    @Test
    public void testUpdateCache() {
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);

        when(mockRouter.route(ftc)).thenReturn(2);
        when(mockCache.getCachedDistance(ftc)).thenReturn(null);

        boolean updateCache = true;
        int distance = cacheRouter.route(ftc);

        assertEquals(distance, 2);

        verify(mockCache).getCachedDistance(ftc);
        verify(mockCache).updateCache(ftc, 2);
        verify(mockRouter).route(ftc);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCacheAndRouterIsNull() {
        mockRouter = null;
        mockCache = null;
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);
    }

    @Test
    public void testCacheIsNull() {
        CacheRouter cacheRouter = new CacheRouter(mockRouter, null);

        when(mockRouter.route(ftc)).thenReturn(2);
        int i = cacheRouter.route(ftc);

        assertEquals(2, i);

        verify(mockRouter).route(ftc);
    }

    @Test
    public void testRouterIsNullButFoundInCache() {
        mockRouter = null;
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);
        when(mockCache.getCachedDistance(ftc)).thenReturn(2);

        int i = cacheRouter.route(ftc);

        assertEquals(2, i);

        verify(mockCache).getCachedDistance(ftc);
    }


    @Test
    public void testResultIsNull() {
        mockRouter = null;
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);
        when(mockCache.getCachedDistance(ftc)).thenReturn(null);

        RuntimeException e = null;
        try {
            int i = cacheRouter.route(ftc);
        } catch (RuntimeException es) {
            e = es;
        }

        assertNotNull(e);

        verify(mockCache).getCachedDistance(ftc);
    }


    @Test
    public void testListRouting() {
        FromToCoordinates ftc2 = new FromToCoordinates(2, 2, 2, 2.0001);
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);
        ArrayList<FromToCoordinates> ftcList = new ArrayList<FromToCoordinates>();
        ftcList.add(ftc);
        ftcList.add(ftc2);

        when(mockCache.getCachedDistance(ftc)).thenReturn(null);
        when(mockRouter.route(ftc)).thenReturn(2);

        when(mockCache.getCachedDistance(ftc2)).thenReturn(4);

        ArrayList<Integer> distances = cacheRouter.route(ftcList);
        for (Integer d : distances ) {
            assertNotNull(d);
        }

        verify(mockCache).getCachedDistance(ftc);
        verify(mockCache).getCachedDistance(ftc2);
        verify(mockCache).updateCache(ftc, 2);
        verify(mockRouter).route(ftc);
        try {
            verify(mockCache).prepareForUpdates();
            verify(mockCache).commitUpdates();
        } catch (SQLException e) {
            assert false;
        }
    }

    @Test
    public void testListRouting2() {
        FromToCoordinates ftc2 = new FromToCoordinates(2, 2, 2, 2.0001);
        CacheRouter cacheRouter = new CacheRouter(mockRouter, mockCache);
        ArrayList<FromToCoordinates> ftcList = new ArrayList<FromToCoordinates>();
        int listSize = 5;
        for (int i= 0; i < listSize; i++) {
            ftcList.add(ftc);
        }

        when(mockRouter.route(ftc)).thenReturn(2);
        when(mockCache.getCachedDistance(ftc)).thenReturn(null);

        ArrayList<Integer> distances = cacheRouter.route(ftcList);

        for (Integer d : distances ) {
            assertNotNull(d);
        }

        verify(mockCache, times(listSize)).getCachedDistance(ftc);
        verify(mockRouter, times(listSize)).route(ftc);
        verify(mockCache, times(listSize)).updateCache(ftc, 2);

        try {
            verify(mockCache).prepareForUpdates();
            verify(mockCache).commitUpdates();
        } catch (SQLException e) {
            assert false;
        }
    }

}
