import java.sql.*;

/**
 * Created by rmkujala on 5/3/16.
 */
public class SqliteDistanceCache {
    private String databaseFilename;
    private Connection connection;
    private String distanceTableName;
    private String walkColumnName;

    public SqliteDistanceCache(String databaseFilename) {
        this.databaseFilename = databaseFilename;
        this.distanceTableName = "stop_distances";
        this.walkColumnName = "d_walk";
        try {
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + this.databaseFilename);
            // Check that a stop_distances table exists
            // if not create it
            String tabledef = "CREATE table IF NOT EXISTS " + this.distanceTableName + " " +
                            "(lat1 REAL, lon1 REAL, lat2 REAL, lon2 REAL, " + walkColumnName + " INT)";
            Statement stmt = connection.createStatement();
            stmt.execute(tabledef);
            stmt = connection.createStatement();
            String indexdef= "CREATE UNIQUE INDEX IF NOT EXISTS coordinate_index ON " + this.distanceTableName + " " +
                    "(lat1 , lon1, lat2, lon2)";
            stmt.execute(indexdef);
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
    }

    public String getDbFname() {
        return databaseFilename;
    }

    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            return;
        }
    }

    public Integer getCachedDistance(FromToCoordinates ftc) {
        try {
            assert (!this.connection.isClosed());
            Double fromLat = ftc.getFromPoint().getLat();
            Double fromLon = ftc.getFromPoint().getLon();
            Double toLat = ftc.getToPoint().getLat();
            Double toLon = ftc.getToPoint().getLon();
            String query = "SELECT d_walk FROM " + this.distanceTableName + " " +
                    "WHERE" +
                    " (lat1=" + fromLat.toString() + ") AND " +
                    " (lon1=" + fromLon.toString() + ") AND " +
                    " (lat2=" + toLat.toString() + ") AND " +
                    " (lon2=" + toLon.toString() + ");";
            Statement stmt = this.connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getInt("d_walk");
            }
            return null; // otherwise
        } catch (Exception e) {
            return null;
        }
    }

    public void prepareForUpdates() throws SQLException {
        this.connection.setAutoCommit(false);
    }

    public void commitUpdates() throws SQLException {
        this.connection.commit();
        this.connection.setAutoCommit(true);
    }

    public boolean updateCache(FromToCoordinates ftc, int distance) {
        String distanceString = (new Integer(distance).toString());
        try {
            assert (!this.connection.isClosed());
            Double fromLat = ftc.getFromPoint().getLat();
            Double fromLon = ftc.getFromPoint().getLon();
            Double toLat = ftc.getToPoint().getLat();
            Double toLon = ftc.getToPoint().getLon();
            String query = "INSERT OR REPLACE INTO " + this.distanceTableName + " " +
                    " (lat1, lon1, lat2, lon2," + this.walkColumnName + ") VALUES (" +
                    fromLat.toString() + "," +
                    fromLon.toString() + "," +
                    toLat.toString() + "," +
                    toLon.toString() + "," +
                    distanceString + "); ";
            Statement stmt = this.connection.createStatement();
            int i = stmt.executeUpdate(query);
            if (i == 1) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

}
