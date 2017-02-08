/**
 * Created by rmkujala on 4/21/16.
 */
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class GtfsSqliteDistancesReaderWriter {
    private final String stopDistancesTableName = "stop_distances";
    private final String stopsTable = "stops";
    private final String lat1ColName = "S1lat";
    private final String lon1ColName = "S1lon";
    private final String lat2ColName = "S2lat";
    private final String lon2ColName = "S2lon";
    private final String straightLineDistanceColName = "d";
    private final String osmDistanceColName = "d_walk";
    private final String fromStopColName = "from_stop_I";
    private final String toStopColName = "to_stop_I";
    private ArrayList<FromToCoordinates> fromToCoordinatesList = new ArrayList<>();
    private ArrayList<Double> origDistances = new ArrayList<>();
    private ArrayList<Integer> fromStopIList = new ArrayList<>();
    private ArrayList<Integer> toStopIList = new ArrayList<>();
    private boolean dataHasBeenRead = false;
    private String databaseFilename = null;

    public GtfsSqliteDistancesReaderWriter(String databaseFilename) {
        this.databaseFilename = databaseFilename;
    }

    /*
    Read data on stops into memory
     */
    public void readData() {
        if (this.dataHasBeenRead) {
            return;
        }
        Connection c;
        Statement stmt;
        // databaseFileName = "main.day.sqlite";
        try {
            c = DriverManager.getConnection("jdbc:sqlite:"+this.databaseFilename);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            String query = ("SELECT " +
                            this.fromStopColName + ", " +
                            this.toStopColName + ", " +
                            "S1.lat" + " as " + this.lat1ColName + ", " +
                            "S1.lon" + " as " + this.lon1ColName + ", " +
                            "S2.lat" + " as " + this.lat2ColName + ", " +
                            "S2.lon" + " as " + this.lon2ColName + ", " +
                            this.straightLineDistanceColName + " " +
                            "FROM " + this.stopDistancesTableName + " " +
                            "LEFT JOIN " + this.stopsTable + " S1 ON (from_stop_I=S1.stop_I) " +
                            "LEFT JOIN " + this.stopsTable + " S2 ON (to_stop_I  =S2.stop_I);" );
            ResultSet rs = stmt.executeQuery(query);
            System.out.println("Data load query executed, reading data now...");
            ResultSetMetaData rsmd = rs.getMetaData();
            // for (int i = 0; i < rsmd.getColumnCount(); i++) {
            //    System.out.println(rsmd.getColumnName(i+1));
            // }
            while (rs.next()) {
                double lat1 = rs.getDouble(lat1ColName);
                double lat2 = rs.getDouble(lat2ColName);
                double lon1 = rs.getDouble(lon1ColName);
                double lon2 = rs.getDouble(lon2ColName);
                FromToCoordinates ftc = new FromToCoordinates(lat1, lon1, lat2, lon2);
                this.fromToCoordinatesList.add(ftc);
                this.origDistances.add(rs.getDouble((straightLineDistanceColName)));
                this.fromStopIList.add(rs.getInt(fromStopColName));
                this.toStopIList.add(rs.getInt(toStopColName));
            }
            System.out.println("Read data");
            c.close();
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
        this.dataHasBeenRead = true;
    }

    public ArrayList<Double> getStraightLineDistances() {
        if (! this.dataHasBeenRead)
            this.readData();
        return this.origDistances;
    }

    public ArrayList<FromToCoordinates> getFromToCoords() {
        if (! this.dataHasBeenRead)
            this.readData();
        return this.fromToCoordinatesList;
    }

    public void writeRoutedDistances(List<Integer> routedDistances) {
        // data should have been read before writing (based on logic)!
        assert this.dataHasBeenRead;
        Connection c;
        Statement stmt;
        // databaseFileName = "main.day.sqlite";
        try {
            c = DriverManager.getConnection("jdbc:sqlite:" + this.databaseFilename);
            System.out.println("\nOpened GTFS database successfully");
            stmt = c.createStatement();
            System.out.println("Executing updates to the database...");
            c.setAutoCommit(false);
            for (int i= 0; i<routedDistances.size(); i++) {
                String distanceString = Double.toString(routedDistances.get(i));
                String fromStopI = Integer.toString(fromStopIList.get(i));
                String toStopI = Integer.toString(toStopIList.get(i));
                String update = String.format("UPDATE %s SET %s = %s WHERE %s=%s AND %s=%s;",
                        stopDistancesTableName, this.osmDistanceColName, distanceString,
                        fromStopColName, fromStopI, toStopColName, toStopI);
                // stmt.addBatch(update);
                stmt.executeUpdate(update);
            }
            c.commit();
            c.setAutoCommit(true);
            // stmt.executeBatch();
            c.close();
            System.out.println("Data successfully updated");
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
    }
}
