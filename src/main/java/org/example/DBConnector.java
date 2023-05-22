package org.example;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;



//classe che si connette al database e passa le informazioni

public class DBConnector {
    private Connection conn;
    private String url;
    private String user;
    private String pass;

    public DBConnector() throws SQLException {
        url = "jdbc:postgresql://136.243.101.139:5433/postgres";
        user = "postgres";
        pass = "***REDACTED***";
        this.conn = DriverManager.getConnection(url, user, pass);

    }

    public void createTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE values (station_name varchar, time_stamp timestamp , sensorName varchar(255)  , value double precision , primary key (station_name, time_stamp, sensorName));" +
                    "CREATE TABLE model (station_name varchar, time_stamp timestamp , sensor_name varchar(255)  , position int , primary key (station_name, time_stamp, sensor_name));"+
                    "CREATE TABLE message ( message_type varchar(255) ," +
                    "message_id int not null auto_increment, " +
                    "stationName varchar," +
                    "time_stamp timestamp," +
                    "acquisition_timestamp timestamp," +
                    "gps_timestamp timestamp," +
                    "latitude float(20), " +
                    "longitude float(20), " +
                    "primary key (stationName, time_stamp));" +
                    "CREATE TABLE project ( project_name varchar(255)," +
                    "station_name varchar(255), primary key (project_name, station_name));";

            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void joinTables(){
        try (Statement stmt = conn.createStatement()) {
            String sql ="CREATE TABLE messages AS (SELECT * FROM (values inner join message on (station = stationName)) " +
                    "INNER JOIN model as mod on(stationName = station_name )); ";
            stmt.executeUpdate(sql);
            System.out.println("join tables...");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    public void dropTables(){
        try (Statement stmt = conn.createStatement()) {
            //TODO: AGGIUNGI "SEI SICURTO?"CONTROLLO INPUT CONSOLE
            String sql = "DROP TABLE project; " +
                    "DROP TABLE message;"
                   + "DROP TABLE values;"
                  +"DROP TABLE model ";
            stmt.executeUpdate(sql);
            System.out.print("DROP table in given database...");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void insertValuesInValues(String station_name, Timestamp time_stamp, String sensorName, double value) {
        try (
                PreparedStatement pstmt =
                        conn.prepareStatement("INSERT INTO values(station_name, time_stamp, sensorName, value) VALUES(?,?, ?, ?);");
        )
        {
            pstmt.setString(1,station_name);
            pstmt.setTimestamp(2,time_stamp);
            pstmt.setString(3,sensorName);
            pstmt.setDouble(4,value);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("error in insertValuesInValues");
        }

    }
    public boolean insertValuesInMessage(Message.MessageType messageType, String stationName, Timestamp timestamp, Timestamp acquisitionTimestamp, Timestamp gpsTimestamp, float latitude, float longitude) {
        String sql ="INSERT INTO message(message_type,stationName,time_stamp, acquisition_timestamp, gps_timestamp, latitude, longitude) VALUES(?, ?,?,?,?,?,?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);)
        {
            pstmt.setString(1,messageType.toString());
            pstmt.setString(2,stationName);
            pstmt.setTimestamp(3,timestamp);
            pstmt.setTimestamp(4,acquisitionTimestamp);
            pstmt.setTimestamp(5,gpsTimestamp);
            pstmt.setFloat(6,latitude);
            pstmt.setFloat(7,longitude);
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("error in insertValuesInMessage");
            return false;
        }
    }
    public boolean insertValuesInModelValues(String station_name, Timestamp time_stamp, String sensorName, int position) {
        String sql = "INSERT INTO model(station_name, time_stamp, sensor_name, position) VALUES(?, ?, ?, ?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);) {
            pstmt.setString(1, station_name);
            pstmt.setTimestamp(2, time_stamp);
            pstmt.setString(3, sensorName);
            pstmt.setInt(4, position);
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("error in insertValuesInModelValues");
            e.getStackTrace();
            return false;
        }
    }
    //cancello tutti gli elementi dalla tabella project
    public void updateProject(Project p) throws IOException {
        p.updateProjects();
        String query = "DELETE FROM project;";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        insertProjectsInProject(p);
    }
    //inserisco gli elementi nella cartella project
    public void insertProjectsInProject(Project p) throws IOException {
        for(String name: p.getProjectStations()) {
            String sql = "INSERT INTO project (project_name, station_name) VALUES (?, ?);";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, p.getProjectName());
                pstmt.setString(2, name);
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}


