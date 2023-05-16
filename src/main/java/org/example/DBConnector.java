package org.example;
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
            String sql = "CREATE TABLE values (message_id int , sensorName varchar(255)  , value double precision , primary key (message_id, sensorName));" +
                    "CREATE TABLE model (message_id int , sensor_name varchar(255)  , position int , primary key (message_id, sensor_name));"+
                    "CREATE TABLE message ( message_type varchar(255) ," +
                    "message_id int primary key, " +
                    "stationName varchar(255)," +
                    "time_stamp TIMESTAMP," +
                    "acquisition_timestamp TIMESTAMP," +
                    "gps_timestamp TIMESTAMP," +
                    "latitude FLOAT(15), " +
                    "longitude FLOAT(15));" +
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
            String sql = //"DROP TABLE messages; " +
                    "DROP TABLE message;"
                   + "DROP TABLE values;"
                  +"DROP TABLE model ";
            stmt.executeUpdate(sql);
            System.out.print("DROP table in given database...");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void printTables(){
        String sql = "SELECT * FROM message_type";
        try (Statement stmt = conn.createStatement();
             ResultSet rs= stmt.executeQuery(sql)
        ) {
            while(rs.next()) {
                //Display values
                System.out.print("ID: " + rs.getString("type"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void insertValuesInValues(int message_id, String sensorName, double value) {


        try (
                PreparedStatement pstmt =
                        conn.prepareStatement("INSERT INTO values(message_id, sensorName, value) VALUES(?, ?, ?);");
        )
        {
            pstmt.setInt(1,message_id);
            pstmt.setString(2,sensorName);
            pstmt.setDouble(3,value);
            pstmt.executeUpdate();
            System.out.println(message_id);

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    public void insertValuesInMessage(Message.MessageType messageType, int messageId, String stationName, Timestamp timestamp, Timestamp acquisitionTimestamp, Timestamp gpsTimestamp, float latitude, float longitude, int i) {
        String sql ="INSERT INTO message(message_type, message_id,stationName,time_stamp, acquisition_timestamp, gps_timestamp, latitude, longitude) VALUES(?, ?, ?,?,?,?,?,?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);)
        {
            pstmt.setString(1,messageType.toString());
            pstmt.setInt(2,messageId);
            pstmt.setString(3,stationName);
            pstmt.setTimestamp(4,timestamp);
            pstmt.setTimestamp(5,acquisitionTimestamp);
            pstmt.setTimestamp(6,gpsTimestamp);
            pstmt.setFloat(7,latitude);
            pstmt.setFloat(8,longitude);
            pstmt.executeUpdate();
            System.out.print(stationName+ ""+ i);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void insertValuesInModelValues(int messageId, String sensorName, int position) {
        String sql = "INSERT INTO model(message_id, sensor_name, position) VALUES(?, ?, ?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);) {
            pstmt.setInt(1, messageId);
            pstmt.setString(2, sensorName);
            pstmt.setInt(3, position);
            pstmt.executeUpdate();
            System.out.print(messageId);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addToDatabase(Project p) {
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


