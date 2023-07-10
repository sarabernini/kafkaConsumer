package org.example;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


//classe che si connette al database ed esegue le query

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
    //metodo che esegue le query per la creazione delle tabelle nel database
    public void createTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE values (station_name varchar, time_stamp timestamp , sensorName varchar(255)  , value double precision , primary key (station_name, time_stamp, sensorName));" +
                    "CREATE TABLE model (station_name varchar, time_stamp timestamp , sensor_name varchar(255)  , position int , primary key (station_name, time_stamp, sensor_name));"+
                    "CREATE TABLE message ( message_type varchar(255) ," +
                    "message_id serial not null, " +
                    "stationName varchar," +
                    "time_stamp timestamp," +
                    "acquisition_timestamp timestamp," +
                    "gps_timestamp timestamp," +
                    "primary key (stationName, time_stamp));" +
                    "CREATE TABLE project ( project_name varchar(255)," +
                    "station_name varchar(255), primary key (project_name, station_name));" +
                    "CREATE TABLE station (station_name varchar primary key , latitude float(25), longitude float(25));";

            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo per l'inserimento dei valori passati in input nella tabella Values
    public void insertValues(String station_name, Timestamp time_stamp, String sensorName, double value) {
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
    //metodo per l'inserimento dei valori passati in input nella tabella Message
    public boolean insertMessage(Message.MessageType messageType, String stationName, Timestamp timestamp, Timestamp acquisitionTimestamp, Timestamp gpsTimestamp, float latitude, float longitude) {
        String sql ="INSERT INTO message(message_type,stationName,time_stamp, acquisition_timestamp, gps_timestamp) VALUES(?, ?,?,?,?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);)
        {
            pstmt.setString(1,messageType.toString());
            pstmt.setString(2,stationName);
            pstmt.setTimestamp(3,timestamp);
            pstmt.setTimestamp(4,acquisitionTimestamp);
            pstmt.setTimestamp(5,gpsTimestamp);
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            System.out.println("error in insertValuesInMessage");
            return false;
        }
    }
    //metodo per l'inserimento dei valori passati in input nella tabella Message
    public void insertModelValues(String station_name, Timestamp time_stamp, String sensorName, int position) {
        String sql = "INSERT INTO model(station_name, time_stamp, sensor_name, position) VALUES(?, ?, ?, ?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql);) {
            pstmt.setString(1, station_name);
            pstmt.setTimestamp(2, time_stamp);
            pstmt.setString(3, sensorName);
            pstmt.setInt(4, position);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //cancello tutti gli elementi dalla tabella project
    public void deleteProject() throws IOException {
        String query = "DELETE FROM project;";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che insierisce i valori presi in input nella tabella project
    public void insertProject(Project p) throws IOException {
        for(Station station: p.getProjectStations()) {
            String sql = "INSERT INTO project (project_name, station_name) VALUES (?, ?);";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, p.getProjectName());
                pstmt.setString(2, station.getName());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //metodo che inserisce i valori presi in input nella tabella Station
    public void insertStations(Project p){
        for(Station station: p.getProjectStations()) {
            String sql = "INSERT INTO station (station_name, latitude, longitude) VALUES (?, ?, ?);";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, station.getName());
                pstmt.setFloat(2, station.getLatitude());
                pstmt.setFloat(3, station.getLongitude());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //metodo che crea la media oraria dei valori dei sensori
    public void createAvg(){
        String query= "SELECT station_name, sensorname, DATE(time_stamp) as date, EXTRACT (HOUR FROM time_stamp) as hour, AVG(value)" +
                "       INTO average_values" +
                "       FROM values" +
                "       GROUP BY(station_name, sensorname, date, hour)";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che crea la tabella del meteo, del tipo dato in input
    public void createWeather(String typeOfData){
        try (Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE " + typeOfData + " (date timestamp, value double precision);";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che insirisce i valori presi in input nella tabella del meteo di un certo tipo dato in input
    public void insertWeather(ArrayList<SingleWeather> weatherList, String typeOfData){
        for(SingleWeather singleWeather: weatherList){
            String sql = "INSERT INTO "+ typeOfData +" (date, value) VALUES (?, ?);";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setTimestamp(1,singleWeather.getDate());
                pstmt.setDouble(2, singleWeather.getValue());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}


