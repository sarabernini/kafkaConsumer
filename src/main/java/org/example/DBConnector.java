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

    //attributes
    private Connection conn;
    private String url;
    private String user;
    private String pass;

    //constructor
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
                    "station_name varchar(255),location varchar(255) primary key (project_name, station_name));" +
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
    //metodo che cancella tutti gli elementi dalla tabella project
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
            String sql = "INSERT INTO project (project_name, station_name, location) VALUES (?, ?,?);";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, p.getProjectName());
                pstmt.setString(2, station.getName());
                pstmt.setString(3, p.getLocation());
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
    //metodo che crea la tabella del meteo, del tipo dato in input
    public void createWeather(String typeOfData){
        try (Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE " + typeOfData + " (date timestamp, value double precision, location varchar(255), primary key (date));";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che insirisce i valori presi in input nella tabella del meteo di un certo tipo dato in input
    public void insertWeather(ArrayList<SingleWeather> weatherList, String typeOfData){
        for(SingleWeather singleWeather: weatherList){
            String sql = "INSERT INTO "+ typeOfData +" (date, value, location) VALUES (?, ?, ?);";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setTimestamp(1,singleWeather.getDate());
                pstmt.setDouble(2, singleWeather.getValue());
                pstmt.setString(3, singleWeather.getLocation());
                pstmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //metodo che unisce le tabelle meteo di una certa località e ne calcola le medie orarie
    public void calculateWeatherAverage(String location){
        try (Statement stmt = conn.createStatement()) {
            String sql= "select date(r.date) as day, extract( hour from r.date) as hour, avg(r.value) as rain, " +
                    "avg(t.value) as temparature, avg(w.value) as wind, r.location into weather_"+ location +
                    " from rain_"+ location +" as r left join temperature_"+ location +" as t on r.date = t.date  " +
                    " left join wind_"+ location +" w on t.date =w.date" +
                    " group by day, hour, r.location";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che crea una tabella contenete per ogni stazione le medie orarie dei valori dei suoi sensori
    public void createStationsValues() {
        try (Statement stmt = conn.createStatement()) {
            String sql= "select v1.station_name , DATE(m.acquisition_timestamp) as date,  EXTRACT (HOUR FROM m.acquisition_timestamp) as hour, avg(v1.value) as \"co\", avg(v2.value) as \"co2\",avg(v3.value) as \"no2\", avg(v4.value) as \"o3\", avg(v5.value) as \"pm10\", avg(v6.value) as \"pm25\", avg(case when extract(isodow from m.acquisition_timestamp)= 1 then 1 else 0 end) as monday, avg(case when extract(isodow from m.acquisition_timestamp)= 2 then 1 else 0 end) as tuesday,avg(case when extract(isodow from m.acquisition_timestamp)= 3 then 1 else 0 end) as wednesday, avg(case when extract(isodow from m.acquisition_timestamp)= 4 then 1 else 0 end) as thursday, avg(case when extract(isodow from m.acquisition_timestamp)= 5 then 1 else 0 end) as friday, avg(case when extract(isodow from m.acquisition_timestamp)= 6 then 1 else 0 end) as saturday, avg(case when extract(isodow from m.acquisition_timestamp)= 7 then 1 else 0 end) as sunday\n" +
                    "into station_values\n" +
                    "from (((((\"values\" v1 left join \"values\" v2 on v1.\"time_stamp\" = v2.\"time_stamp\" and v1.station_name= v2.station_name) left join \"values\" v3 on v1.\"time_stamp\" = v3.\"time_stamp\" and v1.station_name= v3.station_name) left join \"values\" v4 on v1.\"time_stamp\" = v4.\"time_stamp\" and v1.station_name= v4.station_name) left join \"values\" v5 on v1.\"time_stamp\" = v5.\"time_stamp\" and v1.station_name= v5.station_name) left join \"values\" v6 on v1.\"time_stamp\" = v6.\"time_stamp\" and v1.station_name= v6.station_name)join message m on v1.\"time_stamp\" = m.\"time_stamp\" \n" +
                    "where v1.sensorname  = 'co' and v2.sensorname  = 'co2' and v3.sensorname  = 'no2'  and v4.sensorname  = 'o3' and v5.sensorname  = 'pm10'  and v6.sensorname  = 'pm25'\n" +
                    "group by v1.sensorname, v2.sensorname, v3.sensorname ,v4.sensorname ,v5.sensorname ,v6.sensorname , v1.station_name, date, hour ";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che unisce la tabella con i valori medi e quella con le temperature medie, restituendo i valori nella tabella dataset
    public void createDataset() {
        try (Statement stmt = conn.createStatement()) {
            String sql= "select sv.\"date\" ,sv.\"hour\" ,sv.co, sv.co2, sv.no2, sv.o3, sv.pm10, sv.pm25, sv.monday , sv.tuesday , sv.wednesday , sv.thursday , sv.friday , sv.saturday , sv.sunday , w.rain, w.temperature, w.wind, (case when p.location=\'prato\' then 1 else 0 end) as Prato, (case when p.location=\'lucca\' then 1 else 0 end) as Lucca\n" +
                    "into dataset\n" +
                    "from ((station_values sv left join project p on sv.station_name = p.station_name)left join (select * from weather_prato union select * from weather_lucca) as w on sv.date = w.day)\n" +
                    "where p.\"location\" = w.\"location\"  and sv.\"hour\" = w.\"hour\"";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}


