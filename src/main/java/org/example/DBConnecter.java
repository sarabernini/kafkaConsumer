package org.example;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


//classe che si connette al database e passa le informazioni

public class DBConnecter {
    private Consumer consumer;
    private Connection conn;
    private String url;
    private String user;
    private String pass;

    public DBConnecter(Consumer consumer) throws SQLException {
        this.consumer = consumer;
        url = "jdbc:postgresql://136.243.101.139:5433/postgres";
        user = "postgres";
        pass = "***REDACTED***";
        this.conn = DriverManager.getConnection(url, user, pass);

    }

    public void createTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement();
        ) {
            String sql = "CREATE TABLE message_type (type varchar(255) primary key);" +
                    "CREATE TABLE values (station varchar(255) , sensorName varchar(255)  , value float(3) , primary key (station, sensorName));" +
                    "CREATE TABLE model (station_name varchar(255) , sensor_name varchar(255)  , position float(6) , primary key (station_name, sensor_name));"+
                    "CREATE TABLE message ( message_type varchar(255) ," +
                    "message_id int primary key, " +
                    "stationName varchar(255)," +
                    "time_stamp TIMESTAMP," +
                    "acquisition_timestamp TIMESTAMP," +
                    "gps_timestamp TIMESTAMP," +
                    "latitude FLOAT(10), " +
                    "longitude FLOAT(10));";

            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement();
        ) {
            String sql = "INSERT INTO message_type VALUES ('DATA');" +
                    "INSERT INTO message_type VALUES ('EXIT');" +
                    "INSERT INTO message_type VALUES ('STATUS');" +
                    "INSERT INTO message_type VALUES ('CONTROL');" +
                    "INSERT INTO message_type VALUES ('MODEL')";
            stmt.executeUpdate(sql);
            System.out.println("insert in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void joinTables(){
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement();

        ) {
            String sql ="CREATE TABLE messages AS (SELECT * FROM (values inner join message on (station = stationName)) " +
                    "INNER JOIN model as mod on(stationName = station_name )); ";
            stmt.executeUpdate(sql);
            System.out.println("join tables...");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
    public void dropTables(){
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement();
        ) {
            //TODO: AGGIUNGI "SEI SICURTO?"CONTROLLO INPUT CONSOLE
            String sql = //"DROP TABLE messages; " +
                    "DROP TABLE message_type;"
                    + "DROP TABLE message;"
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
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement();
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
}

