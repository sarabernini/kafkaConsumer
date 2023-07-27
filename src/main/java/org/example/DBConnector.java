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
            String sql1= "select date(r.date) as day, extract( hour from r.date) as hour, avg(r.value) as rain, " +
                    "avg(t.value) as temperature, avg(w.value) as wind, r.location into weather_"+ location +
                    " from rain_"+ location +" as r left join temperature_"+ location +" as t on r.date = t.date  " +
                    " left join wind_"+ location +" w on t.date =w.date" +
                    " group by day, hour, r.location";
            stmt.executeUpdate(sql1);
            String sql2= "DROP TABLE rain_"+location+";" + "DROP TABLE wind_"+location+";" + "DROP TABLE temperature_"+location+";";
            stmt.executeUpdate(sql2);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che crea una tabella contenete per ogni stazione le medie orarie dei valori dei suoi sensori
    public void createStationsValues() {
        try (Statement stmt = conn.createStatement()) {
            String sql= "select m.stationname , DATE(m.acquisition_timestamp) as date,  (EXTRACT (HOUR FROM m.acquisition_timestamp))::int as hour , " +
                    "avg(v5.value) as co, avg(v2.value) as co2, avg(v3.value) as no2, avg(v4.value) as o3, avg(v1.value) as pm10, avg(v6.value) as pm25, avg(v7.value) as rh\n" +
                    "into station_values\n" +
                    "from (((((((message m join values v1 on m.stationname = v1.station_name and m.time_stamp = v1.time_stamp and v1.sensorname  = 'co') join \n" +
                    "values v2 on m.stationname = v2.station_name and m.time_stamp = v2.time_stamp and v2.sensorname  = 'co2') join \n" +
                    "values v3 on m.stationname = v3.station_name and m.time_stamp = v3.time_stamp  and v3.sensorname  = 'no2') join \n" +
                    "values v4 on m.stationname = v4.station_name and m.time_stamp = v4.time_stamp and v4.sensorname  = 'o3') join\n" +
                    "values v5 on m.stationname = v5.station_name and m.time_stamp = v5.time_stamp and v5.sensorname  = 'pm10') join \n" +
                    "values v6  on m.stationname = v6.station_name and m.time_stamp = v6.time_stamp and v6.sensorname  = 'pm25') join \n" +
                    "values v7 on m.stationname = v7.station_name and m.time_stamp = v7.time_stamp and v7.sensorname  = 'rh')\n" +
                    "where v5.value <900 and v6.value <900\n" +
                    "group by m.stationname , date, hour, v1.sensorname, v2.sensorname, v3.sensorname ,v4.sensorname ,v5.sensorname ,v6.sensorname, v7.sensorname ";
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    //metodo che unisce la tabella con i valori medi e quella con le temperature medie, restituendo i valori nella tabella dataset
    public void createTrainingDataset(Timestamp startDate, Timestamp endDate) {
        try (Statement stmt = conn.createStatement()) {
            String sql1= "select * into rh_values from(\n" +
                    "select date, hour, stationname, rh/10 as rh\n" +
                    "from station_values sv\n" +
                    "where stationname in (select stationname from station_values where rh>100 and rh<=1000 group by stationname)\n" +
                    "union \n" +
                    "select date, hour, stationname, rh/100 as rh\n" +
                    "from station_values sv2\n" +
                    "where stationname in (select stationname from station_values where rh>1000 group by stationname)\n" +
                    "union \n" +
                    "select date, hour, stationname, rh as rh\n" +
                    "from station_values sv2\n" +
                    "where stationname not in (select stationname from station_values where rh>100 group by stationname)) as r";
            stmt.executeUpdate(sql1);
            String sql2= "select sv.hour ,sv.co, sv.co2, sv.no2, sv.o3, sv.pm10, sv.pm25, r.rh,  \n" +
                    "(case when extract(isodow from sv.date)= 1 then 1 else 0 end)::integer as monday,\n" +
                    "(case when extract(isodow from sv.date)= 2 then 1 else 0 end)::integer as tuesday , \n" +
                    "(case when extract(isodow from sv.date)= 3 then 1 else 0 end)::integer as wednsday , \n" +
                    "(case when extract(isodow from sv.date)= 4 then 1 else 0 end)::integer as thursday ,\n" +
                    "(case when extract(isodow from sv.date)= 5 then 1 else 0 end)::integer as friday , \n" +
                    "(case when extract(isodow from sv.date)= 6 then 1 else 0 end)::integer as saturday , \n" +
                    "(case when extract(isodow from sv.date)= 7 then 1 else 0 end)::integer as sunday , \n" +
                    "w.rain, w.temperature, w.wind, (case when p.location='prato' then 1 else 0 end)::integer as Prato,\n" +
                    "(case when p.location='lucca' then 1 else 0 end)::integer as Lucca\n" +
                    "into training_dataset\n" +
                    "from (((station_values sv left join project p on sv.stationname = p.station_name)join rh_values r on sv.stationname = r.stationname and sv.date= r.date and sv.hour= r.hour) left join\n" +
                    "      (select * from weather_prato union select * from weather_lucca) as w on sv.date = w.day)\n" +
                    "where p.location = w.location  and sv.hour = w.hour and sv.date >= '"+startDate+"' and sv.date <= '"+endDate+"'";
            stmt.executeUpdate(sql2);
            String sql3= "DROP TABLE rh_values";
            stmt.executeUpdate(sql3);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDatasetToCompare(Timestamp endDate) {
        try (Statement stmt = conn.createStatement()) {
            String sql1= "select * into rh_values from(\n" +
                    "select date, hour, stationname, rh/10 as rh\n" +
                    "from station_values sv\n" +
                    "where stationname in (select stationname from station_values where rh>100 and rh<=1000 group by stationname)\n" +
                    "union \n" +
                    "select date, hour, stationname, rh/100 as rh\n" +
                    "from station_values sv2\n" +
                    "where stationname in (select stationname from station_values where rh>1000 group by stationname)\n" +
                    "union \n" +
                    "select date, hour, stationname, rh as rh\n" +
                    "from station_values sv2\n" +
                    "where stationname not in (select stationname from station_values where rh>100 group by stationname)) as r";
            stmt.executeUpdate(sql1);
            String sql2= "select sv.date, sv.hour ,avg(sv.co) as co, avg(sv.co2) as co2, avg(sv.no2) as no2, avg(sv.o3) as o3, avg(sv.pm10) AS pm10, avg(sv.pm25) as pm25, avg(r.rh) as rh, \n" +
                    "(case when extract(isodow from sv.date)= 1 then 1 else 0 end)::integer as monday,\n" +
                    "(case when extract(isodow from sv.date)= 2 then 1 else 0 end)::integer as tuesday , \n" +
                    "(case when extract(isodow from sv.date)= 3 then 1 else 0 end)::integer as wednsday , \n" +
                    "(case when extract(isodow from sv.date)= 4 then 1 else 0 end)::integer as thursday ,\n" +
                    "(case when extract(isodow from sv.date)= 5 then 1 else 0 end)::integer as friday , \n" +
                    "(case when extract(isodow from sv.date)= 6 then 1 else 0 end)::integer as saturday , \n" +
                    "(case when extract(isodow from sv.date)= 7 then 1 else 0 end)::integer as sunday , \n" +
                    "avg(w.rain) as rain, avg(w.temperature) as temperature, avg(w.wind) as wind, (case when p.location='prato' then 1 else 0 end)::integer as Prato,\n" +
                    "(case when p.location='lucca' then 1 else 0 end)::integer as Lucca\n" +
                    "into dataset_to_compare\n" +
                    "from (((station_values sv left join project p on sv.stationname = p.station_name)join rh_values r on sv.stationname = r.stationname and sv.date= r.date and sv.hour= r.hour) left join\n" +
                    "      (select * from weather_prato union select * from weather_lucca) as w on sv.date = w.day)\n" +
                    "where p.location = w.location  and sv.hour = w.hour and sv.date >'"+endDate+"'" +
                    "group by sv.date, sv.hour, monday, tuesday , wednsday ,thursday ,friday ,saturday ,sunday, prato, lucca ";
            stmt.executeUpdate(sql2);
            String sql3= "DROP TABLE rh_values";
            stmt.executeUpdate(sql3);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void training(String project_name, String task, String algorithm, String relation_name, String y_column_name, int n_estimators) {
        try (Statement stmt = conn.createStatement()) {
            String sql= "SELECT * FROM pgml.train(\n" +
                    "  project_name => '"+project_name+"', \n" +
                    "  task => '"+task+"', \n" +
                    "  algorithm => '"+algorithm+"',\n" +
                    "  relation_name => '"+relation_name+"', \n" +
                    "  y_column_name => '"+y_column_name+"',\n" +
                    "  preprocess => '{\n" +
                    "    \"wind\": {\"impute\": \"mean\" },\n" +
                    "   \"co2\": {\"impute\": \"mean\"},\n" +
                    "\"no2\": {\"impute\": \"mean\"},\n" +
                    "\"o3\": {\"impute\": \"mean\"},\n" +
                    "\"pm10\": {\"impute\": \"mean\"}\n" +
                    "}',\n" +
                    "  hyperparams => '{\n" +
                    "        \"n_estimators\":"+n_estimators+"\n" +
                    "    }'\n" +
                    ")";
            ResultSet resultSet= stmt.executeQuery(sql);
            while (resultSet.next()){
                System.out.println("project: "+resultSet.getString("project")+", task: "+ resultSet.getString("task")+", algorithm: "+ resultSet.getString("algorithm")+", deployed: " + resultSet.getBoolean("deployed"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public double predict(String project_name,String relation_name, String array) {
        try (Statement stmt = conn.createStatement()) {
            String sql= "select pgml.predict( " +
                    "'"+project_name+"', " +
                    "array"+array+") as prediction " +
                    "from "+relation_name+" LIMIT 1";
            ResultSet resultSet= stmt.executeQuery(sql);
            while (resultSet.next()){
                System.out.println("prediction: "+ resultSet.getDouble("prediction"));
                return resultSet.getDouble("prediction");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0.0;
    }
}


