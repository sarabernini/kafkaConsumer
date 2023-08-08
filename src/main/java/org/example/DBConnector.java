package org.example;
import java.io.File;
import java.io.FileWriter;
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
    public void createTrainingDataset(boolean meteo) {
        try (Statement stmt = conn.createStatement()) {
            String meteo_string1= "";
            String meteo_string2 ="";
            String meteo_string3= "";
            String meteo_string4= "";
            String meteo_string5= "";
            if(meteo){
                meteo_string1 = "join weather_prato wp on wp.day= sv.date and wp.hour = sv.hour\n ";
                meteo_string2 = ", av.rain a_rain, av.temperature a_temperature, av.wind a_wind, av2.rain b_rain, av2.temperature b_temperature, av2.wind b_wind, av3.rain c_rain, av3.temperature c_temperature, av3.wind c_wind,av4.rain d_rain, av4.temperature d_temperature, av4.wind d_wind";
                meteo_string3 = ", av.rain, av.temperature, av.wind, av2.rain, av2.temperature , av2.wind , av3.rain , av3.temperature , av3.wind ,av4.rain , av4.temperature, av4.wind ";
                meteo_string4= "_meteo";
                meteo_string5= ", avg(wp.rain) as rain, avg(wp.temperature) as temperature, avg(wp.wind) as wind";
            }
            String sql1= "select \n" +
                    "sv. \"date\" ,(case \n" +
                    "when sv.hour >= 0 and sv.hour <=2 then 1 \n" +
                    "when sv.hour >= 3 and sv.hour <=5 then 2 \n" +
                    "when sv.hour >= 6 and sv.hour <=8 then 3  \n" +
                    "when sv.hour >= 9 and sv.hour <=11 then 4\n" +
                    "when sv.hour >= 12 and sv.hour <=14 then 5\n" +
                    "when sv.hour >= 15 and sv.hour <=17 then 6\n" +
                    "when sv.hour >= 18 and sv.hour <=20 then 7\n" +
                    "when sv.hour >= 21 and sv.hour <= 23 then 8\n" +
                    "end\n" +
                    ") as portion_of_day, \n" +
                    "(case  when sv.hour <12 then 0 else 1 end) as afternoon , avg(co) as co, avg(co2) as co2, avg(no2) as no2," +
                    " avg(o3) as o3, avg(pm10) as pm10, avg(pm25) as pm25, avg(rh) as rh " + meteo_string5+
                    " into average_values \n" +
                    "from station_values sv join project p on sv.stationname = p. station_name " + meteo_string1 +
                    "where p.location = 'prato'\n group by sv.\"date\" ,portion_of_day, afternoon";
           stmt.executeUpdate(sql1);
            String sql2= "select extract(isodow from av.date)::integer as day_of_week, av.afternoon, av.co as A_co, av.co2 A_co2, av.no2 A_no2, av.o3 A_o3, av.pm10 A_pm10, av.pm25 A_pm25, av.rh a_rh, \n" +
                    "av2.co as B_co, av2.co2 B_co2, av2.no2 B_no2, av2.o3 B_o3, av2.pm10 B_pm10, av2.pm25 B_pm25, av2.rh b_rh,\n" +
                    "av3.co as C_co, av3.co2 C_co2, av3.no2 C_no2, av3.o3 C_o3, av3.pm10 C_pm10, av3.pm25 C_pm25, av3.rh c_rh,\n" +
                    "av4.co as D_co, av4.co2 D_co2, av4.no2 D_no2, av4.o3 D_o3, av4.pm10 D_pm10, av4.pm25 D_pm25, av4.rh d_rh "+ meteo_string2+
                    ", max(av5.co) as max_co, max(av5.no2) as max_no2, max(av5.o3) as max_o3, max(av5.pm10) as max_pm10, max(av5.pm25) as max_pm25\n" +
                    "into training_dataset"+meteo_string4+"\n" +
                    "from average_values av join average_values av2 on av.\"date\" = av2.date and av.afternoon = av2.afternoon \n" +
                    "join average_values av3 on av.\"date\" = av3.date and av.afternoon = av3.afternoon\n" +
                    "join average_values av4 on av.\"date\" = av4.date and av.afternoon = av4.afternoon \n" +
                    "join average_values av5 on ((av.afternoon = 0 and av. \"date\" = av5.date and av5.afternoon= 1) or (av.afternoon = 1 and av5.date = av.date + interval '1 day' and av5.afternoon = 0))\n" +
                    "where (av.afternoon= 0 and av.portion_of_day= 1 and av2.portion_of_day = 2  and av3.portion_of_day = 3 and av4.portion_of_day = 4) or \n" +
                    "(av.afternoon= 1 and av.portion_of_day= 5 and av2.portion_of_day = 6  and av3.portion_of_day = 7 and av4.portion_of_day = 8)\n" +
                    "group by day_of_week , av.afternoon, av.co, av.co2, av.no2, av.o3, av.pm10, av.pm25, av.rh, \n" +
                    "av2.co, av2.co2, av2.no2, av2.o3, av2.pm10, av2.pm25, av2.rh, \n" +
                    "av3.co, av3.co2, av3.no2, av3.o3, av3.pm10, av3.pm25, av3.rh, \n" +
                    "av4.co, av4.co2, av4.no2, av4.o3, av4.pm10, av4.pm25, av4.rh"+meteo_string3;
            stmt.executeUpdate(sql2);
            String sql3= "DROP TABLE average_values";
            stmt.executeUpdate(sql3);
            System.out.println("Created table in given database...");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Double> readRealDataToCompare(boolean meteo){
        String realData= "3 0 11.7208951274651 459.9695564599634 195.25665856869978 377.369851362091 357.2632172625252 " +
                "5.2541834202374735 76.00419713384478 2.382675410914079 465.94768797569185 212.14096965977575 386.14326089731435 " +
                "355.84764555006024 5.836723619084574 77.80989610114037 13.845939845119723 451.2069249359331 210.0451416280353 431.76073301781264 " +
                "361.740231294194 6.813093634385008 64.59816251398853 13.02086069973809 425.47154527897226 196.6515127302326 418.1098660057433 " +
                "360.5488946308387 6.422355591938668 57.75658890863715 0.0 23.016666666666683 133.41666666666666 0.0 20.95538461538462 " +
                "108.2 0.0 23.91953125 142.79296875 0.0 28.519921875000012 88.859375 12.800331897164863 192.2116266918283 420.2561798442744 356.30872273916395 6.570110225569032 ";
        ArrayList<Double> result= new ArrayList<>();
        String[] rd= realData.split(" ");
        if(meteo){
           for(int i=0; i<42;i++){
               result.add(Double.parseDouble(rd[i]));
           }
        }else{
            for(int i=0; i<30;i++){
                result.add(Double.parseDouble(rd[i]));
            }
        }
        for(int i= 42;i<47;i++ ){
            result.add(Double.parseDouble(rd[i]));
        }
        return result;
    }


    public void training(String project_name, String task, String algorithm, String relation_name) {
        try (Statement stmt = conn.createStatement()) {
            String sql= "SELECT * FROM pgml.train_joint(\n" +
                    "'"+project_name+"',\n" +
                    "    task => '"+task+"',\n" +
                    "    relation_name => '"+relation_name+"',\n" +
                    "    algorithm => '"+algorithm+"',\n" +
                    "    preprocess => '{\n" +
                    "\t\t\"wind\": {\"impute\": \"mode\"}}',\n" +
                    "  \ty_column_name => ARRAY['max_co', 'max_no2', 'max_o3', 'max_pm10', 'max_pm25'],\n" +
                    "  \tsearch => 'grid', \n" +
                    "    search_params => '{\n" +
                    "        \"max_depth\": [ 4, 10, 20, 30] ,\n" +
                    "        \"n_estimators\": [20, 40, 80, 100],\n" +
                    "\t\t\"learning_rate\": [0.1,0.2, 0.3, 0.4],\n" +
                    "\t\t\"test_size\": [0.05, 0.10, 0.20, 0.25]\n" +
                    "   }'\n" +
                    ")";
            ResultSet resultSet= stmt.executeQuery(sql);
            while (resultSet.next()){
                System.out.println("project: "+resultSet.getString("project")+", task: "+ resultSet.getString("task")+", algorithm: "+ resultSet.getString("algorithm")+", deployed: " + resultSet.getBoolean("deployed"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void predict(String project_name,String relation_name, String array) throws IOException {
        FileWriter file = new FileWriter("predictedValue.txt");
        try (Statement stmt = conn.createStatement()) {
            String sql= "select pgml.predict_batch( " +
                    "'"+project_name+"', " +
                    "array_agg(array"+array+")) as prediction " +
                    "from "+relation_name+" LIMIT 5";
            ResultSet resultSet= stmt.executeQuery(sql);
            while(resultSet.next()){
                file.write(String.valueOf(resultSet.getDouble(1))+"\n");
            }
            file.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}


