package org.example;
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
        url = "";
        user = "";
        pass = "";
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
    public void createStationsValues(Boolean meteo) {
        try (Statement stmt = conn.createStatement()) {
            String sql = "select m.stationname , DATE(m.acquisition_timestamp) as date,  (EXTRACT (HOUR FROM m.acquisition_timestamp))::int as hour , " +
                    "avg(v1.value) as co, avg(v2.value) as co2, avg(v3.value) as no2, avg(v4.value) as o3, avg(v5.value) as pm10, avg(v6.value) as pm25, avg(v7.value) as rh\n" +
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
    public void createTrainingDataset(boolean meteo){

        String meteo_string1 = "";
        String meteo_string2 = "";
        String meteo_string3 = "";
        String meteo_string4 = "";
        String meteo_string5 = "";
        String meteo_string6 = "";

        if (meteo) {
            meteo_string1 = ", av.rain a_rain, av.temperature a_temperature, av.wind a_wind, av2.rain b_rain, av2.temperature b_temperature, av2.wind b_wind, av3.rain c_rain, av3.temperature c_temperature, av3.wind c_wind,av4.rain d_rain, av4.temperature d_temperature, av4.wind d_wind, av5.rain e_rain, av5.temperature e_temperature, av5.wind e_wind";
            meteo_string2 = ", av.rain, av.temperature, av.wind, av2.rain, av2.temperature , av2.wind , av3.rain , av3.temperature , av3.wind ,av4.rain , av4.temperature, av4.wind, av5.rain, av5.temperature, av5.wind";
            meteo_string3 = "_meteo";
            meteo_string4 = "join project p on sv.stationname = p. station_name  join weather_prato wp on wp.day= sv.date and wp.hour = sv.hour and wp.location =p.location \n";
            meteo_string5 = ", avg(wp.rain) as rain, avg(wp.temperature) as temperature, avg(wp.wind) as wind";
            meteo_string6 = "_meteo2";
        }

        try (Statement stmt = conn.createStatement()) {
            String sql1 = "select \n" +
                    "sv.date , sv.stationname, (case \n" +
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
                    " avg(o3) as o3, avg(pm10) as pm10, avg(pm25) as pm25, avg(rh) as rh " + meteo_string5 +
                    " into average_values"+meteo_string3+" \n" +
                    "from station_values sv " + meteo_string4 +
                    "group by sv.date , sv.stationname, portion_of_day, afternoon";
            //stmt.executeUpdate(sql1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Statement stmt = conn.createStatement()) {
            String sql = "select extract(isodow from av.date)::integer as day_of_week, av.afternoon, av.co as A_co, av.co2 A_co2, av.no2 A_no2, av.o3 A_o3, av.pm10 A_pm10, av.pm25 A_pm25, av.rh a_rh, \n" +
                    "av2.co as B_co, av2.co2 B_co2, av2.no2 B_no2, av2.o3 B_o3, av2.pm10 B_pm10, av2.pm25 B_pm25, av2.rh b_rh,\n" +
                    "av3.co as C_co, av3.co2 C_co2, av3.no2 C_no2, av3.o3 C_o3, av3.pm10 C_pm10, av3.pm25 C_pm25, av3.rh c_rh,\n" +
                    "av4.co as D_co, av4.co2 D_co2, av4.no2 D_no2, av4.o3 D_o3, av4.pm10 D_pm10, av4.pm25 D_pm25, av4.rh d_rh " + meteo_string1 +
                    ", max(av5.co) as max_co, max(av5.no2) as max_no2, max(av5.o3) as max_o3, max(av5.pm10) as max_pm10, max(av5.pm25) as max_pm25, " +
                    "avg(av5.co) as avg_co, avg(av5.no2) as avg_no2, avg(av5.o3) as avg_o3, avg(av5.pm10) as avg_pm10, avg(av5.pm25) as avg_pm25\n" +
                    "into training_dataset" + meteo_string6 + "\n" +
                    "from average_values"+meteo_string3+" av join average_values"+meteo_string3+" av2 on av.date = av2.date and av.afternoon = av2.afternoon and av.stationname = av2.stationname \n" +
                    "join average_values"+meteo_string3+" av3 on av.date = av3.date and av.afternoon = av3.afternoon and av.stationname = av3.stationname\n" +
                    "join average_values"+meteo_string3+" av4 on av.date = av4.date and av.afternoon = av4.afternoon and av.stationname = av4.stationname\n" +
                    "join average_values"+meteo_string3+"  av5 on ((av.afternoon = 0 and av.date = av5.date and av5.afternoon= 1) or (av.afternoon = 1 and av5.date = av.date + interval '1 day' and av5.afternoon = 0)) and av.stationname = av5.stationname \n" +
                    "where (av.afternoon= 0 and av.portion_of_day= 1 and av2.portion_of_day = 2  and av3.portion_of_day = 3 and av4.portion_of_day = 4) or \n" +
                    "(av.afternoon= 1 and av.portion_of_day= 5 and av2.portion_of_day = 6  and av3.portion_of_day = 7 and av4.portion_of_day = 8)\n" +
                    "group by day_of_week , av.afternoon, av.co, av.co2, av.no2, av.o3, av.pm10, av.pm25, av.rh, \n" +
                    "av2.co, av2.co2, av2.no2, av2.o3, av2.pm10, av2.pm25, av2.rh, \n" +
                    "av3.co, av3.co2, av3.no2, av3.o3, av3.pm10, av3.pm25, av3.rh, \n" +
                    "av4.co, av4.co2, av4.no2, av4.o3, av4.pm10, av4.pm25, av4.rh" + meteo_string2;
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void readRealDataToCompare(boolean meteo) throws IOException {
        ArrayList<Double> result= new ArrayList<>();
        int number_of_column;
        FileWriter file;
        String relation_name;
        try (Statement stmt = conn.createStatement()) {
            if(meteo){
                relation_name= "training_dataset_meteo2";
                file = new FileWriter("realValueMeteo2.txt");
                number_of_column= 55;
            }else{
                relation_name= "training_dataset";
                file = new FileWriter("realValue.txt");
                number_of_column= 40;
            }
            String sql= "SELECT * FROM "+relation_name+" order by random() LIMIT 10";
            ResultSet resultSet= stmt.executeQuery(sql);
            while (resultSet.next()){
                for(int i=1; i<number_of_column+1;i++){
                    result.add(resultSet.getDouble(i));
                    file.write(String.valueOf(resultSet.getDouble(i))+"\n");
                }
            }
            file.close();
            for(int i= 0; i<10; i++){
                String sql2 = "DELETE FROM "+relation_name+" WHERE day_of_week = "+ result.get(0+number_of_column*i)+
                        " and afternoon = "+result.get(1+number_of_column*i)+" and a_co= " + result.get(2+number_of_column*i)+ " and a_co2= "
                        + result.get(3+number_of_column*i)+ " and a_no2 = "+ result.get(4+number_of_column*i)+" and a_o3 = "+ result.get(5+number_of_column*i)+" ";
                stmt.executeUpdate(sql2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void training(String project_name, String task, String algorithm, String relation_name, String element_to_predict) {
        try (Statement stmt = conn.createStatement()) {
            String sql= "SELECT * FROM pgml.train_joint(\n" +
                    "'"+project_name+"',\n" +
                    "    task => '"+task+"',\n" +
                    "    relation_name => '"+relation_name+"',\n" +
                    "    algorithm => '"+algorithm+"',\n" +
                    "    preprocess => '{\n" +
                    "\t\t\"wind\": {\"impute\": \"mode\"}}',\n" +
                    "  \ty_column_name => ARRAY["+element_to_predict+"],\n" +
                    "  \tsearch => 'grid', \n" +
                    "    search_params => '{\n" +
                    "        \"max_depth\": [ 5, 10, 15, 20] ,\n" + // \"max_depth\": [ 5, 10, 20, 30] ,\n" +
                    "        \"n_estimators\": [50,150,250, 350],\n" + // \"n_estimators\": [40, 60, 80, 100, 150, 200],\n" +
                    "         \"learning_rate\": [0.01, 0.05, 0.1, 0.2, 0.3],\n" + // "  \"learning_rate\": [0.01, 0.05, 0.1, 0.2, 0,3],\n" +
                    "           \"test_size\": [0.025, 0.05. 0.10]\n" + // "  \"test_size\": [0.025, 0.05, 0.10]\n" +
                    "   }'\n" +
                    ")";

            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()){
                System.out.println("project: "+resultSet.getString("project")+", task: "+ resultSet.getString("task")+", algorithm: "+ resultSet.getString("algorithm")+", deployed: " + resultSet.getBoolean("deployed"));
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void predict(String project_name, String array) throws IOException {
        FileWriter file = new FileWriter("predictedValue.txt");
        try (Statement stmt = conn.createStatement()) {
            String sql= "select pgml.predict_batch( " +
                    "'"+project_name+"', " +
                    "array"+array+") as prediction " +
                    "LIMIT 10";
            ResultSet resultSet= stmt.executeQuery(sql);
            while(resultSet.next()){
                file.write(String.valueOf(resultSet.getDouble(1))+"\n");
            }
            file.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createDailyDataset(int i) {
        try (Statement stmt = conn.createStatement()) {
            String sql= "select extract(isodow from av.date)::integer as day_of_week, av.co as A_co, av.co2 A_co2, av.no2 A_no2, av.o3 A_o3, av.pm10 A_pm10, av.pm25 A_pm25, av.rh A_rh, " +
                    "av2.co as B_co, av2.co2 B_co2, av2.no2 B_no2, av2.o3 B_o3, av2.pm10 B_pm10, av2.pm25 B_pm25, av2.rh B_rh, " +
                    "av3.co as C_co, av3.co2 C_co2, av3.no2 C_no2, av3.o3 C_o3, av3.pm10 C_pm10, av3.pm25 C_pm25, av3.rh C_rh, " +
                    "av4.co as D_co, av4.co2 D_co2, av4.no2 D_no2, av4.o3 D_o3, av4.pm10 D_pm10, av4.pm25 D_pm25, av4.rh D_rh, "+
                    "av5.co as E_co, av5.co2 E_co2, av5.no2 E_no2, av5.o3 E_o3, av5.pm10 E_pm10, av5.pm25 E_pm25, av5.rh E_rh, "+
                    "av6.co as F_co, av6.co2 F_co2, av6.no2 F_no2, av6.o3 F_o3, av6.pm10 F_pm10, av6.pm25 F_pm25, av6.rh F_rh, "+
                    "av7.co as G_co, av7.co2 G_co2, av7.no2 G_no2, av7.o3 G_o3, av7.pm10 G_pm10, av7.pm25 G_pm25, av7.rh G_rh, "+
                    "av8.co as H_co, av8.co2 H_co2, av8.no2 H_no2, av8.o3 H_o3, av8.pm10 H_pm10, av8.pm25 H_pm25, av8.rh H_rh, "+
                    "max(av9.co) as max_co, max(av9.no2) as max_no2, max(av9.o3) as max_o3, max(av9.pm10) as max_pm10, max(av9.pm25) as max_pm25, " +
                    "avg(av9.co) as avg_co, avg(av9.no2) as avg_no2, avg(av9.o3) as avg_o3, avg(av9.pm10) as avg_pm10, avg(av9.pm25) as avg_pm25\n" +
                    "into training_daily_dataset_"+i+
                    "\n from average_values av join average_values av2 on av.date = av2.date and av.stationname = av2.stationname \n" +
                    "join average_values av3 on av.date = av3.date  and av.stationname = av3.stationname\n" +
                    "join average_values av4 on av.date = av4.date  and av.stationname = av4.stationname\n" +
                    "join average_values av5 on av.date = av5.date  and av.stationname = av5.stationname\n" +
                    "join average_values av6 on av.date = av6.date  and av.stationname = av6.stationname\n" +
                    "join average_values av7 on av.date = av7.date  and av.stationname = av7.stationname\n" +
                    "join average_values av8 on av.date = av8.date  and av.stationname = av8.stationname\n" +
                    "join average_values  av9 on av9.date = av.date + interval '"+i+" day' and av.stationname = av9.stationname \n" +
                    "where av.portion_of_day = 1 and av2.portion_of_day = 2  and av3.portion_of_day = 3 and av4.portion_of_day = 4 " +
                    "and av5.portion_of_day = 5 and av6.portion_of_day = 6 and av7.portion_of_day = 7 and av8.portion_of_day = 8 " +
                    "group by day_of_week ,av.co, av.co2, av.no2, av.o3, av.pm10, av.pm25, av.rh, \n" +
                    "av2.co, av2.co2, av2.no2, av2.o3, av2.pm10, av2.pm25, av2.rh, \n" +
                    "av3.co, av3.co2, av3.no2, av3.o3, av3.pm10, av3.pm25, av3.rh, \n" +
                    "av4.co, av4.co2, av4.no2, av4.o3, av4.pm10, av4.pm25, av4.rh, "+
                    "av5.co, av5.co2, av5.no2, av5.o3, av5.pm10, av5.pm25, av5.rh, "+
                    "av6.co, av6.co2, av6.no2, av6.o3, av6.pm10, av6.pm25, av6.rh, "+
                    "av7.co, av7.co2, av7.no2, av7.o3, av7.pm10, av7.pm25, av7.rh, "+
                    "av8.co, av8.co2, av8.no2, av8.o3, av8.pm10, av8.pm25, av8.rh";
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void readDataToCompare() throws IOException {
        /*per leggere dati nel caso 9 cambiare realValueMeteo.txt in realValueMeteo2.txt, commentare string meteo e meteo1
        prive di contenuto e modificare il numero di iterazioni del ciclo for da 53 a 56*/

        FileWriter realValue = new FileWriter("realValue.txt");
        FileWriter realValueMeteo = new FileWriter("realValueMeteo.txt");
        //String meteo="av5.rain e_rain, av5.temperature e_temperature, av5.wind e_wind,";
        //String meteo1=", av5.rain, av5.temperature, av5.wind";
        String meteo="";
        String meteo1="";
        try (Statement stmt = conn.createStatement()) {
            String sql = "select extract(isodow from av.date)::integer as day_of_week, av.afternoon, av.co as A_co, av.co2 A_co2, av.no2 A_no2, av.o3 A_o3, av.pm10 A_pm10, av.pm25 A_pm25, av.rh a_rh,\n" +
                    "av2.co as B_co, av2.co2 B_co2, av2.no2 B_no2, av2.o3 B_o3, av2.pm10 B_pm10, av2.pm25 B_pm25, av2.rh b_rh,\n" +
                    "av3.co as C_co, av3.co2 C_co2, av3.no2 C_no2, av3.o3 C_o3, av3.pm10 C_pm10, av3.pm25 C_pm25, av3.rh c_rh,\n" +
                    "av4.co as D_co, av4.co2 D_co2, av4.no2 D_no2, av4.o3 D_o3, av4.pm10 D_pm10, av4.pm25 D_pm25, av4.rh d_rh , \n" +
                    "av.rain a_rain, av.temperature a_temperature, av.wind a_wind, av2.rain b_rain, av2.temperature b_temperature, av2.wind b_wind, av3.rain c_rain, av3.temperature c_temperature, av3.wind c_wind,av4.rain d_rain, av4.temperature d_temperature, av4.wind d_wind, " +meteo+
                    "max(av5.co) as max_co, max(av5.no2) as max_no2, max(av5.o3) as max_o3, max(av5.pm10) as max_pm10, max(av5.pm25) as max_pm25,\n" +
                    "avg(av5.co) as avg_co, avg(av5.no2) as avg_no2, avg(av5.o3) as avg_o3, avg(av5.pm10) as avg_pm10, avg(av5.pm25) as avg_pm25\n" +
                    "from testing_dataset av join testing_dataset av2 on av.date = av2.date and av.afternoon = av2.afternoon and av.stationname = av2.stationname\n" +
                    "join testing_dataset av3 on av.date = av3.date and av.afternoon = av3.afternoon and av.stationname = av3.stationname\n" +
                    "join testing_dataset av4 on av.date = av4.date and av.afternoon = av4.afternoon and av.stationname = av4.stationname\n" +
                    "join testing_dataset  av5 on ((av.afternoon = 0 and av.date = av5.date and av5.afternoon= 1) or (av.afternoon = 1 and av5.date = av.date + interval '1 day' and av5.afternoon = 0)) and av.stationname = av5.stationname\n" +
                    "where av.stationname= 'SMART59' and((av.afternoon= 0 and av.portion_of_day= 1 and av2.portion_of_day = 2  and av3.portion_of_day = 3 and av4.portion_of_day = 4) or \n" +
                    "(av.afternoon= 1 and av.portion_of_day= 5 and av2.portion_of_day = 6  and av3.portion_of_day = 7 and av4.portion_of_day = 8))\n" +
                    "group by day_of_week , av.afternoon, av.co, av.co2, av.no2, av.o3, av.pm10, av.pm25, av.rh,\n" +
                    "av2.co, av2.co2, av2.no2, av2.o3, av2.pm10, av2.pm25, av2.rh,\n" +
                    "av3.co, av3.co2, av3.no2, av3.o3, av3.pm10, av3.pm25, av3.rh,\n" +
                    "av4.co, av4.co2, av4.no2, av4.o3, av4.pm10, av4.pm25, av4.rh , \n" +
                    "av.rain, av.temperature, av.wind,\n" +
                    "av2.rain, av2.temperature , av2.wind , \n" +
                    "av3.rain , av3.temperature , av3.wind ,\n" +
                    "av4.rain , av4.temperature, av4.wind"+meteo1;
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                for (int i = 1; i < 53; i++) {
                    realValueMeteo.write(resultSet.getDouble(i) + "\n");
                }
            }
            realValueMeteo.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Statement stmt = conn.createStatement()) {
            String sql = "select extract(isodow from av.date)::integer as day_of_week, av.afternoon, av.co as A_co, av.co2 A_co2, av.no2 A_no2, av.o3 A_o3, av.pm10 A_pm10, av.pm25 A_pm25, av.rh a_rh,\n" +
                    "av2.co as B_co, av2.co2 B_co2, av2.no2 B_no2, av2.o3 B_o3, av2.pm10 B_pm10, av2.pm25 B_pm25, av2.rh b_rh,\n" +
                    "av3.co as C_co, av3.co2 C_co2, av3.no2 C_no2, av3.o3 C_o3, av3.pm10 C_pm10, av3.pm25 C_pm25, av3.rh c_rh,\n" +
                    "av4.co as D_co, av4.co2 D_co2, av4.no2 D_no2, av4.o3 D_o3, av4.pm10 D_pm10, av4.pm25 D_pm25, av4.rh d_rh , \n" +
                    "max(av5.co) as max_co, max(av5.no2) as max_no2, max(av5.o3) as max_o3, max(av5.pm10) as max_pm10, max(av5.pm25) as max_pm25,\n" +
                    "avg(av5.co) as avg_co, avg(av5.no2) as avg_no2, avg(av5.o3) as avg_o3, avg(av5.pm10) as avg_pm10, avg(av5.pm25) as avg_pm25\n" +
                    "from testing_dataset av join testing_dataset av2 on av.date = av2.date and av.afternoon = av2.afternoon and av.stationname = av2.stationname\n" +
                    "join testing_dataset av3 on av.date = av3.date and av.afternoon = av3.afternoon and av.stationname = av3.stationname\n" +
                    "join testing_dataset av4 on av.date = av4.date and av.afternoon = av4.afternoon and av.stationname = av4.stationname\n" +
                    "join testing_dataset  av5 on ((av.afternoon = 0 and av.date = av5.date and av5.afternoon= 1) or (av.afternoon = 1 and av5.date = av.date + interval '1 day' and av5.afternoon = 0)) and av.stationname = av5.stationname\n" +
                    "where av.stationname= 'SMART59' and((av.afternoon= 0 and av.portion_of_day= 1 and av2.portion_of_day = 2  and av3.portion_of_day = 3 and av4.portion_of_day = 4) or \n" +
                    "(av.afternoon= 1 and av.portion_of_day= 5 and av2.portion_of_day = 6  and av3.portion_of_day = 7 and av4.portion_of_day = 8))\n" +
                    "group by day_of_week , av.afternoon, av.co, av.co2, av.no2, av.o3, av.pm10, av.pm25, av.rh,\n" +
                    "av2.co, av2.co2, av2.no2, av2.o3, av2.pm10, av2.pm25, av2.rh,\n" +
                    "av3.co, av3.co2, av3.no2, av3.o3, av3.pm10, av3.pm25, av3.rh,\n" +
                    "av4.co, av4.co2, av4.no2, av4.o3, av4.pm10, av4.pm25, av4.rh \n";
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                for (int i = 1; i < 41; i++) {
                    realValue.write(resultSet.getDouble(i) + "\n");
                }
            }
            realValue.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        for (int i = 1; i <=5 ; i++) {
            if(i != 4) {
                FileWriter file = new FileWriter("realValue" + i + "Day.txt");
                try (Statement stmt = conn.createStatement()) {
                    String sql = "select extract(isodow from av.date)::integer as day_of_week, av.co as A_co, av.co2 A_co2, av.no2 A_no2, av.o3 A_o3, av.pm10 A_pm10, av.pm25 A_pm25, av.rh A_rh, " +
                            "av2.co as B_co, av2.co2 B_co2, av2.no2 B_no2, av2.o3 B_o3, av2.pm10 B_pm10, av2.pm25 B_pm25, av2.rh B_rh, " +
                            "av3.co as C_co, av3.co2 C_co2, av3.no2 C_no2, av3.o3 C_o3, av3.pm10 C_pm10, av3.pm25 C_pm25, av3.rh C_rh, " +
                            "av4.co as D_co, av4.co2 D_co2, av4.no2 D_no2, av4.o3 D_o3, av4.pm10 D_pm10, av4.pm25 D_pm25, av4.rh D_rh, " +
                            "av5.co as E_co, av5.co2 E_co2, av5.no2 E_no2, av5.o3 E_o3, av5.pm10 E_pm10, av5.pm25 E_pm25, av5.rh E_rh, " +
                            "av6.co as F_co, av6.co2 F_co2, av6.no2 F_no2, av6.o3 F_o3, av6.pm10 F_pm10, av6.pm25 F_pm25, av6.rh F_rh, " +
                            "av7.co as G_co, av7.co2 G_co2, av7.no2 G_no2, av7.o3 G_o3, av7.pm10 G_pm10, av7.pm25 G_pm25, av7.rh G_rh, " +
                            "av8.co as H_co, av8.co2 H_co2, av8.no2 H_no2, av8.o3 H_o3, av8.pm10 H_pm10, av8.pm25 H_pm25, av8.rh H_rh, " +
                            "max(av9.co) as max_co, max(av9.no2) as max_no2, max(av9.o3) as max_o3, max(av9.pm10) as max_pm10, max(av9.pm25) as max_pm25, " +
                            "avg(av9.co) as avg_co, avg(av9.no2) as avg_no2, avg(av9.o3) as avg_o3, avg(av9.pm10) as avg_pm10, avg(av9.pm25) as avg_pm25\n" +
                            "from testing_dataset av join testing_dataset av2 on av.date = av2.date and av.stationname = av2.stationname \n" +
                            "join testing_dataset av3 on av.date = av3.date  and av.stationname = av3.stationname\n" +
                            "join testing_dataset av4 on av.date = av4.date  and av.stationname = av4.stationname\n" +
                            "join testing_dataset av5 on av.date = av5.date  and av.stationname = av5.stationname\n" +
                            "join testing_dataset av6 on av.date = av6.date  and av.stationname = av6.stationname\n" +
                            "join testing_dataset av7 on av.date = av7.date  and av.stationname = av7.stationname\n" +
                            "join testing_dataset av8 on av.date = av8.date  and av.stationname = av8.stationname\n" +
                            "join testing_dataset  av9 on av9.date = av.date + interval '" + i + " day' and av.stationname = av9.stationname \n" +
                            "where av.stationname= 'SMART59' and(av.portion_of_day = 1 and av2.portion_of_day = 2  and av3.portion_of_day = 3 and av4.portion_of_day = 4 " +
                            "and av5.portion_of_day = 5 and av6.portion_of_day = 6 and av7.portion_of_day = 7 and av8.portion_of_day = 8) " +
                            "group by day_of_week ,av.co, av.co2, av.no2, av.o3, av.pm10, av.pm25, av.rh, \n" +
                            "av2.co, av2.co2, av2.no2, av2.o3, av2.pm10, av2.pm25, av2.rh, \n" +
                            "av3.co, av3.co2, av3.no2, av3.o3, av3.pm10, av3.pm25, av3.rh, \n" +
                            "av4.co, av4.co2, av4.no2, av4.o3, av4.pm10, av4.pm25, av4.rh, " +
                            "av5.co, av5.co2, av5.no2, av5.o3, av5.pm10, av5.pm25, av5.rh, " +
                            "av6.co, av6.co2, av6.no2, av6.o3, av6.pm10, av6.pm25, av6.rh, " +
                            "av7.co, av7.co2, av7.no2, av7.o3, av7.pm10, av7.pm25, av7.rh, " +
                            "av8.co, av8.co2, av8.no2, av8.o3, av8.pm10, av8.pm25, av8.rh";
                    ResultSet resultSet = stmt.executeQuery(sql);

                    while (resultSet.next()) {
                        for (int j = 1; j < 68; j++) {
                            file.write(resultSet.getDouble(j) + "\n");
                        }
                    }
                    file.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}


