package org.example;

import com.microsoft.schemas.office.office.STInsetMode;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        ArrayList<String> pollutantsList = new ArrayList<>();
        pollutantsList.add("co");
        pollutantsList.add("co2");
        pollutantsList.add("no2");
        pollutantsList.add("o3");
        pollutantsList.add("pm10");
        pollutantsList.add("pm25");

        Timestamp startPeriod= Timestamp.valueOf("2023-05-26 09:30:00");
        Timestamp endPeriod= Timestamp.valueOf("2023-07-22 23:59:00");

        String location = "lucca";
        DBConnector db= new DBConnector();
        //readData(db);
        //updateWeather(db, startPeriod, endPeriod, location);
        makePrediction(db);


    }

    public static void readData(DBConnector db) throws IOException {
        ArrayList<Message> listOfMessage = new ArrayList<>();
        Project p1= new Project("UIA", "prato");
        Project p2= new Project("Carilucca", "lucca");
        ArrayList<Project> projectList= new ArrayList<>();
        projectList.add(p1);
        projectList.add(p2);
        Consumer c= new Consumer(listOfMessage, projectList, db);
        //db.createTable();
        db.deleteProject();
        for(Project p: projectList){
            p.updateProjects(db);
        }
        c.readData();
    }

    public static void updateWeather(DBConnector db, Timestamp startTime, Timestamp endTime, String location){
        ArrayList<String> filesToRead = new ArrayList<>();
        filesToRead.add("rain_"+location+"");
        filesToRead.add("temperature_"+location);
        filesToRead.add("wind_"+location);
        Weather weather = new Weather(db, startTime, endTime, filesToRead, location);
        weather.createAllTableInDB();
        weather.calculateAverage();
    }

    public static void makePrediction(DBConnector dbConnector) throws SQLException, IOException {
        Predictor predictor = new Predictor(dbConnector);
       // predictor.createDataset(true);
        predictor.training("test7","regression", "xgboost", "training_dataset_meteo");
        System.out.println(predictor.comparePrediction("test7", "training_dataset_meteo", true));
        //predictor.createDataset(false);
        predictor.training("test8","regression", "xgboost", "training_dataset");
        System.out.println(predictor.comparePrediction("test8", "training_dataset", false));
    }

}