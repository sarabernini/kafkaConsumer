package org.example;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        DBConnector db= new DBConnector();
        //readData(db);
        //updateWeather(db);
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

    public static void updateWeather(DBConnector db) throws IOException {
        ArrayList<String> filesToRead = new ArrayList<>();
        filesToRead.add("rain_prato");
        filesToRead.add("temperature_prato");
        filesToRead.add("wind_prato");
        Weather weather = new Weather(db, Timestamp.valueOf("2023-05-26 09:30:00"), Timestamp.valueOf("2023-07-07 23:59:00"), filesToRead, "lucca");
        //weather.createAllTableInDB();
        weather.calculateAverage();
    }

    public static void makePrediction(DBConnector dbConnector){
        Predictor predictor = new Predictor(dbConnector);
        predictor.createDataset();
        predictor.training();
        predictor.predict();
    }

}