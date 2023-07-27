package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
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

    public static void updateWeather(DBConnector db, Timestamp startTime, Timestamp endTime, String location) throws IOException {
        ArrayList<String> filesToRead = new ArrayList<>();
        filesToRead.add("rain_"+location+"");
        filesToRead.add("temperature_"+location);
        filesToRead.add("wind_"+location);
        Weather weather = new Weather(db, startTime, endTime, filesToRead, location);
        weather.createAllTableInDB();
        weather.calculateAverage();
    }

    public static void makePrediction(DBConnector dbConnector){
        int number_of_era= 10;
        Predictor predictor = new Predictor(dbConnector);
        predictor.createDataset(Timestamp.valueOf("2023-05-26 09:30:00"), Timestamp.valueOf("2023-07-19 23:59:59"));
        /*double rf_prediction= 0;
        double xgboost_prediction =0;
        for(int i = 0; i<number_of_era; i++){
            predictor.training("xgboost_test_"+i, "regression", "xgboost", "training_dataset", "pm25", 25);
            predictor.training("random_forest_test_"+i, "regression", "random_forest", "training_dataset", "pm25", 25);
            xgboost_prediction+= 8.8- predictor.predict("xgboost_test_"+i, "training_dataset", "[20::float,19.73,411.16,83.68,47.78,233.60,0,0,0,0,0,1,0,0,22.9,53,1,0]");
            rf_prediction+=8.84- predictor.predict("random_forest_test_"+i, "training_dataset", "[20::float,19.73,411.16,83.68,47.78,233.60,0,0,0,0,0,1,0,0,22.9,53,1,0]");
        }
        System.out.println("errore medio random forest:"+ rf_prediction/number_of_era);
        System.out.println("errore medio xgboost:" + xgboost_prediction/number_of_era);*/
    }

}