package org.example;

import com.microsoft.schemas.office.office.STInsetMode;

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
        db.readDataToCompare();
        //makePrediction(db);



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

    public static void makePrediction(DBConnector dbConnector) throws IOException {
        String element_to_predict = "'max_co', 'max_no2', 'max_o3', 'max_pm10', 'max_pm25', 'avg_co', 'avg_no2', 'avg_o3', 'avg_pm10', 'avg_pm25'";
        String element_to_predict2 = "'max_co', 'max_o3', 'max_pm10', 'max_pm25', 'avg_co', 'avg_o3', 'avg_pm10', 'avg_pm25'";
        String element_to_predict3 = "'max_co', 'max_no2', 'max_o3', 'max_pm25', 'avg_co', 'avg_no2', 'avg_o3', 'avg_pm25'";
        Predictor predictor = new Predictor(dbConnector);
        //predictor.createDataset(true);
        //predictor.training("caso1","regression", "xgboost", "training_dataset_meteo", element_to_predict);
        //predictor.comparePrediction("caso1");
        //predictor.createDataset(false);
        //predictor.training("caso2","regression", "xgboost", "training_dataset", element_to_predict);
        //predictor.comparePrediction("caso2");

        //predictor.training("caso3","regression", "xgboost", "training_dataset", element_to_predict2);
        //predictor.comparePrediction("caso3");
        //predictor.training("caso4","regression", "xgboost", "training_dataset", element_to_predict3);
        //predictor.comparePrediction("caso4");

        //predictor.createDailyDataset(1);
        //predictor.createDailyDataset(2);
        //predictor.createDailyDataset(5);
        //predictor.training("caso5","regression", "xgboost", "training_daily_dataset_1", element_to_predict);
        //predictor.training("caso6","regression", "xgboost", "training_daily_dataset_2", element_to_predict);
        //predictor.training("caso7","regression", "xgboost", "training_daily_dataset_3", element_to_predict);
        //predictor.training("caso8","regression", "xgboost", "training_daily_dataset_5", element_to_predict);

        //predictor.comparePrediction("caso5");
        //predictor.comparePrediction("caso6");
        //predictor.comparePrediction("caso7");
        //predictor.comparePrediction("caso8");

        predictor.createDataset(true);
        predictor.training("caso9", "regression", "xgboost", "training_dataset_meteo2", element_to_predict);






    }

}