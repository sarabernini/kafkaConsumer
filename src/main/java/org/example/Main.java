package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        ArrayList<Message> listOfMessage = new ArrayList<>();
        DBConnector db= new DBConnector();
        Project p1= new Project("UIA");
        Project p2= new Project("Carilucca");
        ArrayList<Project> projectList= new ArrayList<>();
        projectList.add(p1);
        projectList.add(p2);
        Consumer c= new Consumer(listOfMessage, projectList, db);
        //db.createTable();
        /*db.deleteProject();
        for(Project p: projectList){
            p.updateProjects(db);
        }*/
        //c.readData();
        //db.createAvg();
        updateWeather(db);

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

}