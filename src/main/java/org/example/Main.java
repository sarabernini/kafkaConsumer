package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        /*ArrayList<Message> listOfMessage = new ArrayList<>();
        DBConnector db= new DBConnector();
        Project p1= new Project("UIA");
        Project p2= new Project("Carilucca");
        ArrayList<Project> projectList= new ArrayList<>();
        projectList.add(p1);
        projectList.add(p2);
        Consumer c= new Consumer(listOfMessage, projectList, db);
        //db.createTable();
        for(Project p: projectList){
            db.updateProject(p);
        }
        c.readData();
        //db.createAvg();

    }

    public static void resetAll(DBConnector db, Project p) throws SQLException, IOException {
        db.dropTables();
        db.createTable();
        db.updateProject(p);*/
        PeriodWeather periodWeather= new PeriodWeather(null, null);
        periodWeather.readExcelFile();
        DBConnector db= new DBConnector();
        db.creatWeather();
        db.insertWeather(periodWeather);
    }
}