package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        ArrayList<Message> listOfMessage = new ArrayList<>();
        DBConnector db= new DBConnector();
        Project p= new Project("UIA");
        p.getStationsNames();
        Consumer c= new Consumer(listOfMessage, p, db);
        c.readData();
        db.createTable();
        c.addMessagesToDatabase();


        //db.dropTables();
        //db.joinTables();
    }
}