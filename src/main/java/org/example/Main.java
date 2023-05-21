package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        ArrayList<Message> listOfMessage = new ArrayList<>();
        DBConnector db= new DBConnector();
        Project p= new Project("UIA");
        Consumer c= new Consumer(listOfMessage, p, db);
        p.getStationsNames();
        c.readData();
    }

    public void resetAll(DBConnector db, Project p) throws SQLException, IOException {
        db.dropTables();
        db.createTable();
        db.updateProject(p);
    }
}