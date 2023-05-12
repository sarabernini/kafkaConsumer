package org.example;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;



public class Main {
    public static void main(String[] args) throws IOException, SQLException {
        ArrayList<Message> listOfMessage = new ArrayList<>();
        Consumer c= new Consumer(listOfMessage);
        //c.readData();
        DBConnecter db= new DBConnecter(c);
        //db.createTable();
        //db.dropTables();
        db.joinTables();

    }
}