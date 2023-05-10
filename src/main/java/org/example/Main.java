package org.example;

import java.io.IOException;
import java.util.ArrayList;


public class Main {
    public static void main(String[] args) throws IOException {
        ArrayList<Message> listOfMessage = new ArrayList<>();
        Consumer c= new Consumer(listOfMessage);
        c.readData();
    }
}