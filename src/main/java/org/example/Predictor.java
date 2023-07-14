package org.example;

public class Predictor {
    private DBConnector dbConnector;

    public Predictor(DBConnector dbConnector){
        this.dbConnector = dbConnector;
    }
    public void createDataset(){
        //dbConnector.createStationsValues();
        dbConnector.createDataset();
    }
    public void training(){

    }
    public void predict(){

    }
}
