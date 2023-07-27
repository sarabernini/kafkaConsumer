package org.example;

import java.awt.desktop.SystemSleepEvent;
import java.sql.Timestamp;
import java.util.ArrayList;

public class Predictor {
    private DBConnector dbConnector;

    public Predictor(DBConnector dbConnector){
        this.dbConnector = dbConnector;
    }
    public void createDataset(Timestamp startPeriod, Timestamp endPeriod){
        //dbConnector.createStationsValues();
        //dbConnector.createTrainingDataset(startPeriod, endPeriod);
        dbConnector.createDatasetToCompare(endPeriod);
    }
    public void training(String project_name, String task, String algorithm, String relation_name, String y_column_name, int n_estimators){
        dbConnector.training(project_name, task, algorithm, relation_name, y_column_name, n_estimators);
    }
    public double predict(String project_name, String relation_name, String array){
       return dbConnector.predict(project_name, relation_name, array);
    }
}
