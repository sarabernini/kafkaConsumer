package org.example;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

import static java.lang.Math.pow;

public class Predictor {
    private DBConnector dbConnector;
    private ArrayList<String> pollutantsList;

    public Predictor(DBConnector dbConnector, ArrayList<String> pollutantsList ){
        this.dbConnector = dbConnector;
        this.pollutantsList= pollutantsList;
    }
    public void createDataset(Timestamp startPeriod, Timestamp endPeriod){
        //dbConnector.createStationsValues();
        dbConnector.createTrainingDataset(startPeriod, endPeriod);
        dbConnector.createDatasetToCompare(endPeriod);
    }
    public void training(String project_name, String task, String algorithm, String relation_name, String y_column_name, int n_estimators){
        dbConnector.training(project_name, task, algorithm, relation_name, y_column_name, n_estimators);
    }
    public void predict(String project_name, String relation_name, String array) throws IOException {
       dbConnector.predict(project_name, relation_name, array);
    }

    public Double comparePrediction(String project_name, String relation_name) throws IOException{
        Double meanSquareError= 0.0;
        int numberOfPredictions = 0;
        dbConnector.readRealDataToCompare();
        File realValuesFile = new File("realValue.txt");
        Scanner realValues = new Scanner(realValuesFile);
        while(realValues.hasNext()){
            String[] pollutions= new String[6];
            int j=0;
            String array= "[";
            for(int i= 0; i<19; i++){
                String s = realValues.next();
                if(i>1 && i<8){
                    pollutions[j]= s;
                    j++;
                }else{
                    array = array + s+"::real,";
                }
            }
            array= array + realValues.next() +"::real]";
            System.out.println(array);
            dbConnector.predict(project_name, relation_name, array);
            File predictedValuesFile = new File("predictedValue.txt");
            Scanner predictedValues = new Scanner(predictedValuesFile);
            for(int i=0; i<6; i++) {
                meanSquareError += pow(Double.parseDouble(pollutions[i] )- Double.parseDouble(predictedValues.next()), 2);
                numberOfPredictions++;
            }
            predictedValues.close();
        }
        realValues.close();
        return meanSquareError/numberOfPredictions;
    }
}
