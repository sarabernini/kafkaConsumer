package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
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
        dbConnector.createStationsValues();
        dbConnector.createTrainingDataset(startPeriod, endPeriod);
        dbConnector.createDatasetToCompare(endPeriod);
    }
    public void training(String project_name, String task, String algorithm, String relation_name, String y_column_name, int n_estimators){
        dbConnector.training(project_name, task, algorithm, relation_name, y_column_name, n_estimators);
    }
    public ResultSet predict(String project_name, String relation_name, String array){
       return dbConnector.predict(project_name, relation_name, array);
    }

    public Double comparePrediction(String project_name, String relation_name) throws SQLException, IOException {
        Double meanSquareError= 0.0;
        int numberOfPredictions = 0;
        dbConnector.readRealDataToCompare();
        File realValuesFile = new File("realValue.txt");
        Scanner realValues = new Scanner(realValuesFile);
        while(realValues.hasNextLine()){
            String[] pollutions= new String[6];
            int j=0;
            String array= "[";
            for(int i= 0; i<19; i++){
                if(i>1 && i<8){
                    pollutions[j]= realValues.next();
                    j++;
                }else{
                    array = array + realValues.next()+"::real,";
                }
            }
            array= array +realValues.next()+"::real]";
            System.out.println(array);
            ResultSet predictedValues= predict(project_name, relation_name, array);
            for(int i=0; i<6; i++) {
                predictedValues.next();
                meanSquareError += pow(Double.parseDouble(pollutions[i] )- predictedValues.getDouble(0), 2);
                numberOfPredictions++;
            }

        }
        realValues.close();
        return meanSquareError/numberOfPredictions;
    }
}
