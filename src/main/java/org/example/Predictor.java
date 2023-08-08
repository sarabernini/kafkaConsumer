package org.example;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import static java.lang.Math.pow;

public class Predictor {
    private DBConnector dbConnector;
    private ArrayList<Double> realValues;

    public Predictor(DBConnector dbConnector ){
        this.dbConnector = dbConnector;
        this.realValues = new ArrayList<>();
    }
    public void createDataset(boolean meteo){
        //dbConnector.createStationsValues();
        dbConnector.createTrainingDataset(meteo);

    }
    public void training(String project_name, String task, String algorithm, String relation_name){
        dbConnector.training(project_name, task, algorithm, relation_name);
    }
    public void predict(String project_name, String relation_name, String array) throws IOException {
       dbConnector.predict(project_name, relation_name, array);
    }

    public Double comparePrediction(String project_name, String relation_name, Boolean meteo) throws IOException{
        realValues= dbConnector.readRealDataToCompare(meteo);
        int index= 30;
        if(meteo){
            index = 42;
        }
        Double meanSquareError= 0.0;
        int numberOfPredictions = 0;
        String array= "[";
        for(int i= 0; i<index-1; i++){
           array = array + realValues.get(i)+"::real,";
        }
        array= array + realValues.get(index-1)+"::real]";
        ArrayList<Double> pollutions= new ArrayList<>();
        for(int i= index; i<index+5; i++){
            pollutions.add(realValues.get(i));
        }
        System.out.println(array);
        dbConnector.predict(project_name, relation_name, array);
        File predictedValuesFile = new File("predictedValue.txt");
        Scanner predictedValues = new Scanner(predictedValuesFile);
        for(int i=0; i<5; i++) {
            String predictedValue= predictedValues.next();
            System.out.println(pollutions.get(i)+" -> "+predictedValue);
            meanSquareError += pow(pollutions.get(i)- Double.parseDouble(predictedValue), 2);
            numberOfPredictions++;
        }
        predictedValues.close();
        return meanSquareError/numberOfPredictions;
    }
}
