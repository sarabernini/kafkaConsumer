package org.example;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import static java.lang.Math.pow;

public class Predictor {
    private DBConnector dbConnector;

    public Predictor(DBConnector dbConnector ){
        this.dbConnector = dbConnector;
    }
    public void createDataset(boolean meteo) throws IOException {
        dbConnector.createStationsValues(meteo);
        dbConnector.createTrainingDataset(meteo);
        dbConnector.readRealDataToCompare(meteo);
    }

    public void training(String project_name, String task, String algorithm, String relation_name, String element_to_predict){
        dbConnector.training(project_name, task, algorithm, relation_name, element_to_predict);
    }

    public void predict(String project_name, String relation_name, String array) throws IOException {
       dbConnector.predict(project_name, relation_name, array);
    }

    public Double comparePrediction(String project_name, String relation_name) throws IOException{
        int index;
        File realValuesFile;
        Scanner realValues;
        Double meanSquareError= 0.0;
        int numberOfPredictions = 1;
        String array= "[";
        ArrayList<Double> pollutions= new ArrayList<>();

        switch (project_name){
            case "case1":
                realValuesFile = new File("realValueMeteo.txt");
                realValues = new Scanner(realValuesFile);
                numberOfPredictions= 10;
                for(int i= 0; i<52; i++) {
                    if (i < 41) {
                        array = array + realValues.next() + "::real,";
                    } else if (i == 41) {
                        array = array + realValues.next() + "::real]";
                    } else {
                        pollutions.add(Double.parseDouble(realValues.next()));
                    }
                }
                realValues.close();
                break;
            case "case2":
                realValuesFile = new File("realValue.txt");
                realValues = new Scanner(realValuesFile);
                numberOfPredictions= 10;
                for(int i= 0; i<40; i++) {
                    if (i < 29) {
                        array = array + realValues.next() + "::real,";
                    } else if (i == 29) {
                        array = array + realValues.next() + "::real]";
                    } else {
                        pollutions.add(Double.parseDouble(realValues.next()));
                    }
                }
                realValues.close();
                break;
            case "case3":
                realValuesFile = new File("realValue.txt");
                realValues = new Scanner(realValuesFile);
                numberOfPredictions= 8;
                for(int i= 0; i<40; i++){
                    if(i<30 || i == 31){
                        array = array + realValues.next()+"::real,";
                    }else if(i==36){
                        array= array + realValues.next()+"::real]";
                    }else{
                        pollutions.add(Double.parseDouble(realValues.next()));
                    }
                }
                realValues.close();
                break;
            case "case4":
                realValuesFile = new File("realValue.txt");
                realValues = new Scanner(realValuesFile);
                numberOfPredictions= 8;
                for(int i= 0; i<40; i++){
                    if(i<30 || i == 33){
                        array = array + realValues.next()+"::real,";
                    }else if(i==38){
                        array= array + realValues.next()+"::real]";
                    }else{
                        pollutions.add(Double.parseDouble(realValues.next()));
                    }
                }
                realValues.close();
                break;
        }

        System.out.println(array);
        System.out.println(Arrays.toString(pollutions.toArray()));
        dbConnector.predict(project_name, relation_name, array);

        File predictedValuesFile = new File("predictedValue.txt");
        Scanner predictedValues = new Scanner(predictedValuesFile);
        for(int i=0; i<10; i++) {
            String predictedValue= predictedValues.next();
            System.out.println(pollutions.get(i)+" -> "+predictedValue);
            meanSquareError += pow(pollutions.get(i)- Double.parseDouble(predictedValue), 2);
            numberOfPredictions++;
        }
        predictedValues.close();
        return meanSquareError/numberOfPredictions;
    }

    public void createDailyDataset(int i) {
        dbConnector.createDailyDataset(i);
    }
}
