package org.example;

import java.io.*;
import java.lang.reflect.Array;
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
        //dbConnector.createStationsValues(meteo);
        //dbConnector.createTrainingDataset(meteo);
        dbConnector.readRealDataToCompare(meteo);
    }

    public void training(String project_name, String task, String algorithm, String relation_name, String element_to_predict){
        dbConnector.training(project_name, task, algorithm, relation_name, element_to_predict);
    }

    public void predict(String project_name, String relation_name, String array) throws IOException {
       dbConnector.predict(project_name, relation_name, array);
    }

    public void comparePrediction(String project_name, String relation_name) throws IOException {
        File realValuesFile;
        Scanner realValues;
        ArrayList<Double> predictedValue = new ArrayList<>();
        ArrayList<Double> pollutions = new ArrayList<>();
        int numberOfPredictions = 10;


        switch (project_name) {
            case "case1":
                realValuesFile = new File("realValueMeteo.txt");
                realValues = new Scanner(realValuesFile);
                break;
            case "case5":
                realValuesFile = new File("realValue1Day.txt");
                realValues = new Scanner(realValuesFile);
                break;
            case "case6":
                realValuesFile = new File("realValue2Day.txt");
                realValues = new Scanner(realValuesFile);
                break;
            case "case7":
                realValuesFile = new File("realValue3Day.txt");
                realValues = new Scanner(realValuesFile);
                break;
            default:
                realValuesFile = new File("realValue.txt");
                realValues = new Scanner(realValuesFile);
        }

        for (int j = 0; j < 10; j++) {
            String array = "[";
            switch (project_name) {
                case "case1" -> {
                    numberOfPredictions=10;
                    for (int i = 0; i < 52; i++) {
                        if (i < 41) {
                            array = array + realValues.next() + "::real, ";
                        } else if (i == 41) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            pollutions.add(Double.parseDouble(realValues.next()));
                        }
                    }
                }
                case "case2" -> {
                    numberOfPredictions=10;
                    for (int i = 0; i < 40; i++) {
                        if (i < 29) {
                            array = array + realValues.next() + "::real,";
                        } else if (i == 29) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            pollutions.add(Double.parseDouble(realValues.next()));
                        }
                    }
                }
                case "case3" -> {
                    numberOfPredictions=8;
                    for (int i = 0; i < 40; i++) {
                        if (i < 30 || i == 31) {
                            array = array + realValues.next() + "::real,";
                        } else if (i == 36) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            pollutions.add(Double.parseDouble(realValues.next()));
                        }
                    }
                }
                case "case4" -> {
                    numberOfPredictions=8;
                    for (int i = 0; i < 40; i++) {
                        if (i < 30 || i == 33) {
                            array = array + realValues.next() + "::real,";
                        } else if (i == 38) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            pollutions.add(Double.parseDouble(realValues.next()));
                        }
                    }
                }
                default -> {
                    numberOfPredictions=10;
                    for (int i = 0; i < 67; i++) {
                        if (i < 56) {
                            array = array + realValues.next() + "::real, ";
                        } else if (i == 56) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            pollutions.add(Double.parseDouble(realValues.next()));
                        }
                    }
                }
            }

            System.out.println(array);
            System.out.println(Arrays.toString(pollutions.toArray()));
            dbConnector.predict(project_name, relation_name, array);

            File predictedValuesFile = new File("predictedValue.txt");
            Scanner predictedValues = new Scanner(predictedValuesFile);
            for (int i = 0; i < numberOfPredictions; i++) {
                predictedValue.add(Double.parseDouble(predictedValues.next()));
            }
            predictedValues.close();
        }
        realValues.close();

        printPrediction(pollutions, predictedValue, project_name);

    }

    public void printPrediction(ArrayList<Double> real, ArrayList<Double> predicted, String project_name){
        for(int i=0; i<real.size();i++){
            System.out.println(real.get(i) +" -> "+predicted.get(i));
        }
        ArrayList<Double> mse = new ArrayList<>();

        Double meanSquareErrorCO = 0.0;
        Double meanSquareErrorNO2 = 0.0;
        Double meanSquareErrorO3 = 0.0;
        Double meanSquareErrorPM10 = 0.0;
        Double meanSquareErrorPM25 = 0.0;

        switch(project_name){
            case "case3":
                for(int i=0;i<10;i++){
                    meanSquareErrorCO+= pow(predicted.get((4 * i))-real.get((4 * i)), 2);
                    meanSquareErrorO3+= pow(predicted.get(1+(4*i))-real.get(1+(4*i)), 2);
                    meanSquareErrorPM10+= pow(predicted.get(2+(4*i))-real.get(2+(4*i)), 2);
                    meanSquareErrorPM25+= pow(predicted.get(3+(4*i))-real.get(3+(4*i)), 2);
                }
                break;
            case "case4":
                for(int i=0;i<10;i++){
                    meanSquareErrorCO+= pow(predicted.get((4 * i))-real.get((4 * i)), 2);
                    meanSquareErrorNO2+= pow(predicted.get(1+(4*i))-real.get(1+(4*i)), 2);
                    meanSquareErrorO3+= pow(predicted.get(2+(4*i))-real.get(2+(4*i)), 2);
                    meanSquareErrorPM25+= pow(predicted.get(3+(4*i))-real.get(3+(4*i)), 2);
                }
                break;
            default:
                for(int i=0;i<10;i++) {
                    meanSquareErrorCO += pow(predicted.get((5 * i))-real.get((5 * i)), 2);
                    meanSquareErrorNO2 += pow(predicted.get(1 + (5 * i))-real.get(1 + (5 * i)), 2);
                    meanSquareErrorO3 += pow(predicted.get(2 + (5 * i))-real.get(2 + (5 * i)), 2);
                    meanSquareErrorPM10 += pow(predicted.get(3 + (5 * i))-real.get(3 + (5 * i)), 2);
                    meanSquareErrorPM25 += pow(predicted.get(4 + (5 * i))-real.get(4 + (5 * i)), 2);
                }
        }
        System.out.println("errore quadratico medio CO: "+meanSquareErrorCO/20);
        System.out.println("errore quadratico medio NO2: "+meanSquareErrorNO2/20);
        System.out.println("errore quadratico medio O3: "+meanSquareErrorO3/20);
        System.out.println("errore quadratico medio PM10: "+meanSquareErrorPM10/20);
        System.out.println("errore quadratico medio PM25: "+meanSquareErrorPM25/25);
    }

    public void createDailyDataset(int i) {
        dbConnector.createDailyDataset(i);
    }
}
