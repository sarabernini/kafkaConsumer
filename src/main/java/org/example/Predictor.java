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
        dbConnector.createTrainingDataset(meteo);
        //dbConnector.readRealDataToCompare(meteo);
    }

    public void training(String project_name, String task, String algorithm, String relation_name, String element_to_predict){
        dbConnector.training(project_name, task, algorithm, relation_name, element_to_predict);
    }

    public void predict(String project_name, String array) throws IOException {
       dbConnector.predict(project_name, array);
    }

    public void comparePrediction(String project_name) throws IOException {
        File realValuesFile;
        Scanner realValues;
        ArrayList<Double> predictedValue = new ArrayList<>();
        ArrayList<Double> pollutions = new ArrayList<>();
        int numberOfExample;
        int numberOfPredictions = 10;


        switch (project_name) {
            case "caso1":
                realValuesFile = new File("realValueMeteo.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample=15;
                break;
            case "caso2":
                realValuesFile = new File("realValue.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample=15;
                break;
            case "caso5":
                realValuesFile = new File("realValue1Day.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample = 7;
                break;
            case "caso6":
                realValuesFile = new File("realValue2Day.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample = 6;
                break;
            case "caso7":
                realValuesFile = new File("realValue3Day.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample = 5;
                break;
            case "caso8":
                realValuesFile = new File("realValue5Day.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample = 3;
                break;
            default:
                realValuesFile = new File("realValue.txt");
                realValues = new Scanner(realValuesFile);
                numberOfExample = 15;
                numberOfPredictions= 8;
        }

        for (int j = 0; j < numberOfExample; j++) {
            String array = "[";
            String real="[";
            double p;
            switch (project_name) {
                case "caso1" :
                    for (int i = 0; i < 52; i++) {
                        if (i < 41) {
                            array = array + realValues.next() + "::real, ";
                        } else if (i == 41) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            p =Double.parseDouble(realValues.next());
                            pollutions.add(p);
                            real= real + p +", ";
                        }
                    }
                break;
                case "caso2":
                    for (int i = 0; i < 40; i++) {
                        if (i < 29) {
                            array = array + realValues.next() + "::real,";
                        } else if (i == 29) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            p =Double.parseDouble(realValues.next());
                            pollutions.add(p);
                            real= real + p +", ";
                        }
                    }
                break;
                case "caso3":
                    for (int i = 0; i < 40; i++) {
                        if (i < 30 || i == 31) {
                            array = array + realValues.next() + "::real,";
                        } else if (i == 36) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            p =Double.parseDouble(realValues.next());
                            pollutions.add(p);
                            real= real + p +", ";
                        }
                    }
                break;
                case "caso4":
                    for (int i = 0; i < 40; i++) {
                        if (i < 30 || i == 33) {
                            array = array + realValues.next() + "::real,";
                        } else if (i == 38) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            p =Double.parseDouble(realValues.next());
                            pollutions.add(p);
                            real= real + p +", ";
                        }
                    }
                break;
                default:
                    for (int i = 0; i < 67; i++) {
                        if (i < 56) {
                            array = array + realValues.next() + "::real, ";
                        } else if (i == 56) {
                            array = array + realValues.next() + "::real]";
                        } else {
                            p =Double.parseDouble(realValues.next());
                            pollutions.add(p);
                            real= real + p +", ";
                        }
                    }
                break;
            }

            //System.out.println(array);
            System.out.println(real);
            dbConnector.predict(project_name, array);

            String predicted= "[";
            Double pr;

            File predictedValuesFile = new File("predictedValue.txt");
            Scanner predictedValues = new Scanner(predictedValuesFile);
            for (int i = 0; i < numberOfPredictions; i++) {
                pr = Double.parseDouble(predictedValues.next());
                predictedValue.add(pr);
                predicted = predicted+ pr +", ";
            }
            predictedValues.close();
            System.out.println(predicted);
        }
        realValues.close();



        //printPrediction(pollutions, predictedValue, project_name, numberOfExample);

    }

    public void printPrediction(ArrayList<Double> real, ArrayList<Double> predicted, String project_name, int numberOfExemple){
        System.out.println(project_name);
        for(int i=0; i<real.size();i++){
            //System.out.println(real.get(i) +" -> "+predicted.get(i));
        }

        ArrayList<Double> mse = new ArrayList<>();

        Double meanSquareErrorMaxCO = 0.0;
        Double meanSquareErrorMaxNO2 = 0.0;
        Double meanSquareErrorMaxO3 = 0.0;
        Double meanSquareErrorMaxPM10 = 0.0;
        Double meanSquareErrorMaxPM25 = 0.0;

        Double meanSquareErrorAvgCO = 0.0;
        Double meanSquareErrorAvgNO2 = 0.0;
        Double meanSquareErrorAvgO3 = 0.0;
        Double meanSquareErrorAvgPM10 = 0.0;
        Double meanSquareErrorAvgPM25 = 0.0;

        switch(project_name){
            case "caso3":
                for(int i=0;i<numberOfExemple;i++){
                    meanSquareErrorMaxCO+= pow(predicted.get((8 * i))-real.get((8 * i)), 2);
                    meanSquareErrorMaxO3+= pow(predicted.get(1+(8*i))-real.get(1+(8*i)), 2);
                    meanSquareErrorMaxPM10+= pow(predicted.get(2+(8*i))-real.get(2+(8*i)), 2);
                    meanSquareErrorMaxPM25+= pow(predicted.get(3+(8*i))-real.get(3+(8*i)), 2);
                    meanSquareErrorAvgCO+= pow(predicted.get(4+(8 * i))-real.get(4+(8 * i)), 2);
                    meanSquareErrorAvgO3+= pow(predicted.get(5+(8*i))-real.get(5+(8*i)), 2);
                    meanSquareErrorAvgPM10+= pow(predicted.get(6+(8*i))-real.get(6+(8*i)), 2);
                    meanSquareErrorAvgPM25+= pow(predicted.get(7+(8*i))-real.get(7+(8*i)), 2);
                }
                break;
            case "caso4":
                for(int i=0;i<numberOfExemple;i++){
                    meanSquareErrorMaxCO+= pow(predicted.get((8 * i))-real.get((8 * i)), 2);
                    meanSquareErrorMaxNO2+= pow(predicted.get(1+(8*i))-real.get(1+(8*i)), 2);
                    meanSquareErrorMaxO3+= pow(predicted.get(2+(8*i))-real.get(2+(8*i)), 2);
                    meanSquareErrorMaxPM25+= pow(predicted.get(3+(8*i))-real.get(3+(8*i)), 2);
                    meanSquareErrorAvgCO+= pow(predicted.get(4+(8 * i))-real.get(4+(8 * i)), 2);
                    meanSquareErrorAvgNO2+= pow(predicted.get(5+(8*i))-real.get(5+(8*i)), 2);
                    meanSquareErrorAvgO3+= pow(predicted.get(6+(8*i))-real.get(6+(8*i)), 2);
                    meanSquareErrorAvgPM25+= pow(predicted.get(7+(8*i))-real.get(7+(8*i)), 2);
                }
                break;
            default:
                for(int i=0;i<numberOfExemple;i++) {
                    meanSquareErrorMaxCO+= pow(predicted.get((10 * i))-real.get((10 * i)), 2);
                    meanSquareErrorMaxNO2+= pow(predicted.get(1+(10*i))-real.get(1+(10*i)), 2);
                    meanSquareErrorMaxO3+= pow(predicted.get(2+(10*i))-real.get(2+(10*i)), 2);
                    meanSquareErrorMaxPM10+= pow(predicted.get(3+(10*i))-real.get(3+(10*i)), 2);
                    meanSquareErrorMaxPM25+= pow(predicted.get(4+(10*i))-real.get(4+(10*i)), 2);
                    meanSquareErrorAvgCO+= pow(predicted.get(5+(10 * i))-real.get(5+(10 * i)), 2);
                    meanSquareErrorAvgNO2+= pow(predicted.get(6+(10*i))-real.get(6+(10*i)), 2);
                    meanSquareErrorAvgO3+= pow(predicted.get(7+(10*i))-real.get(7+(10*i)), 2);
                    meanSquareErrorAvgPM10+= pow(predicted.get(8+(10*i))-real.get(8+(10*i)), 2);
                    meanSquareErrorAvgPM25+= pow(predicted.get(9+(10*i))-real.get(9+(10*i)), 2);
                }
        }
        System.out.println("errore quadratico medio max CO: "+meanSquareErrorMaxCO/numberOfExemple);
        System.out.println("errore quadratico medio max NO2: "+meanSquareErrorMaxNO2/numberOfExemple);
        System.out.println("errore quadratico medio max O3: "+meanSquareErrorMaxO3/numberOfExemple);
        System.out.println("errore quadratico medio max PM10: "+meanSquareErrorMaxPM10/numberOfExemple);
        System.out.println("errore quadratico medio max PM25: "+meanSquareErrorMaxPM25/numberOfExemple);
        System.out.println("");
        System.out.println("errore quadratico medio avg CO: "+meanSquareErrorAvgCO/numberOfExemple);
        System.out.println("errore quadratico medio avg NO2: "+meanSquareErrorAvgNO2/numberOfExemple);
        System.out.println("errore quadratico medio avg O3: "+meanSquareErrorAvgO3/numberOfExemple);
        System.out.println("errore quadratico medio avg PM10: "+meanSquareErrorAvgPM10/numberOfExemple);
        System.out.println("errore quadratico medio avg PM25: "+meanSquareErrorAvgPM25/numberOfExemple);

    }

    public void createDailyDataset(int i) {
        dbConnector.createDailyDataset(i);
    }
}
