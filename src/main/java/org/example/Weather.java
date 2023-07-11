package org.example;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

class SingleWeather {
    //attribute
    private Timestamp date;
    private double value;
    private String location;

    public SingleWeather(Timestamp date, double value, String location){
        this.date= date;
        this.value= value;
        this.location = location;
    }

    public Timestamp getDate(){
        return date;
    }
    public double getValue(){
        return value;
    }

    public String getLocation() {
        return location;
    }
}
public class Weather{
    //attributi
    private ArrayList<SingleWeather> weatherList;
    private ArrayList<String> fileToRead;
    private Timestamp startPeriod;
    private Timestamp endPeriod;
    private DBConnector dbConnector;
    private String location;
    public Weather(DBConnector dbConnector, Timestamp startPeriod, Timestamp endPeriod, ArrayList<String> fileToRead, String location){
        this.startPeriod= startPeriod;
        this.endPeriod= endPeriod;
        this.weatherList= new ArrayList<>();
        this.dbConnector = dbConnector;
        this.fileToRead = fileToRead;
        this.location= location;
    }

    public ArrayList<SingleWeather> getWeatherList(){
        return weatherList;
    }

    //leggo dal file excel le informazioni meteo e per ogni riga inserisco le informazioni  come attributi della classe
    // SingleWeather che a sua volta viene insierita in nella lista weatherList
    public void readFileExcel(String file){
        try
        {
            FileInputStream fis=new FileInputStream(new File(file));
            XSSFWorkbook wb = new XSSFWorkbook(fis);
            XSSFSheet sheet = wb.getSheetAt(0);
            Iterator<Row> itr = sheet.iterator();
            Row row = itr.next();
            //itero sul file excel
            while (itr.hasNext()) {
                row = itr.next();
                Timestamp date= Timestamp.valueOf(convertDate(row.getCell(0).getDateCellValue().toString()));
                if(date.after(startPeriod) && date.before(endPeriod)){
                    Double value;
                    if(file.equals("src/main/resources/wind_prato.xlsx")){
                        value= convertWind(row.getCell(1).toString());
                    }else{
                        value = Double.parseDouble(row.getCell(1).toString());
                    }
                    SingleWeather singleWeather = new SingleWeather(date, value, location);
                    weatherList.add(singleWeather);
                }
            }
        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public void createAllTableInDB(){
        for(String fileName : fileToRead){
            weatherList.clear();
            readFileExcel("src/main/resources/"+ fileName +".xlsx");
            dbConnector.createWeather(fileName);
            dbConnector.insertWeather(weatherList, fileName);
        }
    }

    public double convertWind(String wind){
        String[] letters = wind.split("");
        int i = 0;
        while(!Objects.equals(letters[i], "/")){
            i++;
        }
        String newWind ="";
        while(i < letters.length-1){
            newWind += letters[i+1];
            i++;
        }
        return Double.parseDouble(newWind);
    }

    public String convertDate(String date){
        String[] worldString = date.split(" ");
        switch (worldString[1]){
            case "Jan":
                return(worldString[5] +"-01-"+worldString[2]+" "+worldString[3]);
            case "Feb":
                return(worldString[5] +"-02-"+worldString[2]+" "+worldString[3]);
            case "Mar":
                return(worldString[5] +"-03-"+worldString[2]+" "+worldString[3]);
            case "Apr":
                return(worldString[5] +"-04-"+worldString[2]+" "+worldString[3]);
            case "May":
                return(worldString[5] +"-05-"+worldString[2]+" "+worldString[3]);
            case "Jun":
                return(worldString[5] +"-06-"+worldString[2]+" "+worldString[3]);
            case "Jul":
                return(worldString[5] +"-07-"+worldString[2]+" "+worldString[3]);
            case "Aug":
                return(worldString[5] +"-08-"+worldString[2]+" "+worldString[3]);
            case "Sep":
                return(worldString[5] +"-09-"+worldString[2]+" "+worldString[3]);
            case "Oct":
                return(worldString[5] +"-10-"+worldString[2]+" "+worldString[3]);
            case "Nov":
                return(worldString[5] +"-11-"+worldString[2]+" "+worldString[3]);
            case "Dec":
                return(worldString[5] +"-12-"+worldString[2]+" "+worldString[3]);
        }return "000";
    }
}
