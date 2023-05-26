package org.example;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;

public class PeriodWeather {
    public ArrayList<DailyWeather> getPeriodWeather() {
        return periodWeather;
    }

    private ArrayList<DailyWeather> periodWeather;
    private Timestamp startPeriod;
    private Timestamp endPeriod;

    public PeriodWeather(Timestamp startPeriod, Timestamp endPeriod){
        periodWeather = new ArrayList<>();
        this.startPeriod = startPeriod;
        this.endPeriod= endPeriod;
    }
    public void readExcelFile() throws IOException {
        try
        {
            //obtaining input bytes from a file
            FileInputStream fis=new FileInputStream(new File("C:\\Users\\sarab\\IntelliJProjects\\kafkaConsumer\\src\\main\\resources\\Prato-2023-Maggio.xlsx"));
            //creating workbook instance that refers to .xls file
            XSSFWorkbook wb = new XSSFWorkbook(fis);
            XSSFSheet sheet = wb.getSheetAt(0);     //creating a Sheet object to retrieve object
            Iterator<Row> itr = sheet.iterator();
            Row row = itr.next();
            //iterating over excel file
            while (itr.hasNext()) {
                row = itr.next();
                DailyWeather dailyWeather= new DailyWeather(row.getCell(0).toString(), Timestamp.valueOf(convertDate(row.getCell(1).toString())),
                        convertInt(row.getCell(2).toString()), convertInt(row.getCell(3).toString()), convertInt(row.getCell(4).toString()),
                        convertInt(row.getCell(5).toString()), convertInt(row.getCell(6).toString()), convertInt(row.getCell(7).toString()),
                        convertInt(row.getCell(8).toString()), convertInt(row.getCell(10).toString()), row.getCell(13).toString());
                periodWeather.add(dailyWeather);
            }
        }catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    public int convertInt(String i){
        String[] s= i.split("");
        i="";
        for(int j=0; j<s.length-2; j++){
           i+= ""+s[j];
        }
        return Integer.parseInt(i);
    }
    public String convertDate(String date){
        String[] d = date.split("");
        switch (d[3]+""+d[4]+""+d[5]){
            case "gen":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-01-"+d[0]+d[1]+" 00:00:01");
            case "feb":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-02-"+d[0]+d[1]+" 00:00:01");
            case "mar":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-03-"+d[0]+d[1]+" 00:00:01");
            case "apr":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-04-"+d[0]+d[1]+" 00:00:01");
            case "mag":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-05-"+d[0]+d[1]+" 00:00:01");
            case "giu":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-06-"+d[0]+d[1]+" 00:00:01");
            case "lug":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-07-"+d[0]+d[1]+" 00:00:01");
            case "ago":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-08-"+d[0]+d[1]+" 00:00:01");
            case "set":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-09-"+d[0]+d[1]+" 00:00:01");
            case "ott":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-10-"+d[0]+d[1]+" 00:00:01");
            case "nov":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-11-"+d[0]+d[1]+" 00:00:01");
            case "dic":
                return(d[7]+""+d[8]+""+d[9]+""+d[10]+"-12-"+d[0]+d[1]+" 00:00:01");
        }return "000";
    }
}



