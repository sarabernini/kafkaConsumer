package org.example;

import java.sql.Timestamp;

public class DailyWeather{
    private String location;
    private Timestamp date;
    private int avgTemperature;
    private int maxTemperature;
    private int minTemperature;
    private int dewPoint;
    private int humidity;
    private int maxWind;
    private int avgWind;
    private int pressure;
    private String weatherPhenomena;

    //constructor
    public DailyWeather(){}

    public String getLocation() {
        return location;
    }

    public Timestamp getDate() {
        return date;
    }

    public int getAvgTemperature() {
        return avgTemperature;
    }

    public int getMaxTemperature() {
        return maxTemperature;
    }

    public int getMinTemperature() {
        return minTemperature;
    }

    public int getDewPoint() {
        return dewPoint;
    }

    public int getHumidity() {
        return humidity;
    }

    public int getMaxWind() {
        return maxWind;
    }

    public int getAvgWind() {
        return avgWind;
    }

    public int getPressure() {
        return pressure;
    }

    public String getWeatherPhenomena() {
        return weatherPhenomena;
    }

    public DailyWeather(String location, Timestamp date, int avgTemperature, int minTemperature, int maxTemperature, int dewPoint, int humidity, int avgWind, int maxWind, int pressure, String weatherPhenomena) {
        this.location= location;
        this.date = date;
        this.avgTemperature = avgTemperature;
        this.maxTemperature = maxTemperature;
        this.minTemperature = minTemperature;
        this.dewPoint = dewPoint;
        this.humidity = humidity;
        this.maxWind = maxWind;
        this.avgWind = avgWind;
        this.pressure = pressure;
        this.weatherPhenomena = weatherPhenomena;
    }


    public void setDate(Timestamp date) {
        this.date = date;
    }

    public void setAvgTemperature(int avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    public void setMaxTemperature(int maxTemperature) {
        this.maxTemperature = maxTemperature;
    }

    public void setMinTemperature(int minTemperature) {
        this.minTemperature = minTemperature;
    }

    public void setDewPoint(int dewPoint) {
        this.dewPoint = dewPoint;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public void setMaxWind(int maxWind) {
        this.maxWind = maxWind;
    }

    public void setAvgWind(int avgWind) {
        this.avgWind = avgWind;
    }

    public void setPressure(int pressure) {
        this.pressure = pressure;
    }

    public void setWeatherPhenomena(String weatherPhenomena) {
        this.weatherPhenomena = weatherPhenomena;
    }
}
