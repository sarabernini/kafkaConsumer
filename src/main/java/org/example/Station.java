package org.example;

public class Station {
    //attributes
    private String name;
    private float latitude;
    private float longitude;

    //constructor
    public Station(String name, float latitude, float longitude) {
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    //getter
    public String getName() {
        return name;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

}
