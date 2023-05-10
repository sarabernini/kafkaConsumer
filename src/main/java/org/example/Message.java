package org.example;

import java.util.ArrayList;

public class Message {
    public Message(MessageType message_type, int message_id, String station_name, String timestamp, String acquisition_timestamp, String gps_timestamp, float latitude, float longitude, ArrayList<Value> values, ArrayList<ModelValues> model, String command) {
        this.message_type = message_type;
        this.message_id = message_id;
        this.station_name = station_name;
        this.timestamp = timestamp;
        this.acquisition_timestamp = acquisition_timestamp;
        this.gps_timestamp = gps_timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.values = values;
        this.model = model;
        this.command = command;
    }

    enum MessageType {DATA, STATUS, CONTROL, MODEL, EXIT}

    private MessageType message_type;
    private int message_id = 0;
    private String station_name ="";
    private String timestamp="";
    private String acquisition_timestamp="";
    private String gps_timestamp="";
    private float latitude;
    private float longitude;
    private ArrayList<Value> values;
    private ArrayList<ModelValues> model;
    private String command = "";
}

class Value {
    private double value;
    private String sensor_name;

    public void setValue(double value) {
        this.value = value;
    }

    public void setSensor_name(String sensor_name) {
        this.sensor_name = sensor_name;
    }
}

class ModelValues {
    private int position;
    private String sensor_name;

    public void setPosition(int position) {
        this.position = position;
    }

    public void setSensor_name(String sensor_name) {
        this.sensor_name = sensor_name;
    }
}
