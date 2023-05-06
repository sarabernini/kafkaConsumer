package org.example;

public class Message {
    enum MessageType {DATA, STATUS, CONTROL, MODEL, EXIT}

    private MessageType message_type;
    private int message_id;
    private String station_name;
    private String timestamp;
    private String acquisition_timestamp;
    private String gps_timestamp;
    private float latitude;
    private float longitude;
    private Value[] values;
    private ModelValues[] model;
    private String command;
}

class Value {
    private double value;
    private String sensor_name;
}

class ModelValues {
    private int position;
    private String sensor_name;
}
