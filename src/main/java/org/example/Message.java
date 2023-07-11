package org.example;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.sql.Timestamp;
import java.util.ArrayList;

public class Message {

    enum MessageType {DATA, STATUS, CONTROL, MODEL, EXIT, NULL}

    //attributi
    private MessageType message_type;
    private int message_id;
    private String station_name;
    private Timestamp timestamp;
    private Timestamp acquisition_timestamp;
    private Timestamp gps_timestamp;
    private float latitude;
    private float longitude;
    private ArrayList<Value> values;
    private String command;

    //costruttori
    public Message(MessageType message_type, int message_id, String station_name, java.sql.Timestamp timestamp, java.sql.Timestamp acquisition_timestamp, java.sql.Timestamp gps_timestamp, float latitude, float longitude, ArrayList<Value> values, String command) {
        this.message_type = message_type;
        this.message_id = message_id;
        this.station_name = station_name;
        this.timestamp = timestamp;
        this.acquisition_timestamp = acquisition_timestamp;
        this.gps_timestamp = gps_timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.values = values;
        this.command = command;
    }

    public Message(GenericRecord r) {
        setMessage_type(r.get(0));
        setMessage_id(r.get(2));
        setStation_name(r.get(3));
        setTimestamp(r.get(5));
        setAcquisition_timestamp(r.get(6));
        setGps_timestamp(r.get(7));
        setLatitude(r.get(8));
        setLongitude(r.get(9));
        setValues(r.get(11));
        setCommand(r.get(12));
    }

    //setter (controllo che gli attibuti rispettino certe condizioni e non siano nulli)
    public void setMessage_type(Object m_type) {
        if(m_type != null ){
            this.message_type = MessageType.valueOf(m_type.toString());
        }else{
            this.message_type= MessageType.NULL;
        }
    }

    public void setMessage_id(Object m_id) {
        if(m_id != null){
            this.message_id= Integer.parseInt(m_id.toString());
        }else{
            this.message_id= 0;
        }
    }

    public void setStation_name(Object station_name) {
        if(station_name != null){
            this.station_name = station_name.toString();
        }else{
            this.station_name= "";
        }

    }

    public void setTimestamp(Object timestamp) {
        if(timestamp != null){
            this.timestamp = java.sql.Timestamp.valueOf(convertTime(timestamp));
        }else{
            this.timestamp = null;
        }
    }

    public static String convertTime(Object timestamp) {
        String[] data= timestamp.toString().split("");
        String dataConverted= (data[6]+""+data[7]+""+data[8]+""+data[9]+"-"+data[3]+""+data[4]+"-"+data[0]+""+data[1]+" "+data[11]+""+data[12]+":"+data[14]+""+data[15]+":"+data[17]+""+data[18]);

        return dataConverted;
    }

    public void setAcquisition_timestamp(Object acquisition_timestamp) {
        if (acquisition_timestamp != null){
            this.acquisition_timestamp = java.sql.Timestamp.valueOf(convertTime(acquisition_timestamp));
        }else{
            this.acquisition_timestamp = null;
        }
    }

    public void setGps_timestamp(Object gps_timestamp) {
        if(gps_timestamp != null) {
            this.gps_timestamp = java.sql.Timestamp.valueOf(convertTime(gps_timestamp));
        }else this.gps_timestamp= null;
    }

    public void setLatitude(Object latitude) {
        // la latitudine può andare da 0 a 90
        if(latitude != null && Float.parseFloat(latitude.toString()) <= 90 && Float.parseFloat(latitude.toString()) >= 0 ){
            this.latitude = Float.parseFloat(latitude.toString());
        }else this.latitude =-999 ;

    }

    public void setLongitude(Object longitude) {
        // la latitudine può andare da 0 a 180
        if(longitude != null && Float.parseFloat(longitude.toString()) <= 180 && Float.parseFloat(longitude.toString()) >= 0 ){
            this.longitude = Float.parseFloat(longitude.toString());
        }else this.longitude =-999 ;
    }

    public void setValues(Object val) {
        ArrayList<Value> values= new ArrayList<>();
        if (val != null){
            GenericData.Array v= (GenericData.Array)val;
            for(int i=0; i<v.size(); i++ ) {
                Value value = new Value();
                GenericRecord sensor = (GenericRecord) v.get(i);
                value.setValue(Double.parseDouble(sensor.get(0).toString()));
                value.setSensor_name(sensor.get(1).toString());
                values.add(value);
            }
            this.values= values;
        }else{
            this.values= null;
        }
    }

    public void setCommand(Object command) {
        if(command != null){
            this.command= command.toString();
        } else this.command = "";
    }

    //getter
    public int getMessage_id() {
        return message_id;
    }
    public MessageType getMessage_type() {
        return message_type;
    }

    public String getStation_name() {
        return station_name;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Timestamp getAcquisition_timestamp() {
        return acquisition_timestamp;
    }

    public Timestamp getGps_timestamp() {
        return gps_timestamp;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public ArrayList<Value> getValues() {
        return values;
    }

    public String getCommand() {
        return command;
    }
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
    public double getValue() {
        return value;
    }

    public String getSensor_name() {
        return sensor_name;
    }
}
