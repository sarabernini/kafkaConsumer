package org.example;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.sql.Timestamp;

public class Message {

    enum MessageType {DATA, STATUS, CONTROL, MODEL, EXIT}

    //attributi
    private MessageType message_type;
        private int message_id;
    private String station_name;
    private java.sql.Timestamp timestamp;
    private java.sql.Timestamp acquisition_timestamp;
    private java.sql.Timestamp gps_timestamp;
    private float latitude;
    private float longitude;
    private ArrayList<Value> values;
    private ArrayList<ModelValues> model;
    private String command;

    //costruttori
    public Message(MessageType message_type, int message_id, String station_name, java.sql.Timestamp timestamp, java.sql.Timestamp acquisition_timestamp, java.sql.Timestamp gps_timestamp, float latitude, float longitude, ArrayList<Value> values, ArrayList<ModelValues> model, String command) {
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

    public Message(GenericRecord r) {
        setMessage_type(r.get(0));
        setMessage_id(r.get(1));
        setStation_name(r.get(2));
        setTimestamp(r.get(3));
        setAcquisition_timestamp(r.get(4));
        setGps_timestamp(r.get(5));
        setLatitude(r.get(6));
        setLongitude(r.get(7));
        setValues(r.get(8));
        setModel(r.get(9));
        setCommand(r.get(10));
    }

    //setter (controllo che gli attibuti rispettino certe condizioni e non siano nulli)
    public void setMessage_type(Object m_type) {
        if(m_type != null && (m_type.toString() =="DATA" ||m_type.toString() == "STATUS" || m_type.toString() == "CONTROL"|| m_type.toString() == "MODEL" || m_type.toString() == "EXIT")){
            this.message_type = MessageType.valueOf(m_type.toString());
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

    private String convertTime(Object timestamp) {
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

    public void setModel(Object mod) {
        ArrayList<ModelValues> modelValues= new ArrayList<>();
        if(mod != null){
            GenericData.Array mv= (GenericData.Array)mod;
            for(int i=0; i<mv.size(); i++ ) {
                GenericRecord model = (GenericRecord) mv.get(i);
                ModelValues modelValue = new ModelValues();
                modelValue.setPosition(Integer.parseInt(model.get(0).toString()));
                modelValue.setSensor_name(model.get(1).toString());
                modelValues.add(modelValue);
            }
            this.model= modelValues;
        }else this.model= null;

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
