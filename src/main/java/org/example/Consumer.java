package org.example;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


public class Consumer {
    Schema schema ;
    DatumReader<GenericRecord> datumReader;
    ArrayList<Message> messageList;



    public Consumer(ArrayList<Message> messageList) throws IOException {
        this.schema = new Schema.Parser().parse(new File("src/main/java/org/example/avroschema.avsc"));
        this.datumReader = new GenericDatumReader<>(schema);
        this.messageList = messageList;
    }

    //metodo che legge i dati dal sensore
    public void readData() throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        Properties props = new Properties();
        props.put("bootstrap.servers", "magentatest.servicebus.windows.net:9093");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://magentatest.servicebus.windows.net/;SharedAccessKeyName=listenonly;SharedAccessKey=dVqm1G6eIYDt772MIW9+tlAfkRrY2+D27+AEhLBuU8Y=;EntityPath=airqino\";");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("group.id", "$Default");
        // da asserire quando si comunica con il db
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

        consumer.subscribe(Collections.singletonList("airqino"));

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(10);
                for (ConsumerRecord<String, byte[]> record : records) {
                    this.deserialize(record);
                }
            }
        } finally {
            consumer.close();
        }
    }
    //metodo che deserializza i dati ricevuti, li traduce in un oggetto messaggio che memorizza in una lista di messaggi
    public void deserialize(ConsumerRecord<String, byte[]> record) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
        GenericRecord r= this.datumReader.read(null, decoder);

        Message.MessageType message_type = Message.MessageType.valueOf(r.get(0).toString());
        int message_id =Integer.valueOf(r.get(1).toString());
        String station_name =String.valueOf(r.get(2));
        String timestamp=String.valueOf(r.get(3));
        String acquisition_timestamp=String.valueOf(r.get(4));
        String gps_timestamp=String.valueOf(r.get(5));
        float latitude= Float.valueOf(r.get(6).toString());
        float longitude= Float.valueOf(r.get(7).toString());
        ArrayList<Value> values= new ArrayList<>();
        GenericData.Array v= (GenericData.Array) r.get("values");
        for(int i=0; i<v.size(); i++ ){
            Value value= new Value();
            GenericRecord sensor= (GenericRecord) v.get(i);
            value.setValue(Double.parseDouble(sensor.get(0).toString()));
            value.setSensor_name(String.valueOf(sensor.get(1)));
            values.add(value);
        }
        ArrayList<ModelValues> modelValues= new ArrayList<>();
        GenericData.Array mv= (GenericData.Array) r.get(9);
        for(int i=0; i<mv.size(); i++ ){
            GenericRecord model= (GenericRecord) mv.get(i);
            ModelValues modelValue = new ModelValues();
            modelValue.setPosition(Integer.valueOf(model.get(0).toString()));
            modelValue.setSensor_name(String.valueOf(model.get(1)));
        }
        String command = String.valueOf(r.get(10));
        Message message= new Message(message_type,message_id,station_name,timestamp,acquisition_timestamp,gps_timestamp,latitude,longitude,values,modelValues,command);
        messageList.add(message);
    }

}

