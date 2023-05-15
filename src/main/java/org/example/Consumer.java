package org.example;
import org.apache.avro.Schema;
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


import java.io.*;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    //attributi
    private Schema schema ;
    private DatumReader<GenericRecord> datumReader;
    private ArrayList<Message> messageList;
    private Project project;
    private DBConnector dbConnector;

    //costruttore
    public Consumer(ArrayList<Message> messageList, Project p, DBConnector dbc) throws IOException {
        this.schema = new Schema.Parser().parse(new File("src/main/java/org/example/avroschema.avsc"));
        this.datumReader = new GenericDatumReader<>(schema);
        this.messageList = messageList;
        this.project= p;
        this.dbConnector= dbc;
    }

    //metodo che legge i dati dal sensore
    public void readData() throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        Properties props = new Properties();
        props.put("bootstrap.servers", "magentatest.servicebus.windows.net:9093");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://magentatest.servicebus.windows.net/;SharedAccessKeyName=listenonly;SharedAccessKey=dVqm1G6eIYDt772MIW9+tlAfkRrY2+D27+AEhLBuU8Y=;EntityPath=airqino\";");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("group.id", "$Default");
        // da asserire quando si comunica con il db
        props.put("enable.auto.commit", "true");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);

        consumer.subscribe(Collections.singletonList("airqino"));
        int c= 0;

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(0);
                for (ConsumerRecord<String, byte[]> record : records) {
                    this.deserialize(record);
                    System.out.println(c++);
                }
            }
        } catch (IOException e){
            System.out.println("end");
        }finally {
            consumer.close();
        }
    }
    //metodo che deserializza i dati ricevuti, li traduce in un oggetto messaggio che memorizza in una lista di messaggi
    public void deserialize(ConsumerRecord<String, byte[]> record) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
        GenericRecord r= this.datumReader.read(null, decoder);
       //if(project.conteins(r.get(2))){
            Message message= new Message(r);
            messageList.add(message);
        //}
    }

    public void addMessagesToDatabase(){
        int i=0;
        for(Message m: messageList){
            dbConnector.insertValuesInMessage(m.getMessage_type(), m.getMessage_id(), m.getStation_name(),m.getTimestamp(), m.getAcquisition_timestamp(),m.getGps_timestamp(),m.getLatitude(),m.getLongitude(), i++);
            for(Value v:m.getValues()){
                dbConnector.insertValuesInValues(m.getMessage_id(),v.getSensor_name(),v.getValue());
            }
            for(ModelValues mv: m.getModel()){
                 dbConnector.insertValuesInModelValues(m.getMessage_id(),mv.getSensor_name(),mv.getPosition());
           }
        }

    }

}

