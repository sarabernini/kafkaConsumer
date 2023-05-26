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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    //attributi
    private Schema schema ;
    private DatumReader<GenericRecord> datumReader;
    private ArrayList<Message> messageList;
    private ArrayList<Project> projectList;
    private DBConnector dbConnector;

    //costruttore
    public Consumer(ArrayList<Message> messageList, ArrayList<Project> p, DBConnector dbc) throws IOException {
        this.schema = new Schema.Parser().parse(new File("src/main/java/org/example/avroschema.avsc"));
        this.datumReader = new GenericDatumReader<>(schema);
        this.messageList = messageList;
        this.projectList= p;
        this.dbConnector= dbc;
    }

    //metodo che legge i dati dal sensore
    public void readData() {
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
        props.put("auto.offset.reset", "latest");
        props.put("max.poll.records", "200");
        props.put("fetch.min.bytes", "1");
        props.put("fetch.max.wait.ms", "200");
        props.put("max.poll.interval.ms", "300000");


        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Collections.singletonList("airqino"));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(0);
                for (ConsumerRecord<String, byte[]> record : records) {
                    this.deserialize(record, consumer);
                }
                addMessagesToDatabase();
                messageList.clear();
            }
    }
    //metodo che deserializza i dati ricevuti, li traduce in un oggeto messaggio che memorizza in una lista di messaggi
   public void deserialize(ConsumerRecord<String, byte[]> record, KafkaConsumer consumer) {
        try{
            Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
            GenericRecord r = null;
            r = this.datumReader.read(null, decoder);
            for(Project project: projectList){
                if (project.contains(r.get(2))) {
                    Message message = new Message(r);
                    messageList.add(message);
                }
            }

        } catch (Exception e) {
            System.out.println("errore");
        }
    }

    public void addMessagesToDatabase(){
        for(Message m: messageList){
            if(dbConnector.insertValuesInMessage(m.getMessage_type(), m.getStation_name(),m.getTimestamp(), m.getAcquisition_timestamp(),m.getGps_timestamp(),m.getLatitude(),m.getLongitude())) {
                for (Value v : m.getValues()) {
                    dbConnector.insertValuesInValues(m.getStation_name(), m.getTimestamp(), v.getSensor_name(), v.getValue());
                }
                for (ModelValues mv : m.getModel()) {
                    dbConnector.insertValuesInModelValues(m.getStation_name(),m.getTimestamp(), mv.getSensor_name(), mv.getPosition());
                }
            }else{
                System.out.println(m.getStation_name()+": "+m.getTimestamp());
            }
        }

    }

}

