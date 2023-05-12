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

enum ProjectName {UIA}
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
                ConsumerRecords<String, byte[]> records = consumer.poll(1000);
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
        Message message= new Message(r);
        System.out.println(message.getMessage_id());
        this.messageList.add(message);
    }

    public ArrayList<String> selectProject(ProjectName name) {
        ArrayList<String> stationsName = new ArrayList<>();
        if (name == ProjectName.UIA) {
            stationsName.add("");
        }
    return  stationsName;
    }

}

