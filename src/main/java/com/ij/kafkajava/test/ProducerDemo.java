package com.ij.kafkajava.test;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getName());
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // StringSerializer to encode the string to byte there is IntegerSerializer as will, ETC.
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer(prop);
        // we can add a key after the topic to make every message of
        // that key will go to the same partition every time
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "mmmm");
        // this a future we have to await.
        //producer.send(record);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    logger.info("Received data. \n" +
                            "Topic: "+ recordMetadata.topic() + "\n" +
                            "Partitions: "+ recordMetadata.partition() + "\n");
                } else {
                    logger.error("Error :" + e.getMessage());
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
