package main;

import java.lang.System;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import recordutil.src.main.Record;


public class ProducerPerformance {
    public static void main(String[] args) throws Exception {
        long startApp = System.currentTimeMillis();
        if(args.length < 4) {
            System.out.println("Falta de argumentos, execução: " +
                    "java src.javaproduce.Producer <topic> <acks> <qntRecords> <size>");
            System.exit(1);
        }

        String topicName = args[0];
        String acks = args[1];
        int qntRecords = Integer.parseInt(args[2]);
        int size = Integer.parseInt(args[3]);
        int batchSize = Integer.parseInt(args[3]);

        System.out.println("Topic to send: " + topicName);
        System.out.println("Topic to acks: " + acks);
        System.out.println("Records: " + qntRecords);
        System.out.println("Size: " + size);
        System.out.println("Batch Size: " + batchSize);

        String message = genRecord(size);
        Properties props = newConfig(topicName, acks, qntRecords, batchSize);
        Producer<String, String> producer = new KafkaProducer<>(props);

        Vector<Record> records = new Vector<>();
        AtomicBoolean finished = new AtomicBoolean();
        finished.set(false);
        Sender sender = new Sender(records, finished);
        sender.setPriority(10);
        if (acks.equals("-2"))
            sender.start();

        try {
            long startMs = System.currentTimeMillis();
            ThroughputThrottler throttler = new ThroughputThrottler(-1, startMs);
            // Send messages;
            long startProduce = System.currentTimeMillis();
            for (int i = 1; i <= qntRecords; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, Integer.toString(i), message);
                long sendStartMs = System.currentTimeMillis();

                Timestamp stamp = new Timestamp(System.currentTimeMillis());
                Record _record = new Record("producer-1", topicName, i, qntRecords, record.key(),
                        record.value(), stamp.getTime());

                if (acks.equals("-2")) {
                    synchronized (records) {
                        records.add(_record);
                    }
                }

                record.setAfterTimestamp(stamp.getTime());
                //RecordMetadata metadata = producer.send(record, cb).get();
                producer.send(record);
                producer.flush();

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }

                Thread.sleep(5);
            }
	    
            long stopProduce = System.currentTimeMillis();
            ToolsUtils.printMetrics(producer.metrics());
            producer.close();
            finished.set(true);
            System.out.println("Produce Time: " + (stopProduce - startProduce) / 1000F);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        sender.join();
        System.out.println("Aplication time: " + (System.currentTimeMillis() - startApp) / 1000F);
    }

    private static Properties newConfig(String topicName, String acks, int qntRecords,
                                        int batchSize) {
        Properties props = new Properties();
        props.put(ProducerConfig.QNT_REQUESTS, qntRecords);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-1");
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.21.0.5:9092,172.21.0.6:9092,172.21.0.7:9092");
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.TOPIC_TO_SEND, topicName);
        if (acks.equals(-1) || acks.equals("all"))
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        //props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static String genRecord(int size) {
        int value = 0;
        String message = "";
        Random r = new Random();
        for (int i = 0; i < size; i++) {
            value = r.nextInt((90 - 65) + 1) + 65;
            message += (char) (value);
        }
        return message;
    }
}

