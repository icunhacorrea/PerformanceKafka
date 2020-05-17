package main;

import java.io.ObjectOutputStream;
import java.lang.System;
import java.net.Socket;
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
        if(args.length != 4) {
            System.out.println("Falta de argumentos, execução: " +
                    "java src.javaproduce.Producer <topic> <acks> <qntRecords> <size>");
            System.exit(1);
        }

        String topicName = args[0];
        String acks = args[1];
        int qntRecords = Integer.parseInt(args[2]);
        int size = Integer.parseInt(args[3]);

        System.out.println("Topic to send: " + topicName);
        System.out.println("Topic to acks: " + acks);
        System.out.println("Records: " + qntRecords);
        System.out.println("Size:" + size);

        String message = genRecord(size);
        Properties props = newConfig(topicName, acks, qntRecords);
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Variáveis compartilhadas.
        Vector<Record> records = new Vector<>();
        AtomicBoolean finished = new AtomicBoolean();
        finished.set(false);

        Thread difuser = new Thread(new Runnable() {
            @Override
            public void run() {
		boolean running = true;
                try {
                    while (running) {
                        synchronized (records) {
                            if (records.size() > 99) {
                                send(records);
                                records.clear();
                            }
                        }
                        if (finished.get() == true) {
			    send(records);
                            running = false;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        difuser.start();

<<<<<<< HEAD
        try {
            Random rd = new Random();
            Stats stats = new Stats(qntRecords, 5000);
            long startMs = System.currentTimeMillis();
            ThroughputThrottler throttler = new ThroughputThrottler(-1, startMs);
            // Send messages;
            long startProduce = System.currentTimeMillis();
            for (int i = 0; i < qntRecords; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, Integer.toString(i), message);
                long sendStartMs = System.currentTimeMillis();
                Callback cb = stats.nextCompletion(sendStartMs, message.length(), stats);

                Timestamp stamp = new Timestamp(System.currentTimeMillis());
                Record _record = new Record("producer", topicName, i + 1, qntRecords, record.key(),
                        record.value(), stamp.getTime());
                if (acks.equals("-2")) {
                    synchronized (records) {
                        records.add(_record);
                    }
                }
                record.setAfterTimestamp(stamp.getTime());
                RecordMetadata metadata = producer.send(record, cb).get();

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
                //Thread.sleep(rd.nextInt(11) + 10);
=======
            try {
                producer.send(record, cb);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
>>>>>>> 986fbfe952f4fc905a3fdd38dd963103b8dd5e3b
            }
            long stopProduce = System.currentTimeMillis();
            producer.flush();
            stats.printTotal();
            ToolsUtils.printMetrics(producer.metrics());
            producer.close();
            finished.set(true);
            System.out.println("Produce Time: " + (stopProduce - startProduce) / 1000F);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
	difuser.join();
        System.out.println("Aplication time: " + (System.currentTimeMillis() - startApp) / 1000F);
    }

    private static void send(List<Record> records) {
        try {
            Socket socket = new Socket("14.0.0.4", 6666);
            socket.setSendBufferSize(Integer.MAX_VALUE);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(records);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties newConfig(String topicName, String acks, int qntRecords) {
        Properties props = new Properties();
        props.put(ProducerConfig.QNT_REQUESTS, qntRecords);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092");
        props.put(ProducerConfig.ACKS_CONFIG, acks);
	if (acks.equals(-1) || acks.equals(-1))
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        props.put(ProducerConfig.TOPIC_TO_SEND, topicName);
<<<<<<< HEAD
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 11000);
=======
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 11000);
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
>>>>>>> 986fbfe952f4fc905a3fdd38dd963103b8dd5e3b
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

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }
}

