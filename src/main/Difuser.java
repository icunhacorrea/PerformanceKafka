package main;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.sql.Timestamp;

import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import recordutil.src.main.Record;

public class Difuser<K, V> implements ProducerInterceptor<K, V> {
    ObjectOutputStream oos;
    Socket socket;
    int port;
    InetAddress ip;

    String origem;
    String destino;
    int idSeq = 0, qntRecords;

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        System.out.println("eNTRANDO NO ON SEND.");
        idSeq++;
        Timestamp stamp = new Timestamp(System.currentTimeMillis());
        Record _record = new Record(origem, destino, idSeq, qntRecords, record.key().toString(),
                record.value().toString(), stamp.getTime());
        record.setAfterTimestamp(stamp.getTime());
        try {
            socket = new Socket(ip, port);
            oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(_record);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) { return; }

    @Override
    public void close() { return; }

    @Override
    public void configure(final Map<String, ?> configs) {
        Map<String, Object> config = new HashMap<>(configs);

        port = 6666;
        origem = config.get(ProducerConfig.CLIENT_ID_CONFIG).toString();
        destino = config.get(ProducerConfig.TOPIC_TO_SEND).toString();

        qntRecords = (Integer) config.get(ProducerConfig.QNT_REQUESTS);

        try {
            this.ip = InetAddress.getByName("localhost");

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
