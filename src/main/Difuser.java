package main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.sql.Timestamp;

import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import recordutil.src.main.Record;

public class Difuser<K, V> implements ProducerInterceptor<K, V> {
    ObjectOutputStream oos;
    Socket socket;
    int port = 6666;
    InetAddress ip;

    String origem;
    String destino;
    String acks;
    int idSeq = 0, qntRecords;
    int GRAO = 100;
    List<Record> records = new ArrayList<>();

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        idSeq++;
        Timestamp stamp = new Timestamp(System.currentTimeMillis());
        Record _record = new Record(origem, destino, idSeq, qntRecords, record.key().toString(),
                record.value().toString(), stamp.getTime());
<<<<<<< HEAD
        record.setAfterTimestamp(stamp.getTime());
	System.out.println("Entrei");
=======

>>>>>>> 986fbfe952f4fc905a3fdd38dd963103b8dd5e3b
        if (acks.equals("-2")) {
            /*records.add(_record);
            if (records.size() == GRAO) {
                try {
                    socket = new Socket(ip, port);
                    socket.setSendBufferSize(Integer.MAX_VALUE);
                    oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(records);
                    records.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }*/
            try {
                oos.writeObject(_record);
                oos.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        record.setAfterTimestamp(stamp.getTime());
        return record;
    }

    @Override
<<<<<<< HEAD
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) { 
   	//System.out.println(String.format("onAck topic=%s, part=%d, offset=%d\n",
        //        metadata.topic(), metadata.partition(), metadata.offset()
        //));
	return;
    }
=======
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) { }
>>>>>>> 986fbfe952f4fc905a3fdd38dd963103b8dd5e3b

    @Override
    public void close() { }

    @Override
    public void configure(final Map<String, ?> configs) {
        Map<String, Object> config = new HashMap<>(configs);

        origem = config.get(ProducerConfig.CLIENT_ID_CONFIG).toString();
        destino = config.get(ProducerConfig.TOPIC_TO_SEND).toString();

        qntRecords = (Integer) config.get(ProducerConfig.QNT_REQUESTS);
        acks = (String) config.get(ProducerConfig.ACKS_CONFIG);

        try {
<<<<<<< HEAD
            this.ip = InetAddress.getByName("14.0.0.4");

        } catch (UnknownHostException e) {
=======
            ip = InetAddress.getByName("localhost");
            socket = new Socket(ip, port);
            socket.setSendBufferSize(Integer.MAX_VALUE);
            socket.setKeepAlive(true);
            oos = new ObjectOutputStream(socket.getOutputStream());
        } catch (Exception e) {
>>>>>>> 986fbfe952f4fc905a3fdd38dd963103b8dd5e3b
            e.printStackTrace();
        }
    }
}