package main;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import recordutil.src.main.Record;

public class Difuser<K, V> implements ProducerInterceptor<K, V> {

    String origem;
    String destino;
    String acks;
    int qntRecords, idSeq = 0;

    Ignite ignite;
    IgniteCache<String, String> cache;

    Timestamp stamp;

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        idSeq++;
        stamp = new Timestamp(System.currentTimeMillis());
        Record _record = new Record(origem, destino, idSeq, qntRecords, record.key().toString(),
                record.value().toString(), stamp.getTime());
        if (acks.equals("-2"))
            this.cache.put(_record.getOrigem() + ";" + _record.getDestino() + ";" +
                    _record.getIdSeq(), _record.getTimeStamp() + ";" + _record.getQntRecords() + ";" +
                    _record.getKey() + ";" + _record.getValue());
        record.setAfterTimestamp(stamp.getTime());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) { }

    @Override
    public void close() {
        this.ignite.close();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Map<String, Object> config = new HashMap<>(configs);

        this.origem = config.get(ProducerConfig.CLIENT_ID_CONFIG).toString();
        this.destino = config.get(ProducerConfig.TOPIC_TO_SEND).toString();
        this.qntRecords = (Integer) config.get(ProducerConfig.QNT_REQUESTS);
        this.acks = (String) config.get(ProducerConfig.ACKS_CONFIG);


        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder tcMp = new TcpDiscoveryMulticastIpFinder();
        IgniteConfiguration cfg = new IgniteConfiguration();
        tcMp.setAddresses(Arrays.asList("localhost"));
        spi.setIpFinder(tcMp);
        cfg.setDiscoverySpi(spi);
        cfg.setClientMode(true);

        this.ignite = Ignition.start(cfg);
        this.cache = ignite.getOrCreateCache("cache");

    }
}
