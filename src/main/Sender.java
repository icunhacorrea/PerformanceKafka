package main;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import recordutil.src.main.Record;

import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

public class Sender extends Thread {

    Vector<Record> records;

    AtomicBoolean finished;

    Ignite ignite;

    IgniteCache<String, String> cache;

    public Sender(Vector<Record> records, AtomicBoolean finished) {
        this.records = records;
        this.finished = finished;
    }

    @Override
    public void run() {
        boolean running = true;

        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder tcMp = new TcpDiscoveryMulticastIpFinder();
        IgniteConfiguration cfg = new IgniteConfiguration();
        tcMp.setAddresses(Arrays.asList("monitor1"));
        spi.setIpFinder(tcMp);
        cfg.setDiscoverySpi(spi);
        cfg.setClientMode(true);

        this.ignite = Ignition.start(cfg);
        this.cache = ignite.getOrCreateCache("cache");

        while (running) {
            synchronized (records) {
                if (finished.get() == true)
                    running = false;
                if (records.size() > 24) {

                    records.forEach(_record -> cache.put(_record.getOrigem() + ";" + _record.getDestino() + ";" +
                            _record.getIdSeq(), _record.getTimeStamp() + ";" + _record.getQntRecords() + ";" +
                            _record.getKey() + ";" + _record.getValue()));
                    records.clear();
                }
            }
        }
        ignite.close();
    }
}
