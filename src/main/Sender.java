package main;

import recordutil.src.main.Record;

import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

public class Sender extends Thread {

    Vector<Record> records;

    AtomicBoolean finished;

    public Sender(Vector<Record> records, AtomicBoolean finished) {
        this.records = records;
        this.finished = finished;
    }

    @Override
    public void run() {
        boolean running = true;
        try {
            while (running) {
                synchronized (records) {
                    if (finished.get() == true) {
                        makeSends();
                        running = false;
                    }
                    if (records.size() >= 2048) {
                        makeSends();
                        records.clear();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Exception send() {
        Exception e = null;
        try {
            Socket socket = new Socket("172.21.0.8", 6666);
            //socket.setSendBufferSize(Integer.MAX_VALUE);
            socket.setSoTimeout(1750);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(records);
            //socket.close();
        } catch (Exception ex) {
            e = ex;
        }
        return e;
    }

    public void makeSends() {
        if (send() != null) {
            send();
        }
    }
}
