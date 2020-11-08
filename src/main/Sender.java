package main;

import recordutil.src.main.Record;

import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
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

        SocketAddress sockAddr = new InetSocketAddress("172.21.0.8", 6666);
        Socket socket = new Socket();
        try {
            socket.setSendBufferSize(Integer.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            while (running) {
                synchronized (records) {
                    if (finished.get() == true) {
                        send(socket, sockAddr);
                        running = false;
                    }
                    if (records.size() >= 128) {
                        send(socket, sockAddr);
                        records.clear();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void send(Socket socket, SocketAddress sockAddr) {
        try {
            //Socket socket = new Socket("monitor1", 6666);
            //socket.setSendBufferSize(Integer.MAX_VALUE);
            socket.connect(sockAddr);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(records);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
