package com.company;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.logging.LogManager;

enum MessageType {
    ADD,
    REMOVE,
}

interface SimpleStringMap {
    boolean containsKey(String key);

    Integer get(String key);

    void put(String key, Integer value);

    Integer remove(String key);
}



public class Main {
    private static DistributedMap map;
    public static void main(String[] args) throws Exception {
        // Removing useless logs
        LogManager.getLogManager().reset();
        System.setProperty("java.net.preferIPv4Stack", "true");
        // preparing new map
        map = new DistributedMap();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("C: ");
            String line = in.readLine().toLowerCase();
            if (line.startsWith("quit") || line.startsWith("exit")) {
                map.close();
                return;
            }
            if (line.startsWith("put")) {
                map.put(line.split(" ")[1], Integer.parseInt(line.split(" ")[2]));
            } else if (line.startsWith("get")) {
                System.out.println("result: " + map.get(line.split(" ")[1]));
            } else if (line.startsWith("containsKey")) {
                System.out.println("result: " + map.containsKey(line.split(" ")[1]));
            } else if (line.startsWith("remove")) {
                map.remove(line.split(" ")[1]);
            }
            if (line.startsWith("map")) {
                map.logMapState();
            }
        }
    }
}


class DistributedMap implements Receiver, SimpleStringMap {
    private JChannel jchannel;
    private HashMap<String, Integer> localMap;

    DistributedMap() throws Exception {
        // creating channel and setting connection
        jchannel = new JChannel();
        // delegating receiver
        jchannel.setReceiver(this);
        jchannel.connect("michal-osadnik-channel");
        // initialazing set
        localMap = new HashMap<>();
        // Setting initial state. If there's no other member, does nothing
        // http://www.jgroups.org/manual/html/user-channel.html#StateTransfer
        jchannel.getState(null, 0);
    }

    public boolean containsKey(String key) {
        return localMap.containsKey(key);
    }

    public void logMapState() {
        System.out.println("state:");
        // iterating over set and writing state
        for (Map.Entry<String, Integer> item : localMap.entrySet())
            System.out.println(item.getKey() + ": " + item.getValue());
    }

    public Integer get(String key) {
        return localMap.get(key);
    }

    public void put(String key, Integer value) {
        try {
            // putting elements into set is dne by sending multicast message. Adding to local state is done on receiving
            jchannel.send(new org.jgroups.Message(null, new Message(MessageType.ADD, key, value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Integer remove(String key) {
        try {
            // removing is done in  a similar way as putting. Synchronizing prefaces local removing.
            jchannel.send(new org.jgroups.Message(null, new Message(MessageType.REMOVE, key)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this.localMap.get(key);
    }

    void close() {
        jchannel.close();
    }

    public void receive(org.jgroups.Message msg) {
        // local state handling
        Message mapMsg = msg.getObject();
        if (mapMsg.type == MessageType.ADD) {
            localMap.put(mapMsg.key, mapMsg.value);
        } else if (mapMsg.type == MessageType.REMOVE) {
            localMap.remove(mapMsg.key);
        }
        // Also, printing after adding or removing
        logMapState();
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        synchronized (localMap) {
            // Overrode receiver method for obtaining state by another member on joining or adding partition
            Util.objectToStream(localMap, new DataOutputStream(output));
        }
    }

    @Override
    public void setState(InputStream input) throws Exception {
        synchronized (localMap) {
            // Also, setting own state if needed
            localMap = Util.objectFromStream(new DataInputStream(input));
        }
    }

    @Override
    public void viewAccepted(View view) {
        System.out.println("Accepted");
        // Called when a change in membership has occurred
        handleView(jchannel, view);
    }

    private static void handleView(JChannel channel, View view) {
        if(view instanceof MergeView) {
            ViewHandler handler = new ViewHandler(channel, (MergeView)view);
            handler.start();
        }
    }

    // followed by docs http://www.jgroups.org/manual/index.html#HandlingNetworkPartitions
    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch = ch;
            this.view = view;
        }

        public void run() {
            Vector<View> subgroups = (Vector<View>) view.getSubgroups();
            View tmp_view = subgroups.firstElement();
            Address local_addr = ch.getAddress();
            if (!tmp_view.getMembers().contains(local_addr)) {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will re-acquire the state");
                try {
                    ch.getState(null, 30000);
                } catch (Exception ex) {
                }
            } else {
                System.out.println("Not member of the new primary partition ("
                        + tmp_view + "), will do nothing");
            }
        }
    }
}

// Message needs to be Serializable for transporting via a jchannel socket
class Message implements Serializable {
    MessageType type;
    String key;
    Integer value;

    Message(MessageType type, String key, Integer value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    Message(MessageType type, String key) {
        this.type = type;
        this.key = key;
    }
}
