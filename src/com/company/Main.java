package com.company;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
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
            if (line.startsWith("log")) {
                map.log();
            }
        }
    }
}


class DistributedMap implements Receiver, SimpleStringMap {
    private JChannel mChannel;
    private HashMap<String, Integer> mLocalMap;

    DistributedMap() throws Exception {
        // creating channel and setting connection
        mChannel = new JChannel();
        // delegating receiver
        mChannel.setReceiver(this);
        mChannel.connect("michal-osadnik-channel");
        // initialazing set
        mLocalMap = new HashMap<>();
        // Setting initial state. If there's no other member, does nothing
        // http://www.jgroups.org/manual/html/user-channel.html#StateTransfer
        mChannel.getState(null, 0);
    }

    public boolean containsKey(String key) {
        return mLocalMap.containsKey(key);
    }

    void log() {
        System.out.println("state:");
        // iterating over set and writing state
        for (Map.Entry<String, Integer> item : mLocalMap.entrySet())
            System.out.println(item.getKey() + ": " + item.getValue());
    }

    public Integer get(String key) {
        return mLocalMap.get(key);
    }

    public void put(String key, Integer value) {
        try {
            // putting elements into set is dne by sending multicast message. Adding to local state is done on receiving
            mChannel.send(new org.jgroups.Message(null, new Message(MessageType.ADD, key, value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Integer remove(String key) {
        try {
            // removing is done in  a similar way as putting. Synchronizing prefaces local removing.
            mChannel.send(new org.jgroups.Message(null, new Message(MessageType.REMOVE, key)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mLocalMap.get(key);
    }

    void close() {
        mChannel.close();
    }

    public void receive(org.jgroups.Message msg) {
        // local state handling
        Message mapMsg = msg.getObject();
        if (mapMsg.mType == MessageType.ADD) {
            mLocalMap.put(mapMsg.mKey, mapMsg.mValue);
        } else if (mapMsg.mType == MessageType.REMOVE) {
            mLocalMap.remove(mapMsg.mKey);
        }
        // Also, printing after adding or removing
        log();
    }

    /***
     *
     * STATE_TRANSFER is the existing transfer protocol, which transfers byte[] buffers around.
     * However, at the state provider’s side, JGroups creates an output stream over the byte[] buffer,
     * and passes the ouput stream to the getState(OutputStream) callback, and at the state requester’s
     * side, an input stream is created and passed to the setState(InputStream) callback.
     */

    @Override
    public void setState(InputStream input) throws Exception {
        synchronized (mLocalMap) {
            // Overrode Receiver method for obtaining state by another member on joining or adding partition
            mLocalMap = Util.objectFromStream(new DataInputStream(input));
        }
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        synchronized (mLocalMap) {
            // Also, obtaining own state if needed
            Util.objectToStream(mLocalMap, new DataOutputStream(output));
        }
    }

    @Override
    public void viewAccepted(View view) {
        System.out.println("Accepted");
        // Called when a change in membership has occurred
        handleView(mChannel, view);
    }

    private static void handleView(JChannel channel, View view) {
        if (view instanceof MergeView) {
            // Following docs it has to be run in another thread
            ViewHandler handler = new ViewHandler(channel, (MergeView) view);
            handler.start();
        }
    }

    // followed by docs http://www.jgroups.org/manual/index.html#HandlingNetworkPartitions
    private static class ViewHandler extends Thread {
        JChannel mChannel;
        MergeView mView;

        private ViewHandler(JChannel channel, MergeView view) {
            mChannel = channel;
            mView = view;
        }

        public void run() {
            View firstView = mView.getSubgroups().get(0);
            Address local = mChannel.getAddress();
            if (!firstView.getMembers().contains(local)) {
                System.out.println("Not member of the new primary partition ("
                        + firstView + "), will re-acquire the state");
                try {
                    mChannel.getState(null, 30000);
                } catch (Exception ex) {
                }
            } else {
                System.out.println("Not member of the new primary partition ("
                        + firstView + "), will do nothing");
            }
        }
    }
}

// Message needs to be Serializable for transporting via a mChannel socket
class Message implements Serializable {
    MessageType mType;
    String mKey;
    Integer mValue;

    Message(MessageType type, String key, Integer value) {
        mType = type;
        mKey = key;
        mValue = value;
    }

    Message(MessageType type, String key) {
        mType = type;
        mKey = key;
    }
}
