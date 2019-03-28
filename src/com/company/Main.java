package com.company;

import org.jgroups.*;
import org.jgroups.util.Util;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.logging.LogManager;

enum MsgType {
    ADD,
    REMOVE,
}

public class Main {
private static DistributedMap map;
    public static void main(String[] args) throws Exception {
        LogManager.getLogManager().reset();
        System.setProperty("java.net.preferIPv4Stack","true");
        map = new DistributedMap("michal-osadnik-software");
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.print("> "); System.out.flush();
            String line=in.readLine().toLowerCase();
            if(line.startsWith("quit") || line.startsWith("exit")) {
                map.close();
                return;
            }
            if(line.startsWith("put")){
                map.put(line.split(" ")[1], Integer.parseInt(line.split(" ")[2]));
            } else
            if(line.startsWith("get")){
                System.out.println("result: "+map.get(line.split(" ")[1]));
            } else
            if(line.startsWith("containsKey")){
                System.out.println("result: "+map.containsKey(line.split(" ")[1]));
            } else
            if(line.startsWith("remove")){
                map.remove(line.split(" ")[1]);
            }
            if(line.startsWith("map")){
                map.logMapState();
            }
        }
    }
}


class DistributedMap implements Receiver {
    private JChannel jchannel;
    private HashMap<String, Integer> localMap;
    DistributedMap(String channelId) throws Exception {
        jchannel = new JChannel();
        jchannel.setReceiver(this);
        jchannel.connect(channelId);
        localMap = new HashMap<>();
        jchannel.getState(null, 0);
    }

    boolean containsKey(String key) {
        return localMap.containsKey(key);
    }


    void logMapState(){
        System.out.println("MAP:");
        for (Map.Entry<String,Integer> item:localMap.entrySet())
            System.out.println(item.getKey()+": "+item.getValue());
    }

    Integer get(String key) {
        return localMap.get(key);
    }

    void put(String key, Integer value) {
        try {
            jchannel.send(new org.jgroups.Message(null, new Message(MsgType.ADD, key, value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void remove(String key) {
        try {
            jchannel.send(new org.jgroups.Message(null, new Message(MsgType.REMOVE, key)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void close(){
        jchannel.close();
    }

    public void receive(org.jgroups.Message msg) {
        Message mapMsg = msg.getObject();
        if(mapMsg.type == MsgType.ADD){
            localMap.put(mapMsg.key, mapMsg.value);
        } else
        if(mapMsg.type == MsgType.REMOVE){
            localMap.remove(mapMsg.key);
        }
        logMapState();
    }

    public void getState(OutputStream output) throws Exception {
        synchronized (localMap) {
            Util.objectToStream(localMap, new DataOutputStream(output));
        }
    }

    public void setState(InputStream input) throws Exception {
        synchronized (localMap) {
            localMap = Util.objectFromStream(new DataInputStream(input));
        }
    }

    public void viewAccepted(View view) {
        handleView(jchannel, view);
    }
    // http://www.jgroups.org/manual/index.html#HandlingNetworkPartitions
    private static void handleView(JChannel ch, View new_view) {
        if(new_view instanceof MergeView) {
            ViewHandler handler = new ViewHandler(ch, (MergeView)new_view);
            handler.start();
        }
    }

    private static class ViewHandler extends Thread {
        JChannel ch;
        MergeView view;

        private ViewHandler(JChannel ch, MergeView view) {
            this.ch = ch;
            this.view = view;
        }

        public void run() {
            Vector<View> subgroups = (Vector<View>) view.getSubgroups();
            View tmp_view = subgroups.firstElement(); // picks the first
            Address local_addr = ch.getAddress();
            if(!tmp_view.getMembers().contains(local_addr)) {
                System.out.println("dropping own state");
                try {
                    ch.getState(null, 10000);
                }
                catch(Exception ex) {
                }
            }
            else {
                System.out.println("doing nothing");
            }
        }
    }
}

class Message implements Serializable {
    MsgType type;
    String key;
    Integer value;

    Message(MsgType type, String key, Integer value){
        this.type = type;
        this.key = key;
        this.value = value;
    }

    Message(MsgType type, String key){
        this.type = type;
        this.key = key;
    }
}
