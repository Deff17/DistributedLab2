package com.company;


import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;



import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class DistributedMap implements SimpleStringMap {

    JChannel channel;

    private final HashMap<String,String> map = new HashMap<>();

    public DistributedMap() throws Exception {
        channel = new JChannel();
        //channel = new JChannel(false);

        ProtocolStack stack = new ProtocolStack();
        channel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.43")))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 240000).setValue("interval", 3000))
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new SEQUENCER())
                .addProtocol(new STATE());
        stack.init();

        channel.connect("DPChannel");
        channel.setReceiver(new ReceiverAdapter() {

            public void receive(Message msg) {
                HashCommand hashCommand = (HashCommand) msg.getObject();
                switch (hashCommand.getMethod()) {
                    case "put":
                        System.out.println("put method was received");
                        map.put(hashCommand.getKey(), hashCommand.getValue());
                        break;
                    case "remove":
                        System.out.println("remove method was received");
                        map.remove(hashCommand.getKey());
                        break;
                    default:
                        break;
                }
            }

            public void getState(OutputStream output) throws Exception {
                synchronized(map) {
                    Util.objectToStream(map, new DataOutputStream(output));
                }
            }

            public void setState(InputStream input) throws Exception {
                HashMap<String,String> hashMap;
                hashMap = (HashMap<String,String>) Util.objectFromStream(new DataInputStream(input));
                synchronized(map) {
                    map.clear();
                    map.putAll(hashMap);
                }
                System.out.println(map.size() + " values in map history:");
                map.entrySet().stream().forEach(System.out::println);
            }

            public void viewAccepted(View new_view) {
                handleView(channel, new_view);
            }

        });
        channel.getState(null,240000);

    }

    private static void handleView(JChannel ch, View new_view) {
        if(new_view instanceof MergeView) {
            ViewHandler handler=new ViewHandler(ch, (MergeView)new_view);
            // requires separate thread as we don't want to block JGroups
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
            List<View> subgroups = view.getSubgroups();
            View tmp_view = subgroups.get(0); // picks the first
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

    @Override
    public boolean containsKey(String key) {
        synchronized (map){
            return map.containsKey(key);
        }
    }

    @Override
    public String get(String key) {
        synchronized (map){
            return map.get(key);
        }
    }

    @Override
    public String put(String key, String value) {
        synchronized (map){

            HashCommand hashCommand = new HashCommand("put",key,value);

            try {
                channel.send(null, hashCommand);
                System.out.println("Put hashCommand was send");
            } catch (Exception e) {
                e.printStackTrace();
            }

            return map.put(key,value);
        }
    }

    @Override
    public String remove(String key) {
        synchronized (map){

            HashCommand hashCommand = new HashCommand("remove",key,"");

            try {
                channel.send(null, hashCommand);
                System.out.println("Remove hashCommand was send");
            } catch (Exception e) {
                e.printStackTrace();
            }

            return map.remove(key);
        }
    }


    public void list(){
         this.map.entrySet().stream().forEach(System.out::println);
    }

    public void disconnect() {
        channel.close();
    }

}
