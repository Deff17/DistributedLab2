package com.company;

import java.util.Scanner;

public class App {

    public static void main(String[] args) throws Exception {

        System.setProperty("java.net.preferIPv4Stack", "true");

        Scanner scanner = new Scanner(System.in);

        DistributedMap map = new DistributedMap();


        try{
            while (true){
                String key;
                String value;
                System.out.println("Choose what to do:");
                String method = scanner.nextLine();
                switch (method){
                    case "contains":
                        System.out.println("Type key");
                        key = scanner.nextLine();
                        System.out.println(map.containsKey(key));
                        break;
                    case "get":
                        System.out.println("Type key");
                        key = scanner.nextLine();
                        System.out.println(map.get(key));
                        break;
                    case "put":
                        System.out.println("Type key");
                        key = scanner.nextLine();
                        System.out.println("Type value");
                        value = scanner.nextLine();
                        System.out.println(map.put(key,value));
                        break;
                    case "remove":
                        System.out.println("Type key");
                        key = scanner.nextLine();
                        System.out.println(map.remove(key));
                        break;
                    case "list":
                        map.list();
                        break;
                    default:
                        break;
                }
            }
        }finally {
            map.disconnect();
        }


    }
}