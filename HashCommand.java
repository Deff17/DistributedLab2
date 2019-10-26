package com.company;

import java.io.Serializable;

public class HashCommand implements Serializable{

    private String method;
    private String key;
    private String value;

    public HashCommand(String method, String key, String value){
        this.method = method;
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getMethod() {
        return method;
    }

    public String getValue() {
        return value;
    }
}
