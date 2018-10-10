package com.matt.test.kafka.test;

import java.util.concurrent.ConcurrentHashMap;

public class MapTest {

    public static void main(String[] args) {
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap();
        map.put("1", "aa");
        map.put("2", "bb");
        map.put("3", "cc");
        for (String key:map.keySet()){
            System.out.println("value: "+ map.get(key));
        }
    }

}
