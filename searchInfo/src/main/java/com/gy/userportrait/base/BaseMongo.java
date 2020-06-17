package com.gy.userportrait.base;


import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.youfan.utils.ReadProperties;

import java.util.ArrayList;
import java.util.List;

public class BaseMongo {

    protected static MongoClient mongoClient;

    static {
        List<ServerAddress> addresses = new ArrayList<>();
        String [] mongoaddr = ReadProperties.getKey("mongoaddr", "search.properties").split(",");
        String [] portList = ReadProperties.getKey("mongoport", "search.properties").split(",");
        for(int i = 0 ; i < mongoaddr.length ;i++){
            ServerAddress address = new ServerAddress(mongoaddr[i],Integer.parseInt(portList[i]));
            addresses.add(address);
        }
        mongoClient = new MongoClient(addresses);
    }
}
