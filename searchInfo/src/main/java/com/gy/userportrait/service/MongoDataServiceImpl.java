package com.gy.userportrait.service;

import com.alibaba.fastjson.JSONObject;
import com.gy.userportrait.base.BaseMongo;
import com.mongodb.client.*;
import com.youfan.entity.AnalyResult;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class MongoDataServiceImpl extends BaseMongo {

    public List<AnalyResult> listMongoInfoBy(String tableName){

        List<AnalyResult> result = new ArrayList<>();
        MongoDatabase db = mongoClient.getDatabase("portrait");
        MongoCollection<Document> collection = db.getCollection(tableName);

        Document groupField = new Document();
        Document idFields = new Document();
        idFields.put("info","$info");
        groupField.put("_id",idFields);
        groupField.put("count",new Document("$sum","$count"));

        Document group = new Document("$group",groupField);

        Document projectFields = new Document();
        projectFields.put("_id",false);
        projectFields.put("info","$_id.info");
        projectFields.put("count",true);

        Document project = new Document("$project",projectFields);

        AggregateIterable<Document> iterater = collection.aggregate(
                (List<Document>) Arrays.asList(group, project)
        );

        MongoCursor<Document> cursor = iterater.iterator();
        while(cursor.hasNext()){
            Document document = cursor.next();
            String jsonString = JSONObject.toJSONString(document);
            System.out.println("mongodb数据源:"+jsonString);
            AnalyResult brandUser = JSONObject.parseObject(jsonString, AnalyResult.class);
            result.add(brandUser);
        }
        return result;
    }


    public static List<AnalyResult> findYearBase(String tableName){

        List<AnalyResult> result = new ArrayList<>();
        MongoDatabase db = mongoClient.getDatabase("portrait");
        MongoCollection<Document> collection = db.getCollection(tableName);

        FindIterable<Document> documents = collection.find();

        MongoCursor<Document> cursor = documents.iterator();
        while(cursor.hasNext()){
            Document document = cursor.next();
            String jsonString = JSONObject.toJSONString(document);
            Map dataMap = JSONObject.parseObject(jsonString, Map.class);
            AnalyResult brandUser = new AnalyResult();
            Integer count =(Integer) dataMap.get("count");
            String yearbasetype =(String) dataMap.get("yearbasetype");

            brandUser.setCount(Long.parseLong(count+""));
            brandUser.setInfo(yearbasetype);
            result.add(brandUser);
        }

        Collections.sort(result, new Comparator<AnalyResult>() {
            @Override
            public int compare(AnalyResult o1, AnalyResult o2) {
                return o1.getInfo().compareTo(o2.getInfo());

            }
        });


        return result;
    }

    public static List<AnalyResult> findCommon(String tableName){

        List<AnalyResult> result = new ArrayList<>();
        MongoDatabase db = mongoClient.getDatabase("portrait");
        MongoCollection<Document> collection = db.getCollection(tableName);

        FindIterable<Document> documents = collection.find();

        MongoCursor<Document> cursor = documents.iterator();
        while(cursor.hasNext()){
            Document document = cursor.next();
            String jsonString = JSONObject.toJSONString(document);
            Map dataMap = JSONObject.parseObject(jsonString, Map.class);
            AnalyResult brandUser = new AnalyResult();
            Integer count =(Integer) dataMap.get("count");
            String info =(String) dataMap.get("info");

            brandUser.setCount(Long.parseLong(count+""));
            brandUser.setInfo(info);
            result.add(brandUser);
        }

        Collections.sort(result, new Comparator<AnalyResult>() {
            @Override
            public int compare(AnalyResult o1, AnalyResult o2) {
                return o1.getInfo().compareTo(o2.getInfo());

            }
        });


        return result;
    }

    public static void main(String[] args) {
        List<AnalyResult> list = findYearBase("yearbasestatics");
        for (AnalyResult analyResult : list) {
            System.out.println(analyResult);
        }
    }


}
