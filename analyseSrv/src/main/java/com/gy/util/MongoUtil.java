package com.gy.util;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

public class MongoUtil {

    private static MongoClient mongoClient = new MongoClient("hadoop102",27017);

    public static Document findOneBy(String tableName,String database , String yearBaseType){
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
        Document doc = new Document();
        doc.put("info", yearBaseType);
        FindIterable<Document> itrer = mongoCollection.find(doc);
        MongoCursor<Document> mongoCursor = itrer.iterator();
        if(mongoCursor.hasNext()){
            return mongoCursor.next();
        }else{
            return null;
        }
    }


    public static void main(String[] args) {

        Document document = findOneBy("carrierstatics", "portrait", "联通户");
        System.out.println(document);

    }

    public static void saveOrUpdateMongo(String tableName,String database, Document doc){
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(tableName);
        if(!doc.containsKey("_id")){
            ObjectId objectId = new ObjectId();
            doc.put("_id",objectId);
            mongoCollection.insertOne(doc);
        }

        Document matchDocument = new Document();
        String objectId = doc.get("_id").toString();
        matchDocument.put("_id", new ObjectId(objectId));
        FindIterable<Document> findIterable = mongoCollection.find(matchDocument);

        if(findIterable.iterator().hasNext()){
            mongoCollection.updateOne(matchDocument,new Document("$set",doc));
            try {
                System.out.println("come into saveorupdatemongo ---- update---" + JSONObject.toJSONString(doc));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            mongoCollection.insertOne(doc);
            try {
                System.out.println("come into saveorupdatemongo ---- insert---" + JSONObject.toJSONString(doc));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }
}
