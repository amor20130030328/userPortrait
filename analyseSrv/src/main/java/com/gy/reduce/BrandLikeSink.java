package com.gy.reduce;

import com.gy.entity.BrandLike;
import com.gy.util.MongoUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class BrandLikeSink implements SinkFunction<BrandLike> {

    @Override
    public void invoke(BrandLike value, Context context) {

        String brand = value.getBrand();
        long count = value.getCount();
        Document doc = MongoUtil.findOneBy("brandlikestatics", "portrait", brand);
        if(doc == null){
            doc = new Document();
            doc.put("info",brand);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = count + countpre;
            doc.put("count",total);
        }

        MongoUtil.saveOrUpdateMongo("brandlikestatics","portrait",doc);
    }
}
