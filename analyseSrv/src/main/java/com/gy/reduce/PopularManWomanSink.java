package com.gy.reduce;

import com.gy.entity.PopularManAndWoman;
import com.gy.util.MongoUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class PopularManWomanSink implements SinkFunction<PopularManAndWoman> {


    @Override
    public void invoke(PopularManAndWoman value, Context context) {
        String popularType = value.getPopularType();
        long count = value.getCount();
        Document doc = MongoUtil.findOneBy("chaoManAndWomenstatics", "portrait", popularType);
        if(doc == null){
            doc = new Document();
            doc.put("info",popularType);
            doc.put("count",context);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre + count;
            doc.put("count",total);
        }
        MongoUtil.saveOrUpdateMongo("chaoManAndWomenstatics", "portrait",doc);
    }
}
