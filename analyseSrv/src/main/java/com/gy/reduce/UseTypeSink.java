package com.gy.reduce;

import com.gy.entity.UseTypeInfo;
import com.gy.util.MongoUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class UseTypeSink implements SinkFunction<UseTypeInfo> {

    @Override
    public void invoke(UseTypeInfo value, Context context) {
        String usetype = value.getUsetype();
        long count = value.getCount();
        Document doc = MongoUtil.findOneBy("usetypestatics", "portrait", usetype);
        if(doc == null){
            doc = new Document();
            doc.put("info",usetype);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = count + countpre;
            doc.put("count",total);
        }
        MongoUtil.saveOrUpdateMongo("usetypestatics","portrait",doc);
    }
}
