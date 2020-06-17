package com.gy.analyseSrc;

import com.gy.entity.YearBase;
import com.gy.map.YearBaseMap;
import com.gy.reduce.YearBaseReduce;
import com.gy.util.MongoUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

public class YearBaseTask {

    public static void main(String[] args)  {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = env.readTextFile(params.get("input"));
        DataSet<YearBase> mapresult = text.map(new YearBaseMap());
        DataSet<YearBase> reduceresult = mapresult.groupBy("groupfield").reduce(new YearBaseReduce());

        try {
            List<YearBase> resultList = reduceresult.collect();
            for(YearBase yearBase : resultList){
                String yearType = yearBase.getYearType();
                Long count = yearBase.getCount();
                Document doc = MongoUtil.findOneBy("yearbasestatics", "youfanPortrait", yearType);
                if(doc == null){
                    doc = new Document();
                    doc.put("count",count);
                    doc.put("yearbasetype",yearType);
                }else{
                    Long countpre = doc.getLong("count");
                    Long total = countpre + count;
                    doc.put("count",total);
                }

                MongoUtil.saveOrUpdateMongo("yearbasestatics","portrait",doc);
            }
           // env.execute("year base task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
