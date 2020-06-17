package com.gy.analyseSrc;

import com.gy.entity.CarrierInfo;
import com.gy.map.CarrierMap;
import com.gy.reduce.CarrierReduce;
import com.gy.util.MongoUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

public class CarrierTask {

    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);
        
        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<CarrierInfo> mapresult = text.map(new CarrierMap());
        DataSet<CarrierInfo> reduceresult = mapresult.groupBy("groupField").reduce(new CarrierReduce());


        try {
            List<CarrierInfo> resultList = reduceresult.collect();
            for (CarrierInfo carrierInfo: resultList) {
                String carrier = carrierInfo.getCarrier();
                Long count = carrierInfo.getCount();

                Document doc = MongoUtil.findOneBy("carrierstatics", "portrait", carrier);
                System.out.println(count +"" +carrier + doc);

                if(doc == null){
                    doc = new Document();
                    doc.put("info",carrier);
                    doc.put("count",count);
                }else{

                    Long countPre = doc.getLong("count");
                    Long total = countPre + count;
                    doc.put("count",total);

                }

                MongoUtil.saveOrUpdateMongo("carrierstatics","portrait",doc);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
