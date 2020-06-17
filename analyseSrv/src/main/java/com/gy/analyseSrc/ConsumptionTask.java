package com.gy.analyseSrc;

import com.gy.entity.ConsumptionLevel;
import com.gy.map.ConsumptionLevelMap;
import com.gy.reduce.ConsumptionLevelFinalReduce;
import com.gy.reduce.ConsumptionReduce;
import com.gy.util.MongoUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

public class ConsumptionTask {

    public static void main(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setGlobalJobParameters(parameterTool);
        DataSet<String> text = environment.readTextFile(parameterTool.get("input"));

        DataSet<ConsumptionLevel> mapResult = text.map(new ConsumptionLevelMap());
        DataSet<ConsumptionLevel> reduceResult = mapResult.groupBy("groupfield").reduceGroup(new ConsumptionReduce());
        DataSet<ConsumptionLevel> reduceResultFinal = reduceResult.groupBy("groupfield").reduce(new ConsumptionLevelFinalReduce());

        try{
            List<ConsumptionLevel> resultList = reduceResultFinal.collect();
            for (ConsumptionLevel consumptionLevel: resultList) {
                String consumptiontype = consumptionLevel.getConsumptiontype();
                Long count = consumptionLevel.getCount();
                Document document = MongoUtil.findOneBy("consumptionlevelstatics", "portrait", consumptiontype);
                if(document == null){
                    document = new Document();
                    document.put("info",consumptiontype);
                    document.put("count",count);
                }else{
                    Long countpre = document.getLong("count");
                    Long total = countpre + count;
                    document.put("count",total);
                }
                MongoUtil.saveOrUpdateMongo("consumptionlevelstatics", "portrait", document);
            }

        }catch(Exception e){
            e.printStackTrace();
        }




    }
}

