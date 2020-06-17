package com.gy.reduce;

import com.gy.entity.ConsumptionLevel;
import com.gy.util.HBaseUtils;
import com.sun.tools.corba.se.idl.StringGen;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ConsumptionReduce implements GroupReduceFunction<ConsumptionLevel,ConsumptionLevel> {
    @Override
    public void reduce(Iterable<ConsumptionLevel> values, Collector<ConsumptionLevel> out) throws Exception {

        Iterator<ConsumptionLevel> iterator = values.iterator();
        int sum = 0;
        double totalamount = 0d;
        String userId = "-1";
        while(iterator.hasNext()){
            ConsumptionLevel consumptionLevel = iterator.next();
            String amounttotal = consumptionLevel.getAmounttotal();
            userId = consumptionLevel.getUserId();
            double amountDouble = Double.valueOf(amounttotal);
            totalamount += amountDouble;
            sum++;
        }

        double avramount = totalamount / sum ;    // 高消费5000 ，中等消费 1000 ，1000 低消费   小于 1000
        String flag = "low";
        if(avramount >= 1000 && avramount < 5000){
            flag = "middle";
        }else if(avramount >= 5000){
            flag = "high";
        }

        String tableName = "userflaginfo";
        String rowkey = userId +"";
        String familyname = "consumerinfo";
        String column = "consumptionlevel";
        String data = HBaseUtils.getDate(tableName,rowkey,familyname,column);

        if(StringUtils.isBlank(data)){
            ConsumptionLevel consumptionLevel = new ConsumptionLevel();
            consumptionLevel.setConsumptiontype(flag);
            consumptionLevel.setCount(1L);
            consumptionLevel.setGroupbyfield("== consumptionLevelFinal ==" + flag );
            out.collect(consumptionLevel);
        }else if(!data.equals(flag)){
            ConsumptionLevel consumptionLevel = new ConsumptionLevel();
            consumptionLevel.setConsumptiontype(data);
            consumptionLevel.setCount(-1L);
            consumptionLevel.setGroupbyfield("== consumptionLevelFinal==" + data);

            ConsumptionLevel consumptionLevel2 = new ConsumptionLevel();
            consumptionLevel2.setConsumptiontype(flag);
            consumptionLevel2.setCount(1L);
            consumptionLevel2.setGroupbyfield("== consumptionLevelFinal==" + flag);

            out.collect(consumptionLevel);
            out.collect(consumptionLevel2);
        }

        HBaseUtils.putData(tableName,rowkey,familyname,column,flag);
    }
}
