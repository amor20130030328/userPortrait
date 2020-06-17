package com.gy.reduce;

import com.gy.entity.ConsumptionLevel;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ConsumptionLevelFinalReduce implements ReduceFunction<ConsumptionLevel> {
    @Override
    public ConsumptionLevel reduce(ConsumptionLevel value1, ConsumptionLevel value2) throws Exception {

        String consumptiontype = value1.getConsumptiontype();
        Long count1 = value1.getCount();
        Long count2 = value2.getCount();

        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setConsumptiontype(consumptiontype);
        consumptionLevel.setCount(count1 + count2);
        return consumptionLevel;
    }
}
