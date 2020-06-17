package com.gy.reduce;

import com.gy.entity.YearBase;
import org.apache.flink.api.common.functions.ReduceFunction;

public class YearBaseReduce implements ReduceFunction<YearBase> {


    @Override
    public YearBase reduce(YearBase yearBase, YearBase t1) throws Exception {
        String yearType = yearBase.getYearType();
        Long count1 = yearBase.getCount();
        Long count2 = t1.getCount();

        YearBase finalyearBase = new YearBase();
        finalyearBase.setYearType(yearType);
        finalyearBase.setCount(count1 + count2);
        finalyearBase.setGroupfield(yearBase.getGroupfield());
        return finalyearBase;
    }
}
