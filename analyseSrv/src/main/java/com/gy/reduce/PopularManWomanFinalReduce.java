package com.gy.reduce;

import com.gy.entity.PopularManAndWoman;
import org.apache.flink.api.common.functions.ReduceFunction;

public class PopularManWomanFinalReduce  implements ReduceFunction<PopularManAndWoman> {
    @Override
    public PopularManAndWoman reduce(PopularManAndWoman value1, PopularManAndWoman value2) throws Exception {

        String popularType = value1.getPopularType();
        long count1 = value1.getCount();
        long count2 = value2.getCount();
        PopularManAndWoman popularManAndWomanFinal = new PopularManAndWoman();
        popularManAndWomanFinal.setPopularType(popularType);
        popularManAndWomanFinal.setCount(count1 + count2);
        return popularManAndWomanFinal;
    }
}
