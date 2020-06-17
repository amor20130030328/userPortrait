package com.gy.reduce;

import com.gy.entity.PopularManAndWoman;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.List;

public class PopularManWomanReduce implements ReduceFunction<PopularManAndWoman> {
    @Override
    public PopularManAndWoman reduce(PopularManAndWoman value1, PopularManAndWoman value2) throws Exception {

        String userId = value1.getUserId();
        List<PopularManAndWoman> list1 = value1.getList();
        List<PopularManAndWoman> list2 = value2.getList();
        list1.addAll(list2);

        PopularManAndWoman popularManAndWoman = new PopularManAndWoman();
        popularManAndWoman.setUserId(userId);
        popularManAndWoman.setList(list1);

        System.out.println("Reduce ====> popularManAndWoman： " + popularManAndWoman);
        return popularManAndWoman;
    }
}
