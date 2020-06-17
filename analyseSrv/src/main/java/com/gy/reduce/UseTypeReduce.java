package com.gy.reduce;

import com.gy.entity.UseTypeInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

public class UseTypeReduce implements ReduceFunction<UseTypeInfo> {
    @Override
    public UseTypeInfo reduce(UseTypeInfo useTypeInfo, UseTypeInfo useTypeInfo2) throws Exception {
        String usetype = useTypeInfo.getUsetype();
        long count = useTypeInfo.getCount();
        long count2 = useTypeInfo2.getCount();
        UseTypeInfo useTypeInfofinal = new UseTypeInfo();
        useTypeInfofinal.setUsetype(usetype);
        useTypeInfofinal.setCount(count + count2);
        return useTypeInfofinal;
    }
}
