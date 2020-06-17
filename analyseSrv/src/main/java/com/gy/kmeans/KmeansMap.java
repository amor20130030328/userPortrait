package com.gy.kmeans;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class KmeansMap implements MapFunction<String,Kmeans> {


    @Override
    public Kmeans map(String value) throws Exception {

        if(StringUtils.isBlank(value)){
            return null;
        }

        Random random = new Random();
        String[] temps = value.split(",");
        String variable1 = temps[0];
        String variable2 = temps[2];
        String variable3 = temps[3];

        Kmeans kmeans = new Kmeans();
        kmeans.setVariable1(variable1);
        kmeans.setVariable2(variable2);
        kmeans.setVariable3(variable3);
        kmeans.setGroupbyfield("kmeans==" + random.nextInt(10));
        return kmeans;
    }
}
