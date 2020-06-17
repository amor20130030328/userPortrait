package com.gy.logic;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

public class LogicMap implements MapFunction<String,LogicInfo> {
    @Override
    public LogicInfo map(String value) throws Exception {

        if(StringUtils.isBlank(value)){
            return null;
        }

        Random random = new Random();

        String[] lines = value.split(",");
        String variable1 = lines[0];
        String variable2 = lines[1];
        String variable3 = lines[2];
        String labase = lines[3];

        LogicInfo logicInfo = new LogicInfo();
        logicInfo.setLabase(labase);
        logicInfo.setVariable1(variable1);
        logicInfo.setVariable2(variable2);
        logicInfo.setVariable3(variable3);
        logicInfo.setGroupbyfield("logic==" + random.nextInt(10));

       return logicInfo;

    }
}
