package com.gy.map;

import com.alibaba.fastjson.JSONObject;
import com.gy.entity.PopularManAndWoman;
import com.gy.util.HBaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PopularManWomanByReduceMap implements FlatMapFunction<PopularManAndWoman ,PopularManAndWoman> {
    @Override
    public void flatMap(PopularManAndWoman value, Collector<PopularManAndWoman> out) throws Exception {

        Map<String, Long> resultMap = new HashMap<>();
        String rowkey = "-1";
        if(rowkey.equals("-1")){
            rowkey = value.getUserId() + "";
        }

        String popularType = value.getPopularType();
        long count = value.getCount();
        long pre =  resultMap.get(popularType) == null ? 0L : resultMap.get(popularType);
        resultMap.put(popularType,pre + count);
        String tableName = "userflaginfo";
        String familyName = "userbehavior";
        String column = "chaomanandwomen";
       String data = HBaseUtils.getDate(tableName,rowkey,familyName,column);
        if(StringUtils.isNotBlank(data)){
            Map<String,Long> datamap = JSONObject.parseObject(data,Map.class);
            Set<String> keys = resultMap.keySet();
            for(String key : keys){
                Long pre1 = datamap.get(key) == null ? 0L : datamap.get(key);
                resultMap.put(key,pre1 + resultMap.get(key));
            }
        }

        if(!resultMap.isEmpty()){
            String popularManAndWoman = JSONObject.toJSONString(resultMap);
            HBaseUtils.putData(tableName,rowkey,familyName,column,popularManAndWoman);
            long popularMan = resultMap.get("1") == null ? 0L : resultMap.get("1");
            long popularWoman = resultMap.get("2") == null ? 0L : resultMap.get("2");
            String flag = "woman";
            long finalCount = popularWoman;
            if (popularMan > popularWoman){
                flag = "man";
                finalCount = popularMan;
            }

            if(finalCount > 2000){
                column = "popularType";
                PopularManAndWoman popularManAndWomanTemp = new PopularManAndWoman();
                popularManAndWomanTemp.setPopularType(flag);
                popularManAndWomanTemp.setCount(1L);
                popularManAndWomanTemp.setGroupbyfiled(flag + "==popularManAndWomanReduce");
                String type = HBaseUtils.getDate(tableName, rowkey, familyName, column);
                if(StringUtils.isNotBlank(type) && !type.equals(flag)){
                    PopularManAndWoman popularManAndWomanPre = new PopularManAndWoman();
                    popularManAndWomanPre.setPopularType(type);
                    popularManAndWomanPre.setCount(-1L);
                    popularManAndWomanPre.setGroupbyfiled(type + "==popularManAndWomanReduce");
                    out.collect(popularManAndWomanPre);
                }
                HBaseUtils.putData(tableName,rowkey,familyName,column,flag);
                out.collect(popularManAndWomanTemp);
            }
        }

    }
}
