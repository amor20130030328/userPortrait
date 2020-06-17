package com.gy.map;

import com.alibaba.fastjson.JSONObject;
import com.gy.analyseSrc.UseTypeTask;
import com.gy.entity.UseTypeInfo;
import com.gy.kafka.KafkaEvent;
import com.gy.util.HBaseUtils;
import com.youfan.log.ScanProductLog;
import com.youfan.utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class UseTypeMap implements FlatMapFunction<KafkaEvent,UseTypeInfo> {
    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<UseTypeInfo> out) throws Exception {

        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        int usetype = scanProductLog.getUsetype();

        String usetypename = usetype == 0 ? "pc端" : usetype == 1 ? "移动端":"小程序端";
        String tablename = "userflaginfo";
        String rowkey = userid + "";
        String familyname = "userbehavior";
        String column = "usetypelist";   //运营
        String mapdata = HBaseUtils.getDate(tablename,rowkey,familyname,column);
        Map<String, Long> map = new HashMap<>();
        if(StringUtils.isNotBlank(mapdata)){
            map = JSONObject.parseObject(mapdata,Map.class);
        }

        //获取之前的终端偏好
        String maxpreusetype = MapUtils.getmaxbyMap(map);
        long preusetype = map.get(usetypename) == null ? 0l : map.get(usetypename);
        map.put(usetypename,preusetype + 1);
        String finalstring = JSONObject.toJSONString(map);
        HBaseUtils.putData(tablename,rowkey,familyname,column,finalstring);

        String maxusetype = MapUtils.getmaxbyMap(map);
        if(StringUtils.isNotBlank(maxusetype) && !maxpreusetype.equals(maxusetype)){
            UseTypeInfo useTypeInfo = new UseTypeInfo();
            useTypeInfo.setUsetype(maxpreusetype);
            useTypeInfo.setCount(-1L);
            useTypeInfo.setGroupbyfield("== usetypeinfo ==" + maxpreusetype);
            out.collect(useTypeInfo);
        }

        UseTypeInfo useTypeInfo = new UseTypeInfo();
        useTypeInfo.setUsetype(maxusetype);
        useTypeInfo.setCount(1L);
        useTypeInfo.setGroupbyfield("== usetypeinfo ==" + maxusetype);
        out.collect(useTypeInfo);
        column = "usetype";
        HBaseUtils.putData(tablename,rowkey,familyname,column,maxpreusetype);

    }
}
