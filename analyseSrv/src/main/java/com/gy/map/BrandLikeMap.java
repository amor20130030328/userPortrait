package com.gy.map;

import com.alibaba.fastjson.JSONObject;
import com.gy.entity.BrandLike;
import com.gy.kafka.KafkaEvent;
import com.gy.util.HBaseUtils;
import com.youfan.log.ScanProductLog;
import com.youfan.utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class BrandLikeMap implements FlatMapFunction<KafkaEvent,BrandLike> {
    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<BrandLike> out) throws Exception {
        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data, ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        String brand = scanProductLog.getBrand();
        String tablename = "userflaginfo";
        String rowkey = userid + "";
        String familyname = "userbehavior";
        String column = "brandlist" ;  //运营
        String mapdata = HBaseUtils.getDate(tablename,rowkey,familyname,column);
        Map<String, Long> map = new HashMap<>();
        if(StringUtils.isNotBlank(mapdata)){
            map = JSONObject.parseObject(mapdata,Map.class);
        }

        //获取之前的品牌偏好
        String maxprebrand = MapUtils.getmaxbyMap(map);
        Long prebrand = map.get(brand) == null ? 0l : map.get(brand);
        map.put(brand,prebrand +1 );

        String finalString = JSONObject.toJSONString(map);
        HBaseUtils.putData(tablename,rowkey,familyname,column,finalString);

        String maxbrand = MapUtils.getmaxbyMap(map);

        if(StringUtils.isNotBlank(maxbrand) && !maxprebrand.equals(maxbrand)){
            BrandLike brandLike = new BrandLike();
            brandLike.setBrand(maxprebrand);
            brandLike.setCount(-1L);
            brandLike.setGroupbyfield("==brandlike==" + maxprebrand);
            out.collect(brandLike);
        }

        BrandLike brandLike = new BrandLike();
        brandLike.setBrand(maxbrand);
        brandLike.setCount(1L);
        out.collect(brandLike);
        brandLike.setGroupbyfield("==brandlike==" + maxbrand);
        column = "brandlike";
        HBaseUtils.putData(tablename,rowkey,familyname,column,maxbrand);
    }
}
