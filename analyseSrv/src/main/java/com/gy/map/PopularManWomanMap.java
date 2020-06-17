package com.gy.map;

import com.alibaba.fastjson.JSONObject;
import com.gy.entity.PopularManAndWoman;
import com.gy.kafka.KafkaEvent;
import com.youfan.log.ScanProductLog;
import com.youfan.utils.ReadProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class PopularManWomanMap implements FlatMapFunction<KafkaEvent,PopularManAndWoman> {


    @Override
    public void flatMap(KafkaEvent value, Collector<PopularManAndWoman> out) throws Exception {



        String word = value.getWord();
        System.out.println("word"+word);
        ScanProductLog scanProductLog = JSONObject.parseObject(word, ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        int productid = scanProductLog.getProductid();
        PopularManAndWoman popularManAndWoman = new PopularManAndWoman();
        popularManAndWoman.setUserId(userid+"");
        String popularType = ReadProperties.getKey(productid + "", "productChaoLiudic.properties");
        if(StringUtils.isNotBlank(popularType)){
            popularManAndWoman.setPopularType(popularType);
            popularManAndWoman.setCount(1L);
            popularManAndWoman.setGroupbyfiled("popularManWoman==" + userid);
            List<PopularManAndWoman> list = new ArrayList<>();
            list.add(popularManAndWoman);
            System.out.println("popularManAndWoman"+popularManAndWoman);
            out.collect(popularManAndWoman);

        }
    }
}
