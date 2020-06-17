package com.gy.map;

import com.gy.entity.ConsumptionLevel;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class ConsumptionLevelMap implements MapFunction<String , ConsumptionLevel> {


    @Override
    public ConsumptionLevel map(String value) throws Exception {
        if(StringUtils.isBlank(value)){
            return null;
        }

        String[] orderinfos = value.split(",");
        String id = orderinfos[0];
        String productId = orderinfos[1];
        String producttypeId = orderinfos[2];
        String createtime = orderinfos[3];
        String amount = orderinfos[4];
        String paytype = orderinfos[5];
        String paytime = orderinfos[6];
        String paystatus = orderinfos[7];
        String couponamount = orderinfos[8];
        String totalamount = orderinfos[9];
        String refundamount = orderinfos[10];
        String num = orderinfos[11];
        String userid = orderinfos[12];

        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setUserId(userid);
        consumptionLevel.setAmounttotal(totalamount);
        consumptionLevel.setGroupbyfield("=== consumptionLevel ==" + userid);
        return consumptionLevel;
    }
}
