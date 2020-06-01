package com.gy.map;

import com.gy.entity.CarrierInfo;
import com.gy.util.CarrierUtils;
import com.gy.util.HBaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class CarrierMap implements MapFunction<String,CarrierInfo> {

    @Override
    public CarrierInfo map(String s) throws Exception {

        if(StringUtils.isBlank(s)){
            return null;
        }

        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String username = userinfos[1];
        String sex = userinfos[2];
        String telphone = userinfos[3];
        String email = userinfos[4];
        String age = userinfos[5];
        String registerTime = userinfos[6];
        String usetype = userinfos[7];   //终端类型 : 0:pc端 1.移动端 2. 小程序

        int carriertype = CarrierUtils.getCarrierByTel(telphone);
        String carriertypestring = carriertype == 0 ? "未知运营商" : carriertype == 1 ? "移动用户" : carriertype == 2 ? "联通用户" : "电信用户";

        String tableName = "userflaginfo";
        String rowkey = userid;
        String familyName = "baseinfo";
        String column = "carrierinfo";  //运营商
        HBaseUtils.putData(tableName,rowkey,familyName,column,carriertypestring);
        CarrierInfo carrierInfo = new CarrierInfo();
        String groupField = "carrierInfo==" + carrierInfo;
        carrierInfo.setCount(1L);
        carrierInfo.setCarrier(carriertypestring);
        carrierInfo.setGroupField(groupField);
        return carrierInfo;
    }
}
