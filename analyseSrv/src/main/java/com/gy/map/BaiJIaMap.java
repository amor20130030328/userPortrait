package com.gy.map;

import com.gy.entity.BaijiaInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

public class BaiJIaMap implements MapFunction<String,BaijiaInfo> {


    @Override
    public BaijiaInfo map(String s) throws Exception {

        if(StringUtils.isBlank(s)){
            return null;
        }

        String [] orderinfos = s.split(",");
        String id = orderinfos[0];
        String productid = orderinfos[1];
        String producttypeid = orderinfos[2];
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

        BaijiaInfo baijiaInfo = new BaijiaInfo();
        baijiaInfo.setUserid(userid);
        baijiaInfo.setCreatetime(createtime);
        baijiaInfo.setAmount(amount);
        baijiaInfo.setPaytype(paytype);
        baijiaInfo.setPaystatus(paystatus);
        baijiaInfo.setCouponamount(couponamount);
        baijiaInfo.setTotalamount(totalamount);
        baijiaInfo.setRefundamount(refundamount);
        String groupfield = "baijia==" + userid;
        baijiaInfo.setGroupfield(groupfield);

        ArrayList<BaijiaInfo> list = new ArrayList<>();
        list.add(baijiaInfo);
        baijiaInfo.setList(list);
        return baijiaInfo;
    }
}
