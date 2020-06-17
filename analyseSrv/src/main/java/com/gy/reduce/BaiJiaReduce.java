package com.gy.reduce;

import com.gy.entity.BaijiaInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

public class BaiJiaReduce implements ReduceFunction<BaijiaInfo> {
    @Override
    public BaijiaInfo reduce(BaijiaInfo baijiaInfo, BaijiaInfo t1) throws Exception {
        String userid = baijiaInfo.getUserid();
        List<BaijiaInfo> baijialist1 = baijiaInfo.getList();
        List<BaijiaInfo> baijialist2 = t1.getList();
        List<BaijiaInfo> finallist = new ArrayList<>();
        finallist.addAll(baijialist1);
        finallist.addAll(baijialist2);

        BaijiaInfo baijiaInfoFinal = new BaijiaInfo();
        baijiaInfoFinal.setUserid(userid);
        baijiaInfoFinal.setList(finallist);
        return  baijiaInfoFinal;
    }
}
