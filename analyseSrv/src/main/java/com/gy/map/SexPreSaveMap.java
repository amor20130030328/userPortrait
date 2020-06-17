package com.gy.map;

import com.gy.entity.SexPreInfo;
import com.gy.logic.Logistic;
import com.gy.util.HBaseUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.Random;

public class SexPreSaveMap implements MapFunction<String,SexPreInfo> {

    private ArrayList<Double> weights = null;

    public SexPreSaveMap(ArrayList<Double> finalweight) {
        this.weights = finalweight;
    }

    @Override
    public SexPreInfo map(String value) throws Exception {

        String[] temps = value.split("\t");
        Random random = new Random();
        //清洗以及归一化
        int userid = Integer.valueOf(temps[0]);
        long ordernum = Long.valueOf(temps[1]);//订单的总数
        long orderfre = Long.valueOf(temps[4]);//隔多少天下单
        int manclothes =Integer.valueOf(temps[5]);//浏览男装次数
        int womenclothes = Integer.valueOf(temps[6]);//浏览女装的次数
        int childclothes = Integer.valueOf(temps[7]);//浏览小孩衣服的次数
        int oldmanclothes = Integer.valueOf(temps[8]);//浏览老人的衣服的次数
        double avramount = Double.valueOf(temps[9]);//订单平均金额
        int producttimes = Integer.valueOf(temps[10]);//每天浏览商品数

        ArrayList<String> as = new ArrayList<String>();
        as.add(ordernum+"");
        as.add(orderfre+"");
        as.add(manclothes+"");

        as.add(womenclothes+"");
        as.add(childclothes+"");
        as.add(oldmanclothes+"");

        as.add(avramount+"");
        as.add(producttimes+"");

        String sexFlag = Logistic.classifyVector(as, weights);
        String sexString = sexFlag == "0" ? "女" : "男";
        String tableName = "userflaginfo";
        String rowkey = userid +"";
        String familyname = "baseinfo";
        String column = "sex";   //运营商
        HBaseUtils.putData(tableName,rowkey,familyname,column,sexString);
        return null;
    }
}
