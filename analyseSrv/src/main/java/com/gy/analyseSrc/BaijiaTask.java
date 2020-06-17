package com.gy.analyseSrc;

import com.gy.entity.BaijiaInfo;
import com.gy.entity.EmailInfo;
import com.gy.map.BaiJIaMap;
import com.gy.map.EmailMap;
import com.gy.reduce.BaiJiaReduce;
import com.gy.reduce.EmailReduce;
import com.gy.util.DateUtils;
import com.gy.util.HBaseUtils;
import com.gy.util.MongoUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import scala.Int;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class BaijiaTask {

    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<BaijiaInfo> mapResult = text.map(new BaiJIaMap());
        DataSet<BaijiaInfo> reduceResult = mapResult.groupBy("groupfield").reduce(new BaiJiaReduce());

        try {

            List<BaijiaInfo> resultlist = reduceResult.collect();
            for(BaijiaInfo baijiaInfo : resultlist){
                String userid = baijiaInfo.getUserid();
                List<BaijiaInfo> list = baijiaInfo.getList();
                Collections.sort(list, new Comparator<BaijiaInfo>() {
                    @Override
                    public int compare(BaijiaInfo o1, BaijiaInfo o2) {

                        String timeo1 = o1.getCreatetime();
                        String timeo2 = o2.getCreatetime();
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");
                        Date dateNow = new Date();
                        Date time1 = dateNow;
                        Date time2 = dateNow;

                        try {
                            time1 = dateFormat.parse(timeo1);
                            time2 = dateFormat.parse(timeo2);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return time1.compareTo(time2);
                    }
                });


                BaijiaInfo before = null;
                Map<Integer, Integer> frequencymap = new HashMap<>();
                double maxamount = 0d;
                double sum = 0d;
                for(BaijiaInfo baijiaInfoinner : list){
                    if(before == null){
                        before = baijiaInfoinner;
                        continue;
                    }

                    //计算购买的频率
                    String beforetime = before.getCreatetime();
                    String endstime = baijiaInfoinner.getCreatetime();
                    int days = DateUtils.getDaysBetweenByStartAndEnd(beforetime, endstime, "yyyyMMdd hhmmss");
                    int brefore = frequencymap.get(days) == null ? 0 : frequencymap.get(days);
                    frequencymap.put(days,brefore +1 );

                    //计算最大金额
                    String totalamountstring = baijiaInfoinner.getTotalamount();
                    Double totalamount = Double.valueOf(totalamountstring);

                    if(totalamount > maxamount){
                        maxamount = totalamount;
                    }

                    //计算平均值
                    sum += totalamount;
                    before = baijiaInfoinner;
                }

                double avramount = list.size();
                int totaldays = 0;
                Set<Map.Entry<Integer, Integer>> set = frequencymap.entrySet();
                for(Map.Entry<Integer,Integer> entry : set){
                    Integer frequencydays = entry.getKey();
                    Integer count = entry.getValue();
                    totaldays += frequencydays * count;
                }

                int avrdays = totaldays / list.size() ; //平均天数
                //败家指数 = 支付金额平均值 * 0.3 ,最大支付金额 * 0.3 , 下单频率 * 0.4
                //支付金额平均值30分 （0-20, 5 20-60 10 ,60-100 20,100-150 30, 150-200 40 ,200-250 60 ,250-350 70 ,350-450 80, 450-600 90 ,600以上 100）
                //最大支付金额 30分 (0-5 100, 5-10 90 ,10-30 70 , 30-60 60 , 60-80 40 ,80-100 20 , 100 以上的 10 )
                //下单频率30分 (0-5 100, 5-10 90 , 10-30 70 ,30-60 60 ,60-80 40 ,80-100 20 ,100以上 10)

                int avramountsoce = 0;
                if(avramount >=0 && avramount < 20){
                    avramountsoce = 5;
                }else if(avramount >=20 && avramount < 60){
                    avramountsoce = 10;
                }else if(avramount >=60 && avramount < 100){
                    avramountsoce = 20;
                }else if(avramount >=100 && avramount < 150){
                    avramountsoce = 30;
                }else if(avramount >=150 && avramount < 200){
                    avramountsoce = 40;
                }else if(avramount >=200 && avramount < 250){
                    avramountsoce = 60;
                }else if(avramount >=250 && avramount < 350){
                    avramountsoce = 70;
                }else if(avramount >=350 && avramount < 450){
                    avramountsoce = 80;
                }else if(avramount >=450 && avramount < 600){
                    avramountsoce = 90;
                }else if(avramount >= 600){
                    avramountsoce = 100;
                }

                int maxamountscore = 0;
                if(maxamount >= 0 && maxamount < 20){
                    maxamountscore = 5;
                }else if(maxamount >=20 && maxamount <60){
                    maxamountscore = 10;
                }else if(maxamount >= 60 && maxamount < 200){
                    maxamountscore = 30;
                }else if(maxamount >= 200 && maxamount < 500){
                    maxamountscore = 60;
                }else if(maxamount >= 500 && maxamount < 700){
                    maxamountscore = 80;
                }else if(maxamount >= 700){
                    maxamountscore = 100;
                }

                //下单频率30分（0-5 100 ,5-10 90,10-30 70, 30-60 60, 60-80 40 , 80-100 20,100以上 10）
                int avrdaysscore = 0;
                if(avrdays >= 0 && avrdays < 5){
                    avrdaysscore = 100;
                }else if(avramount >= 5 && avramount < 10){
                    avrdaysscore = 90;
                }else if(avramount >= 10 && avramount < 30){
                    avrdaysscore = 70;
                }else if(avramount >= 30 && avramount < 60){
                    avrdaysscore = 60;
                }else if(avramount >= 60 && avramount < 80){
                    avrdaysscore = 40;
                }else if(avramount >= 80 && avramount < 100){
                    avrdaysscore = 20;
                }else if(avramount >= 100){
                    avrdaysscore = 10;
                }

                double totalscore = (avramountsoce / 100) * 30 + (maxamountscore / 100) * 30 + (avrdaysscore / 100 ) * 40;
                String tableName = "userflaginfo";
                String rowkey = userid;
                String familyname = "baseinfo";
                String column = "baijiascore";
                HBaseUtils.putData(tableName,rowkey,familyname,column,totalscore +"");
            }
            env.execute("baijiascore analy");

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
