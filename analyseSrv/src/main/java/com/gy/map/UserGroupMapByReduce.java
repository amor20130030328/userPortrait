package com.gy.map;

import com.gy.entity.UserGroupInfo;
import com.gy.util.DateUtils;
import com.youfan.utils.ReadProperties;
import org.apache.flink.api.common.functions.MapFunction;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UserGroupMapByReduce implements MapFunction<UserGroupInfo,UserGroupInfo> {

    @Override
    public UserGroupInfo map(UserGroupInfo value) throws Exception {

        //消费类目，电子（电脑，收集，电视）生活家居（衣服，生活用户，床上用品）生鲜（油，米等）
        List<UserGroupInfo> list = value.getList();

        //排序
        Collections.sort(list, new Comparator<UserGroupInfo>() {
            @Override
            public int compare(UserGroupInfo o1, UserGroupInfo o2) {

                String timeo1 = o1.getCreatetime();
                String timeo2 = o1.getCreatetime();
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

        //排序   --- end
        double totalamount = 0l;    //总金额
        double maxamount = Double.MIN_VALUE;   //最大金额
        Map<Integer, Integer> frequencyMap = new HashMap<>();  //消费频次
        UserGroupInfo userGroupInfobefore = null;

        Map<String, Long>  producttypemap = new HashMap<String,Long>(); //商品类别map
        producttypemap.put("1",0l);
        producttypemap.put("2",0l);
        producttypemap.put("3",0l);

        Map<Integer,Long> timeMap = new HashMap<Integer, Long>();  //时间的map
        timeMap.put(1,0l);
        timeMap.put(2,0l);
        timeMap.put(3,0l);
        timeMap.put(4,0l);


        for (UserGroupInfo userGroupInfo:list){
            Double totalamountdouble = Double.valueOf(userGroupInfo.getTotalamount());
            totalamount += totalamountdouble;
            if(totalamountdouble > maxamount){
                maxamount = totalamountdouble;
            }

            if(userGroupInfobefore == null){
                userGroupInfobefore = userGroupInfo;
                continue;
            }

            //计算购买的频率
            String beforetime = userGroupInfobefore.getCreatetime();
            String endstime = userGroupInfo.getCreatetime();
            int days = DateUtils.getDaysBetweenByStartAndEnd(beforetime,endstime,"yyyyMMdd hhmmss");
            int before = frequencyMap.get(days) == null ?0: frequencyMap.get(days);
            frequencyMap.put(days,before + 1);

            //计算消费类目
            String producttypeid = userGroupInfo.getProducttypeid();
            String bitproducttype = ReadProperties.getKey(producttypeid,"productypedic.properties");
            Long pre = producttypemap.get(producttypeid) == null ? 0l : producttypemap.get(producttypeid);
            producttypemap.put(producttypeid,pre + 1);

            //时间点，上午（7-12）1，下午（12-7）2，晚上（7-12）3，凌晨（0-7）4
            String time = userGroupInfo.getCreatetime();
            String hours = DateUtils.getHoursByDate(time);
            Integer hoursInt = Integer.valueOf(hours);
            int timetype = -1;
            if(hoursInt >= 7 && hoursInt < 12){
                timetype = 1;
            }else if(hoursInt >= 12 && hoursInt <19){
                timetype = 2;
            }else if(hoursInt >= 19 && hoursInt <24){
                timetype = 3;
            }else if(hoursInt >= 0 && hoursInt <7){
                timetype = 4;
            }
            Long timespre = timeMap.get(timetype) == null ? 0l : timeMap.get(timetype);
            timeMap.put(timetype,timespre);

        }

        int ordernums = list.size();
        double avramount = totalamount/ordernums;   //平均消费金额
        //消费最大金额
        Set<Map.Entry<Integer, Integer>> set = frequencyMap.entrySet();
        Integer totaldays = 0;

        for(Map.Entry<Integer,Integer> map : set){
            Integer days = map.getKey();
            Integer cou = map.getValue();
            totaldays += days * cou;
        }
        int days = totaldays / ordernums;  //消费频次
        Random random = new Random();

        UserGroupInfo userGroupInfoFinal = new UserGroupInfo();
        userGroupInfoFinal.setUserid(value.getUserid());
        userGroupInfoFinal.setAvramount(avramount);
        userGroupInfoFinal.setMaxamount(maxamount);
        userGroupInfoFinal.setDays(days);
        userGroupInfoFinal.setBuytime1(timeMap.get(1));
        userGroupInfoFinal.setBuytime2(timeMap.get(2));
        userGroupInfoFinal.setBuytime3(timeMap.get(3));
        userGroupInfoFinal.setBuytime4(timeMap.get(4));

        userGroupInfoFinal.setBuytype1(producttypemap.get("1"));
        userGroupInfoFinal.setBuytype2(producttypemap.get("2"));
        userGroupInfoFinal.setBuytype3(producttypemap.get("3"));
        userGroupInfoFinal.setGroupfield("usergroupkmean" + random.nextInt());
        return userGroupInfoFinal;
    }
}
