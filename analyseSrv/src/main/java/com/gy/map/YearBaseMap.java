package com.gy.map;

import com.gy.entity.YearBase;
import com.gy.util.DateUtils;
import com.gy.util.HBaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

public class YearBaseMap implements MapFunction<String,YearBase> {


    @Override
    public YearBase map(String s) throws Exception {

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

        String yearbasetype = DateUtils.getYearBaseByAge(age);
        String tableName = "baseuserscaninfo";
        String rowKey = userid;
        String familyName = "time";
        String column = "";
        HBaseUtils.putData(tableName,rowKey,familyName,column,"");

        YearBase yearBase = new YearBase();
        String groupfield = "yearbase == " + yearbasetype;
        yearBase.setYearType(yearbasetype);
        yearBase.setCount(1L);
        yearBase.setGroupField(groupfield);
        return yearBase;
    }
}
