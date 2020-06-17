package com.gy.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    public static String getYearBaseByAge(String age){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.YEAR , -Integer.valueOf(age));
        Date newdate = calendar.getTime();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
        String newdatestring = dateFormat.format(newdate);
        Integer newdateinteger = Integer.valueOf(newdatestring);
        String yearBaseType = "未知";
        if(newdateinteger >= 1940 && newdateinteger < 1950){
            yearBaseType = "40后";
        }else if(newdateinteger >= 1950 && newdateinteger < 1960){
            yearBaseType = "50后";
        } else if(newdateinteger >= 1960 && newdateinteger < 1970){
            yearBaseType = "60后";
        }else if(newdateinteger >= 1970 && newdateinteger < 1980){
            yearBaseType = "70后";
        }else if(newdateinteger >= 1980 && newdateinteger < 1990){
            yearBaseType = "80后";
        }else if(newdateinteger >= 1990 && newdateinteger < 2000){
            yearBaseType = "90后";
        }else if(newdateinteger >= 2000 && newdateinteger < 2010){
            yearBaseType = "00后";
        }else if(newdateinteger >= 2010){
            yearBaseType = "10后";
        }
        return yearBaseType;
    }

    public static int getDaysBetweenByStartAndEnd(String startTime,String endTime,String dateFormatString) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormatString);
        Date start = simpleDateFormat.parse(startTime);
        Date end = simpleDateFormat.parse(endTime);

        Calendar startCalendar = Calendar.getInstance();
        Calendar endCalendar = Calendar.getInstance();
        startCalendar.setTime(start);
        endCalendar.setTime(end);

        int days = 0;
        while(startCalendar.before(endCalendar)){
            startCalendar.add(Calendar.DAY_OF_YEAR,1);
            days += 1;
        }
        return days;
    }

    public static String getHoursByDate(String timeValue) throws Exception{
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");
        Date time = dateFormat.parse(timeValue);
        dateFormat = new SimpleDateFormat("hh");
        String resulthour = dateFormat.format(time);
        return resulthour;
    }

}
