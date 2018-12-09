package com.luyi.etl.util;

import com.luyi.common.DateTypeEnum;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {
    public static String getYesterDate(){
        Date date = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Long yestime = date.getTime() - 24*60*60*1000;
        Date ldate = new Date(yestime);
        String t = df.format(ldate);
        return t;
    }

    public static int getDataInfo(long time, DateTypeEnum type){
        Date date = new Date(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if (type.type.equals("year")){
            return calendar.get(Calendar.YEAR);
        }else if (type.type.equals("season")){
            int month = calendar.get(Calendar.MONTH)+1;
            if (month == 12){
                return 4;
            }else {
                return (int)month/3+1;
            }
        }else if (type.type.equals("month")){
            return calendar.get(Calendar.MONTH)+1;
        }else if (type.type.equals("week")){
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }else if (type.type.equals("day")){
            return calendar.get(Calendar.DAY_OF_YEAR);
        }else {
            return 0;
        }
    }
    public static long getFirstOfWeek(long time){
        Date date = new Date(time);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int d = 0;
        if (calendar.get(Calendar.DAY_OF_WEEK) == 1) {
            d = -6;
        } else {
            d = 2 - calendar.get(Calendar.DAY_OF_WEEK);
        }
        calendar.add(Calendar.DAY_OF_WEEK, d);
        System.out.println(calendar.get(Calendar.DAY_OF_MONTH));

        return calendar.getTime().getTime();
    }
    public static long getYesterDate(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, -1);
        return calendar.getTime().getTime();
    }
    public static String long2String(long time,String pattern){
        return null;
    }
    public static long string2Long(String time) throws Exception{
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date d = df.parse(time);
        return d.getTime();
    }
    public static long getNewTime(){
        return new Date().getTime();
    }

}
