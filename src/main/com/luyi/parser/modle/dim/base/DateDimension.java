package com.luyi.parser.modle.dim.base;

import com.luyi.common.DateTypeEnum;
import com.luyi.etl.util.TimeUtil;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date calendar = new Date();
    private String type;

    public DateDimension() { }

    public DateDimension(int year, int season, int month, int week, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar) {
        this(year,season,month,week,day);
        this.calendar = calendar;
    }

    public DateDimension(int year, int season, int month, int week, int day,Date calendar, String type) {
        this(year,season,month,week,day,calendar);
        this.type = type;
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, Date calendar, String type) {
        this(year,season,month,week,day,calendar,type);
        this.id = id;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.season);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.week);
        dataOutput.writeInt(this.day);
        dataOutput.writeLong(this.calendar.getTime());
        dataOutput.writeUTF(this.type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.year = dataInput.readInt();
        this.season = dataInput.readInt();
        this.month = dataInput.readInt();
        this.week= dataInput.readInt();
        this.day= dataInput.readInt();
        this.calendar.setTime(dataInput.readLong());
        this.type = dataInput.readUTF();
    }
    public static DateDimension bulitDateDimension(long time, DateTypeEnum type){
        Calendar cal = Calendar.getInstance();
        cal.clear();
        int year =TimeUtil.getDataInfo(time,DateTypeEnum.YEAR);
        if (type.equals(DateTypeEnum.YEAR)){
            cal.set(year,0,1);
            return new DateDimension(year,1,1,1,1,cal.getTime(),type.type);
        }
        int season =TimeUtil.getDataInfo(time,DateTypeEnum.SEASON);
        if (type.equals(DateTypeEnum.SEASON)){
            int month = season * 3 - 2;
            cal.set(year,month-1,1);
            return new DateDimension(year,season,month,1,1,cal.getTime(),type.type);
        }
        int month =TimeUtil.getDataInfo(time,DateTypeEnum.MONTH);
        if (type.equals(DateTypeEnum.MONTH)){
            cal.set(year,month-1,1);
            return new DateDimension(year,season,month,1,1,cal.getTime(),type.type);
        }
        int week =TimeUtil.getDataInfo(time,DateTypeEnum.WEEK);
        if (type.equals(DateTypeEnum.WEEK)){
            long firstDayOfWeek = TimeUtil.getFirstOfWeek(time);
            year = TimeUtil.getDataInfo(firstDayOfWeek,DateTypeEnum.YEAR);
            season = TimeUtil.getDataInfo(firstDayOfWeek,DateTypeEnum.SEASON);
            month = TimeUtil.getDataInfo(firstDayOfWeek,DateTypeEnum.MONTH);
            cal.set(year,month-1,1);
            return new DateDimension(year,season,month,week,1,cal.getTime(),type.type);
        }
        int day =TimeUtil.getDataInfo(time,DateTypeEnum.DAY);
        if (type.equals(DateTypeEnum.DAY)){
            cal.set(year,month-1,1);
            return new DateDimension(year,season,month,week,day,cal.getTime(),type.type);
        }
            throw new RuntimeException("该type暂时不支持获取对应的Datedimension  ,type:" + type.getClass());
    }
    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }
        DateDimension other = (DateDimension)o;
        int tmp = this.id - other.id;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.year - other.year;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.season - other.season;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.week - other.week;
        if (tmp != 0){
            return tmp;
        }
        tmp = this.day - other.day;
        if (tmp != 0){
            return tmp;
        }
        return this.type.compareTo(other.type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return id == that.id &&
                year == that.year &&
                season == that.season &&
                month == that.month &&
                week == that.week &&
                day == that.day &&
                Objects.equals(calendar, that.calendar) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, year, season, month, week, day, calendar, type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "DateDimension{" +
                "id=" + id +
                ", year=" + year +
                ", season=" + season +
                ", month=" + month +
                ", week=" + week +
                ", day=" + day +
                ", calendar=" + calendar +
                ", type='" + type + '\'' +
                '}';
    }
}
