package com.luyi.common;

public enum  DateTypeEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour")
    ;

    public String type;

    DateTypeEnum(String type) {
        this.type = type;
    }

    public static DateTypeEnum valueOfType(String type){
        for (DateTypeEnum data:values()){
            if (data.equals(type)){
                return data;
            }
        }
        return null;
    }

}
