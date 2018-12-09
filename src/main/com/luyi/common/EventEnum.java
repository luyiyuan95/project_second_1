package com.luyi.common;

public enum EventEnum {
    LANUCH(1,"lanuch event","e_l"),
    PAGEVIEW(2,"page view envent","e_pv"),
    CHARGE_REQUEST(3,"charge request envent","e_crt"),
    CHARGE_SUCCESS(4,"charge success envent","e_cs"),
    CHARGE_REFUND(5,"charge refund event","e_cr"),
    EVENT(6,"event","e_e");

    public int id;
    public String enventName;
    public String alias;

    EventEnum(int id, String enventName, String alias) {
        this.id = id;
        this.enventName = enventName;
        this.alias = alias;
    }
    //根据枚举别名获取
    public static EventEnum valueOfAlias(String aliasName){
        for (EventEnum event:values()){
            if (event.alias.equals(aliasName)){
                return event;
            }
        }
        //return null;
        throw new RuntimeException("aliasName暂时没有对应的Event事件");
    }
}
