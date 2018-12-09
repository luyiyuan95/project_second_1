package com.luyi.common;

public enum  KpiTypeEnum {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    ;
    public String kpiName;

    KpiTypeEnum(String kpiName) {
        this.kpiName = kpiName;
    }

    public static KpiTypeEnum valueOfKpiName(String kpi){
        for (KpiTypeEnum k:values()){
            if (k.kpiName.equals(kpi)){
                return k;
            }
        }
        return null;
    }
}
