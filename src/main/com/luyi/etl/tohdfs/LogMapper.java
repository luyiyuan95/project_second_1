package com.luyi.etl.tohdfs;

import com.luyi.common.EventEnum;
import com.luyi.common.LogParameter;
import com.luyi.etl.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class LogMapper extends Mapper<Object,Text,LogWritable,NullWritable> {
    private static final Logger logger = Logger.getLogger(LogMapper.class);
    private LogWritable k = new LogWritable();
    private int inputRecords,outputRecords,filterRecords = 0;
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        this.inputRecords++;
        if (StringUtils.isEmpty(line)){
            this.filterRecords++;
            return;
        }
        Map<String,String> info = LogUtil.parserLog(line);
        String eventName = info.get(LogParameter.LOG_EventName);
        EventEnum event = EventEnum.valueOfAlias(eventName);
        switch (event){
            case LANUCH:
            case EVENT:
            case CHARGE_REFUND:
            case CHARGE_REQUEST:
            case CHARGE_SUCCESS:
            case PAGEVIEW:
                try {
                    for (Map.Entry<String,String> m:info.entrySet()){
                        this.k.setEn(info.getOrDefault(LogParameter.LOG_EventName,""));
                        this.k.setVer(info.getOrDefault(LogParameter.LOG_VersionNumber,""));
                        this.k.setPl(info.getOrDefault(LogParameter.LOG_Platform,""));
                        this.k.setSdk(info.getOrDefault(LogParameter.LOG_SDK,""));
                        this.k.setB_rst(info.getOrDefault(LogParameter.LOG_BrowserResolution,""));
                        this.k.setB_iev(info.getOrDefault(LogParameter.LOG_BrowserInformation,""));
                        this.k.setU_ud(info.getOrDefault(LogParameter.LOG_UUID,""));
                        this.k.setL(info.getOrDefault(LogParameter.LOG_ClientLanguage,""));
                        this.k.setU_mid(info.getOrDefault(LogParameter.LOG_MemberID,""));
                        this.k.setU_sd(info.getOrDefault(LogParameter.LOG_SessionID,""));
                        this.k.setC_time(info.getOrDefault(LogParameter.LOG_ClienTime,""));
                        this.k.setP_url(info.getOrDefault(LogParameter.LOG_CurrentPageURL,""));
                        this.k.setP_ref(info.getOrDefault(LogParameter.LOG_LastPageURL,""));
                        this.k.setTt(info.getOrDefault(LogParameter.LOG_Title,""));
                        this.k.setIp(info.getOrDefault(LogParameter.LOG_IP,""));
                        this.k.setS_time(info.getOrDefault(LogParameter.LOG_Server_Time,""));
                        this.k.setCa(info.getOrDefault(LogParameter.LOG_EN_Category,""));
                        this.k.setAc(info.getOrDefault(LogParameter.LOG_EN_Action,""));
                        this.k.setKv(info.getOrDefault(LogParameter.LOG_EN_Custom,""));
                        this.k.setDu(info.getOrDefault(LogParameter.LOG_EN_Duration,""));
                        this.k.setOid(info.getOrDefault(LogParameter.LOG_OrderID,""));
                        this.k.setOn(info.getOrDefault(LogParameter.LOG_OrderName,""));
                        this.k.setCua(info.getOrDefault(LogParameter.LOG_PaymentAmount,""));
                        this.k.setCut(info.getOrDefault(LogParameter.LOG_CurrencyType,""));
                        this.k.setPt(info.getOrDefault(LogParameter.LOG_PaymentMethod,""));
                        this.k.setCountry(info.getOrDefault(LogParameter.LOG_Country,""));
                        this.k.setProvince(info.getOrDefault(LogParameter.LOG_Province,""));
                        this.k.setCity(info.getOrDefault(LogParameter.LOG_City,""));
                        this.k.setBrowserName(info.getOrDefault(LogParameter.LOG_BrowserName,""));
                        this.k.setBrowserVersion(info.getOrDefault(LogParameter.LOG_BrowserVersion,""));
                        this.k.setOsName(info.getOrDefault(LogParameter.LOG_OsName,""));
                        this.k.setOsVersion(info.getOrDefault(LogParameter.LOG_OsVersion,""));
                        break;
                    }
                } catch (Exception e) {
                    this.filterRecords++;
                }
        }
        this.outputRecords++;
        context.write(this.k,NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("====================inputRecords:" + this.inputRecords +" outputRecords" + this.outputRecords + " filterRecords" + filterRecords);
    }
}
