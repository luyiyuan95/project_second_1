package com.luyi.etl.util;

import com.luyi.common.LogParameter;
import com.luyi.util.IpUtil;
import com.luyi.util.UserAgentUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName LogUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 将hdfs中的数据的每一行都进行ip、useragent、时间戳等的解析
 **/
public class LogUtil {
    private static final Logger logger  = Logger.getLogger(LogUtil.class);
    /**
     *
     * @param log  一行日志
     * @return 将解析后的k-v存储到map中，以便etl的mapper进行使用
     */
    public static Map parserLog(String log){

        if (StringUtils.isEmpty(log)){
            return null;
        }

        Map<String,String> map = new HashMap<>();
        String[] list = log.split(LogParameter.LOG_Spator);
        if (list.length == 4){
            map.put(LogParameter.LOG_IP,list[0]);
            map.put(LogParameter.LOG_Server_Time,list[1].replaceAll("\\.",""));
            handleRequestBody(list[3],map);
            handleIp(map);
            handleUserAgent(map);
        }

        return map;
    }
    //解析url请求事件
    public static Map<String,String> handleRequestBody(String body,Map<String ,String> map) {
        try {
            body = body.split("[?]")[1];
            String[] url = body.split("&");
            for (String s : url) {
                String[] temp = s.split("=");
                String k = temp[0];
                String v = URLDecoder.decode(temp[1], "utf-8");
                if (k != null && !k.trim().equals(""))
                    map.put(k, v);
            }
        } catch (UnsupportedEncodingException e) {
            logger.warn("value解码异常", e);
        }
        return map;
    }
    //ip解析的字段存储到map
    public static Map<String,String>  handleIp(Map<String,String> map){
        if (!map.isEmpty()){
            String ip = map.get(LogParameter.LOG_IP);
            IpUtil.RegionInfo rinfo = IpUtil.parserIp(ip);
            map.put(LogParameter.LOG_Country,rinfo.getCountry());
            map.put(LogParameter.LOG_Province,rinfo.getProvince());
            map.put(LogParameter.LOG_City,rinfo.getCity());
        }
        return map;
    }
    //userAgent解析存储到map
    public static Map<String,String> handleUserAgent(Map<String,String> map){
        if (!map.isEmpty()){
            String userAgent = map.get(LogParameter.LOG_BrowserInformation);
            UserAgentUtil.UserAgentInfo uinof = UserAgentUtil.parserUserAgent(userAgent);
            map.put(LogParameter.LOG_BrowserName,uinof.getBrowserName());
            map.put(LogParameter.LOG_BrowserVersion,uinof.getBrowserVersion());
            map.put(LogParameter.LOG_OsName,uinof.getOsName());
            map.put(LogParameter.LOG_OsVersion,uinof.getOsVersion());
        }
        return map;
    }
}