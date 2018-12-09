package com.luyi.util;

import com.luyi.ip.IPSeeker;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * @ClassName IpUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class IpUtil {
    private static final Logger logger = Logger.getLogger(IpUtil.class);
    static RegionInfo info = null;
    /**
     * 解析ip的方法
     * @param ip 被解析的ip
     * @return
     */

    public static RegionInfo parserIp(String ip){
        if(StringUtils.isEmpty(ip)){
            return info;
        }
        //ip是一个正常值
        String country = IPSeeker.getInstance().getCountry(ip);
        if(StringUtils.isNotEmpty(country)){
            info = new RegionInfo();
            //判断country是否为局域网
            if(country.equals("局域网")){
                info.setCountry("中国");
                info.setProvince("北京市");
                info.setCity("昌平区");
            } else if (country != null && !country.trim().equals("")){
                int index1 = country.indexOf("省");
                if (index1 >0) {
                    info.setCountry("中国");
                    info.setProvince(country.substring(0, index1 + 1));

                    int index2 = country.indexOf("市", index1 + 1);
                    if (index2 > 0) {
                        info.setCity(country.substring(index1+1, index2 + 1));
                    }
                }
            }else{
                String flag = country.substring(0,2);
                switch (flag){
                    case "内蒙":{
                        info.setCountry("中国");
                        info.setProvince(flag + "古");
                        country = country.substring(3);
                        int index = country.indexOf("市");
                        if (index > 0){
                            info.setCity(country.substring(0,index+1));
                        }
                        break;
                    }

                    case "广西":
                    case "宁夏":
                    case "西藏":
                    case "新疆":{
                        info.setCountry("中国");
                        info.setProvince(flag);
                        country = country.substring(2);
                        int index = country.indexOf("市");
                        if (index > 0){
                            info.setCity(country.substring(0,index+1));
                        }
                        break;
                    }

                    case "北京":
                    case "天津":
                    case "重庆":
                    case "上海":{
                        info.setCountry("中国");
                        info.setProvince(flag + "市");
                        country.substring(2);
                        int index = country.indexOf("区");
                        char c = country.charAt(index -1);
                        if (c != '小' && c != '校' || c != '军'){
                            info.setCity(country.substring(0,index+1));
                        }
                        index = country.indexOf("县");
                        if (index > 0){
                            info.setCity(country.substring(0,index+1));
                        }
                        break;
                    }
                    case "香港":
                    case "澳门":{
                        info.setCity("中国");
                        info.setProvince(flag + "特别行政区");
                        break;
                    }
                    case "台湾":{
                        info.setCountry("中国");
                        info.setProvince(flag + "省");
                        break;
                    }

                    default:{
                        info.setCountry(country);
                        break;
                    }
                }
            }

            //返回字段中是否包括省这个字，否则判断是前两个字符是否为北京、天津、上海、重庆和广西、内蒙、西藏、新疆、宁夏和香港、澳门、台湾

            //返回的字段中是否包括市
        }
        return info;
    }


    /**
     * 封装ip解析出来的信息
     */
    public static class RegionInfo{
        private String DEFAULT_VALUE = "unknown";
        private String country = DEFAULT_VALUE;
        private String province = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    country + '\t' + province + '\t' + city + '\t' + '}';
        }
    }
}