package com.luyi.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName UserAgentUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 浏览器的代理对象解析
 **/
public class UserAgentUtil {
    private final static Logger logger = Logger.getLogger(UserAgentUtil.class);
    private static UASparser uaSparser = null;
    //初始化usaparser的对象
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.warn("初始化userAgent解析对象异常.",e);
        }
    }

    /**
     * userAgent的解析
     * @param userAgent
     * @return
     */
    public static UserAgentInfo parserUserAgent(String userAgent){
        UserAgentInfo info = null;
        try {
            //判断userAgent是否为空
            if(StringUtils.isEmpty(userAgent)){
                return info;
            }
            //userAgent肯定不为空
            cz.mallat.uasparser.UserAgentInfo uinfo = uaSparser.parse(userAgent);
            //取出uinfo中的属性值设置到info中
            if(uinfo != null){
                info = new UserAgentInfo();
                info.setBrowserName(uinfo.getUaFamily());
                info.setBrowserVersion(uinfo.getBrowserVersionInfo());
                info.setOsName(uinfo.getOsFamily());
                info.setOsVersion(uinfo.getOsName());
            }
        } catch (IOException e) {
            logger.warn("uasparser解析useragent异常.",e);
        }
        return info;
    }


    /**
     * 用于封装useragent被解析后的信息
     */
    public static class UserAgentInfo {
       private String browserName;
       private String browserVersion;
       private String osName;
       private String osVersion;

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName=" + browserName +"\t" + browserVersion + '\t' +
                    "\t" + osName + "\t" + osVersion + '}';
        }
    }
}