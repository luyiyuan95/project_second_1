package com.luyi.test;

import com.luyi.etl.util.LogUtil;

import java.util.Iterator;
import java.util.Map;

public class LogTest {
    public static void main(String[] args) {
        String data = "192.168.91.1^A" +
                "1543846054.252^A192.168.91.123^A" +
                "/1603.JPG?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2Fdemo.jsp&" +
                "p_ref=http%3A%2F%2Flocalhost%3A8080%2Fdemo4.jsp&" +
                "tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A21&ver=1&pl=website" +
                "&sdk=js&u_ud=FF204389-7217-4719-B301-4A208333A612&u_mid=liyadong&u_sd=37EE9FC3-D4CA-44B5-AACB-577EEC9A7360" +
                "&c_time=1543846054256&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%" +
                "20(KHTML%2C%20like%20Gecko)%20Chrome%2F68.0.3440.106%20Safari%2F537.36&b_rst=1366*768";
        Map<String,String> map = LogUtil.parserLog(data);
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
}
