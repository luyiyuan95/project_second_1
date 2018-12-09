package com.luyi.test;

import com.luyi.ip.IPSeeker;
import com.luyi.util.IpUtil;

import java.util.List;

/**
 * @ClassName IpTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 解析ip的测试类
 **/
public class IpTest {
    public static void main(String[] args) {

       /* System.out.println(IPSeeker.getInstance().getCountry("59.48.116.18"));
        System.out.println(IpUtil.parserIp("59.48.116.18"));*/
       List<String> list = IPSeeker.getInstance().getAllIp();
       for (String ip : list){
           System.out.println(ip + ":"+IpUtil.parserIp(ip));
       }
    }
}