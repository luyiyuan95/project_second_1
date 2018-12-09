package com.luyi.test;

import com.luyi.util.UserAgentUtil;

/**
 * @ClassName UserAgentTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description userAgent的解析测试
 **/
public class UserAgentTest {
    public static void main(String[] args) {
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0"));
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36"));
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)"));
    }
}