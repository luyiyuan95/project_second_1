package com.luyi.etl.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;

public class FSUtil {
    private static final Logger logger = Logger.getLogger(FSUtil.class);
    public static FileSystem getFS(){
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            logger.warn("获取对象异常" ,e);
        }

        return fs;
    }
    public static void  closeFS(FileSystem fs){
        if (fs != null){
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
