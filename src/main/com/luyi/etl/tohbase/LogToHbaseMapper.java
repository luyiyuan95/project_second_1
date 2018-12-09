package com.luyi.etl.tohbase;

import com.luyi.etl.tohdfs.LogWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class LogToHbaseMapper extends Mapper<Object,Text,Text,LogWritable> {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMDddHHmmss");
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lines = line.split("\001");
        String rowkey = lines[10];

    }
}
