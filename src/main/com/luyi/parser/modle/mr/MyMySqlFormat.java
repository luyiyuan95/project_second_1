package com.luyi.parser.modle.mr;

import com.luyi.common.GlobaConstants;
import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.base.BaseDimension;
import com.luyi.parser.modle.dim.base.value.StatsBaseOutputWritable;
import com.luyi.parser.modle.service.DimensionOperateI;
import com.luyi.parser.modle.service.DimensionOperateImpl;
import com.luyi.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MyMySqlFormat extends OutputFormat<BaseDimension,StatsBaseOutputWritable> {
    private static final Logger logger = Logger.getLogger(MyMySqlFormat.class);


    public static  class MyMysqlRecordWritter extends RecordWriter<BaseDimension,StatsBaseOutputWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private DimensionOperateI operateI = null;

        private Map<KpiTypeEnum,PreparedStatement> cache =  new HashMap<KpiTypeEnum,PreparedStatement>(); //缓存kpi-ps
        private Map<KpiTypeEnum,Integer> batch = new HashMap<KpiTypeEnum,Integer>(); //缓存kpi-kpi对应的ps个数

        public MyMysqlRecordWritter(Connection conn, Configuration conf, DimensionOperateI operateI) {
            this.conn = conn;
            this.conf = conf;
            this.operateI = operateI;
        }

        @Override
        public void write(BaseDimension key, StatsBaseOutputWritable value) throws IOException, InterruptedException {
            try {
                PreparedStatement ps = null;
                if (key == null && value == null){
                    return;
                }
                KpiTypeEnum kpi = value.getKpi();
                int count = 1;
                if (this.cache.containsKey(kpi)){
                    ps = this.cache.get(kpi);
                    count = this.batch.get(kpi);
                    count++;
                }else {
                    String sql = this.conf.get(kpi.kpiName);
                    ps = this.conn.prepareStatement(sql);
                    this.cache.put(kpi,ps);
                }
                this.batch.put(kpi,count);

                String calssName = this.conf.get(GlobaConstants.PREFIX_WRITTER+kpi.kpiName);
                Class<?> calssz = Class.forName(calssName);
                ReduceOutputFormatI reduceOutputFormatI =(ReduceOutputFormatI) calssz.newInstance();
                reduceOutputFormatI.outputWritter(conf,key,value,ps,operateI);

                if (/*this.batch.size() % 50 == 0 ||*/ this.batch.get(kpi) % 50 ==0){
                    this.cache.get(kpi).executeBatch();
                    this.conn.commit();
                    this.cache.remove(kpi);
                }


            } catch (Exception e) {
               logger.warn("sql执行异常",e);
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            for (Map.Entry<KpiTypeEnum,PreparedStatement> en:this.cache.entrySet()){
                try {
                    en.getValue().executeBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            for (Map.Entry<KpiTypeEnum,PreparedStatement> e :this.cache.entrySet()){
                    try {
                        e.getValue().close();
                    } catch (SQLException e1) {

                    }finally {

                        if (conn != null){
                            JdbcUtil.close(conn,null,null);
                        }
                    }
                }
            }
    }

    @Override
    public RecordWriter<BaseDimension, StatsBaseOutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Connection conn = JdbcUtil.getConn();
        Configuration conf = taskAttemptContext.getConfiguration();
        DimensionOperateI operateI = new DimensionOperateImpl();
        return new MyMysqlRecordWritter(conn,conf,operateI);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(taskAttemptContext),taskAttemptContext);
    }
}
