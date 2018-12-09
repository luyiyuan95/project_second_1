package com.luyi.etl.tohdfs;
import com.luyi.common.GlobaConstants;
import com.luyi.etl.util.FSUtil;
import com.luyi.etl.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class LogRunner  implements   Tool{
    private static final Logger logger = Logger.getLogger(LogRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new LogRunner(),args );
        } catch (Exception e) {
            logger.warn("运行to hdfs runner异常",e);
        }
    }
    @Override
    public void setConf(Configuration configuration) {
        this.conf = new Configuration();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        setArg(args,conf);
        Job job = Job.getInstance(conf,"to hdfs");
        job.setJarByClass(LogRunner.class);
        job.setMapperClass(LogMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        handleInputAndOutput(job);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0:1;
    }
    public void setArg(String[] args,Configuration conf){
        for (int i = 0;i<args.length;i++){
            if (args[i].equals("-d")){
                if (i+1 < args.length){
                    conf.set(GlobaConstants.RUNNING_DATE,args[i+1]);
                    break;
                }
            }
        }
        if (conf.get(GlobaConstants.RUNNING_DATE) == null){
            conf.set(GlobaConstants.RUNNING_DATE,TimeUtil.getYesterDate());
        }
    }
    private void handleInputAndOutput(Job job){
        String date = job.getConfiguration().get(GlobaConstants.RUNNING_DATE);
        String fields[] = date.split("-");

        try {
            Path inputpath = new Path("/logs/" + fields[1]+"/" + fields[2]);
            Path outputpath = new Path("/ods/" + fields[1] + "/" + fields[2]);
            FileInputFormat.setInputPaths(job,inputpath);
            FileSystem fs = FSUtil.getFS();
            if (fs.exists(outputpath)){
                fs.delete(outputpath,true);
            }
            FileOutputFormat.setOutputPath(job,outputpath);
        } catch (Exception e) {
            logger.warn("设置输入输出异常",e);
        }

    }
}
