package com.luyi.parser.modle.mr.nu;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MyNewUserRunner implements   Tool{
    private static final Logger logger = Logger.getLogger(MyNewUserRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new MyNewUserRunner(),args );
        } catch (Exception e) {
            logger.warn("运行to mysql runner异常",e);
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
        Job job = Job.getInstance(conf,"to new user mysql");
        job.setJarByClass(MyNewUserRunner.class);
        job.setMapperClass(MyNewUserMapper.class);
        job.setReducerClass(MyNewUserReducer.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputWritable.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(ReduceOutputWritable.class);
        job.setOutputFormatClass(MyNewUserOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path("/ods/12/03"));
        FileOutputFormat.setOutputPath(job, new Path("/ods/12/test"));
        return job.waitForCompletion(true) ? 0:1;
    }


}
