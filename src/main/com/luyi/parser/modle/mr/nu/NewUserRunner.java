package com.luyi.parser.modle.mr.nu;

import com.luyi.common.DateTypeEnum;
import com.luyi.common.GlobaConstants;
import com.luyi.etl.util.FSUtil;
import com.luyi.etl.util.TimeUtil;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.DateDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import com.luyi.parser.modle.mr.MyMySqlFormat;
import com.luyi.parser.modle.service.DimensionOperateI;
import com.luyi.parser.modle.service.DimensionOperateImpl;
import com.luyi.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NewUserRunner  implements   Tool{
    private static final Logger logger = Logger.getLogger(com.luyi.etl.tohdfs.LogRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new NewUserRunner(),args );
        } catch (Exception e) {
            logger.warn("运行new user  runner异常",e);
        }
    }
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.conf.addResource("insertquery-mapping.xml");
        this.conf.addResource("insertquery-writter.xml");
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        setArg(args, conf);
        Job job = Job.getInstance(conf, "new user");
        job.setJarByClass(NewUserRunner.class);

        job.setMapperClass(NewUserMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputWritable.class);

        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(ReduceOutputWritable.class);

        handleInputAndOutput(job);

        job.setOutputFormatClass(MyMySqlFormat.class);

        job.setNumReduceTasks(1);

        if (job.waitForCompletion(true)) {
            addTotalNewUser(job);
            addBrowserTotalNewUser(job);
            return 0;
        }else {
            return 1;
        }
    }
    public  void addTotalNewUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            DimensionOperateI operateI = new DimensionOperateImpl();
            String thisDate = job.getConfiguration().get(GlobaConstants.RUNNING_DATE);
            long lasttime = TimeUtil.string2Long(thisDate) - GlobaConstants.DAY_OF_YESTERDAY;
            DateDimension thisd = DateDimension.bulitDateDimension(TimeUtil.string2Long(thisDate),DateTypeEnum.DAY);
            DateDimension lastd = DateDimension.bulitDateDimension(lasttime,DateTypeEnum.DAY);
            int lastDid = -1;
            int thisDid = -1;
            lastDid = operateI.getDimensionIdByDimension(lastd);
            thisDid = operateI.getDimensionIdByDimension(thisd);

            Map<String,Integer> info = new ConcurrentHashMap<String, Integer>();

            conn = JdbcUtil.getConn();
            if (lastDid > 0){
                ps = conn.prepareStatement("select platform_dimension_id,total_install_users from stats_user where date_dimension_id = ?");
                ps.setInt(1,lastDid);
                rs = ps.executeQuery();
                while (rs.next()){
                    info.put(rs.getInt("platform_dimension_id")+"",rs.getInt("total_install_users"));
                }
            }
            if (thisDid > 0){
                ps = conn.prepareStatement("select platform_dimension_id,new_install_users from stats_user where date_dimension_id = ?");
                ps.setInt(1,thisDid);
                rs = ps.executeQuery();
                while (rs.next()){
                    String platform = rs.getInt("platform_dimension_id")+"";
                    int newUsers = rs.getInt("new_install_users");
                    if (info.containsKey(platform)){
                        newUsers += info.get(platform);
                    }
                    info.put(platform,newUsers);
                }
            }

            for (Map.Entry<String,Integer> en : info.entrySet()){
                ps = conn.prepareStatement("insert into stats_user(date_dimension_id ,platform_dimension_id,total_install_users,created) " +
                        "value (?,?,?,?) on duplicate key update total_install_users = ?");
                ps.setInt(1,thisDid);
                ps.setInt(2,Integer.parseInt(en.getKey()));
                ps.setInt(3,en.getValue());
                ps.setString(4,conf.get(GlobaConstants.RUNNING_DATE));
                ps.setInt(5,en.getValue());
                ps.execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,ps,rs);
        }
    }
    public  void addBrowserTotalNewUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            DimensionOperateI operateI = new DimensionOperateImpl();
            String thisDate = job.getConfiguration().get(GlobaConstants.RUNNING_DATE);
            long lasttime = TimeUtil.string2Long(thisDate) - GlobaConstants.DAY_OF_YESTERDAY;
            DateDimension thisd = DateDimension.bulitDateDimension(TimeUtil.string2Long(thisDate),DateTypeEnum.DAY);
            DateDimension lastd = DateDimension.bulitDateDimension(lasttime,DateTypeEnum.DAY);
            int lastDid = -1;
            int thisDid = -1;
            lastDid = operateI.getDimensionIdByDimension(lastd);
            thisDid = operateI.getDimensionIdByDimension(thisd);

            Map<String[],Integer> info = new ConcurrentHashMap<String[], Integer>();

            conn = JdbcUtil.getConn();

            if (lastDid > 0){
                ps = conn.prepareStatement("select platform_dimension_id,browser_dimension_id,total_install_users from stats_device_browser where date_dimension_id = ?");
                ps.setInt(1,lastDid);
                rs = ps.executeQuery();
                while (rs.next()){
                    String[] strarr = {rs.getInt("platform_dimension_id")+"",rs.getInt("browser_dimension_id")+""};
                    info.put(strarr,rs.getInt("total_install_users"));
                }
            }
            if (thisDid > 0){
                ps = conn.prepareStatement("select platform_dimension_id,browser_dimension_id,new_install_users from stats_device_browser where date_dimension_id = ?");
                ps.setInt(1,thisDid);
                rs = ps.executeQuery();
                while (rs.next()){
                    String platform = rs.getInt("platform_dimension_id")+"";
                    String browser = rs.getInt("browser_dimension_id")+"";
                    String[] strarr = {platform,browser};
                    int newUsers = rs.getInt("new_install_users");
                    if (info.containsKey(strarr)){
                        newUsers += info.get(strarr);
                    }
                    info.put(strarr,newUsers);
                }
            }

            for (Map.Entry<String[],Integer> en : info.entrySet()){
                ps = conn.prepareStatement("insert into stats_device_browser(date_dimension_id ,platform_dimension_id,browser_dimension_id,total_install_users,created) " +
                        "value (?,?,?,?,?) on duplicate key update total_install_users = ?");
                ps.setInt(1,thisDid);
                ps.setInt(2,Integer.parseInt(en.getKey()[0]));
                ps.setInt(3,Integer.parseInt(en.getKey()[1]));
                ps.setInt(4,en.getValue());
                ps.setString(5,conf.get(GlobaConstants.RUNNING_DATE));
                ps.setInt(6,en.getValue());
                ps.execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(conn,ps,rs);
        }
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
            Path inputpath = new Path("/ods/" + fields[1] + "/" + fields[2]);
            FileInputFormat.setInputPaths(job,inputpath);
        } catch (Exception e) {
            logger.warn("设置输入输出异常",e);
        }

    }
}
