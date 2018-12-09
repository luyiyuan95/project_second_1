package com.luyi.parser.modle.mr.newMember;

import com.luyi.common.DateTypeEnum;
import com.luyi.common.EventEnum;
import com.luyi.common.KpiTypeEnum;
import com.luyi.etl.util.TimeUtil;
import com.luyi.parser.modle.dim.StatsCommonDimension;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.BrowserDiemension;
import com.luyi.parser.modle.dim.base.DateDimension;
import com.luyi.parser.modle.dim.base.KpiDimension;
import com.luyi.parser.modle.dim.base.PlatformDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;

public class NewMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,TimeOutputWritable> {
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private static final StatsUserDimension k = new StatsUserDimension();
    private static final TimeOutputWritable v = new TimeOutputWritable();
    private KpiDimension newMemberKpi = new KpiDimension(KpiTypeEnum.NEW_MEMBER.kpiName);
    private KpiDimension newbrowserMemberKpi = new KpiDimension(KpiTypeEnum.BROWSER_NEW_MEMBER.kpiName);
    Connection conn = JdbcUtil.getConn();
    PreparedStatement ps= null;
    ResultSet rs = null;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            String[] fields = line.split("\001");
            String envent = fields[0];
            String mid = fields[8];
            String sTime = fields[15];
            String platfrome = fields[2];
            String browserName = fields[28];
            String browserVersion = fields[29];

            if (!envent.equals(EventEnum.PAGEVIEW.alias)){
                //logger.warn("事件不是lanuch事件: " + line);
                return;
            }
            if (StringUtils.isEmpty(sTime) || StringUtils.isEmpty(mid)){
                logger.warn("stime is null:"+sTime+" and uid is null:"+mid);
            }
            ps = conn.prepareStatement("select member_id from member_info ");
            rs = ps.executeQuery();
            boolean flag = false;
            while (rs.next()){
                if (mid.equals(rs.getString("member_id"))){
                    flag = true;
                    ps = conn.prepareStatement("update member_info set last_visit_date = ?,created = ?");
                    ps.setDate(1,new Date(Long.parseLong(sTime)));
                    ps.setDate(2,new Date(TimeUtil.getNewTime()));
                }
            }
            if (flag){return;}

            long longOfTime = Long.valueOf(sTime);

            DateDimension dateDimension = DateDimension.bulitDateDimension(longOfTime,DateTypeEnum.DAY);
            PlatformDimension platformDimension = new PlatformDimension(platfrome);
            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

            BrowserDiemension dafaultBrowser = new BrowserDiemension("","");

            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setKpiDimension(newMemberKpi);
            statsCommonDimension.setPlatformDimension(platformDimension);

            this.k.setStatsCommonDimension(statsCommonDimension);
            this.k.setBrowserDiemension(dafaultBrowser);

            this.v.setId(mid);
            this.v.setTime(Long.parseLong(sTime));

            context.write(this.k,this.v);

            BrowserDiemension browser = new BrowserDiemension(browserName,browserVersion);
            this.k.setBrowserDiemension(browser);
            statsCommonDimension.setKpiDimension(newbrowserMemberKpi);
            this.k.setStatsCommonDimension(statsCommonDimension);

            context.write(this.k,this.v);
        } catch (SQLException e) {
            logger.warn("新增会员sql语句异常",e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            if (conn != null){
                conn.close();
            }
            if (ps != null){
                ps.close();
            }
            if (rs != null){
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
