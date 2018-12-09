package com.luyi.parser.modle.mr.newMember;

import com.luyi.common.KpiTypeEnum;
import com.luyi.etl.util.TimeUtil;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import com.luyi.util.JdbcUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class NewMemberReducer extends Reducer<StatsUserDimension,TimeOutputWritable,StatsUserDimension,ReduceOutputWritable> {
    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private StatsUserDimension k = new StatsUserDimension();
    private ReduceOutputWritable v = new ReduceOutputWritable();
    private Set<String> unique = new HashSet<String>();
    private Map<String,Long> mapout  = new HashMap<String, Long>();
    Connection conn = JdbcUtil.getConn();
    PreparedStatement ps= null;
    ResultSet rs = null;
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {
        try {
            for (TimeOutputWritable to : values){
                this.unique.add(to.getId());
                mapout.put(to.getId(),to.getTime());
            }
            Iterator<String> it = unique.iterator();
            while (it.hasNext()){
                String mid = it.next();
                ps = conn.prepareStatement("insert into member_info(" +
                        "member_id,last_visit_date,member_id_server_date,created) values(?,?,?,?) " +
                        "on duplicate key update last_visit_date = ?,member_id_server_date = ?,created = ?");
                ps.setString(1,mid);
                ps.setDate(2,new Date(mapout.get(mid)));
                ps.setLong(3,mapout.get(mid));
                ps.setDate(4,new Date(TimeUtil.getNewTime()));
                ps.setDate(5,new Date(mapout.get(mid)));
                ps.setLong(6,mapout.get(mid));
                ps.setDate(7,new Date(TimeUtil.getNewTime()));
                ps.execute();
            }
            MapWritable map = new MapWritable();
            map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
            this.v.setValue(map);
            //this.v.setKpi(KpiTypeEnum.NEW_USER);
            this.v.setKpi(KpiTypeEnum.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));

            context.write(key,this.v);
        } catch (SQLException e) {
            e.printStackTrace();
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
