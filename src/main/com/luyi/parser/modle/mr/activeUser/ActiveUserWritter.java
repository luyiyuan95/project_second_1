package com.luyi.parser.modle.mr.activeUser;

import com.luyi.common.GlobaConstants;
import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.BaseDimension;
import com.luyi.parser.modle.dim.base.value.StatsBaseOutputWritable;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import com.luyi.parser.modle.mr.ReduceOutputFormatI;
import com.luyi.parser.modle.service.DimensionOperateI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ActiveUserWritter implements ReduceOutputFormatI {


    @Override
    public void outputWritter(Configuration conf, BaseDimension key, StatsBaseOutputWritable value, PreparedStatement ps, DimensionOperateI operateI) {
        try {
            StatsUserDimension k = (StatsUserDimension) key;
            ReduceOutputWritable v = (ReduceOutputWritable)value;
            int activeUsers = ((IntWritable)v.getValue().get(new IntWritable(-1))).get();
            int i = 0;
            ps.setInt(++i,operateI.getDimensionIdByDimension(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,operateI.getDimensionIdByDimension(k.getStatsCommonDimension().getPlatformDimension()));
            if (v.getKpi().kpiName.equals(KpiTypeEnum.BROWSER_ACTIVE_USER.kpiName)){
                ps.setInt(++i,operateI.getDimensionIdByDimension(k.getBrowserDiemension()));
            }
            ps.setInt(++i,activeUsers);
            ps.setString(++i,conf.get(GlobaConstants.RUNNING_DATE));
            ps.setInt(++i,activeUsers);
            ps.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
