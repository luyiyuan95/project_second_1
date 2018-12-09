package com.luyi.parser.modle.mr.nu;

import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import com.luyi.parser.modle.service.DimensionOperateImpl;
import com.luyi.util.JdbcUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyNewUserOutputFormat extends FileOutputFormat<StatsUserDimension,ReduceOutputWritable> {
    @Override
    public RecordWriter<StatsUserDimension, ReduceOutputWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new NewUserRecordWriter();
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return super.getOutputCommitter(context);
    }

    class NewUserRecordWriter extends RecordWriter<StatsUserDimension,ReduceOutputWritable> {

        private Connection conn = null;
        public NewUserRecordWriter() {

            conn = JdbcUtil.getConn();
        }

        @Override
        public void write(StatsUserDimension statsUserDimension, ReduceOutputWritable reduceOutputWritable) throws IOException, InterruptedException {
            DimensionOperateImpl dimensionOperate = new DimensionOperateImpl();
            int date_dimension_id = 0,platform_dimension_id = 0;
            if (statsUserDimension.getStatsCommonDimension().getDateDimension() != null){
                date_dimension_id = dimensionOperate.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getDateDimension());
            }
            if (statsUserDimension.getStatsCommonDimension().getPlatformDimension() != null){
                platform_dimension_id = dimensionOperate.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getPlatformDimension());
            }
            if (statsUserDimension.getStatsCommonDimension().getKpiDimension() != null){
                dimensionOperate.getDimensionIdByDimension(statsUserDimension.getStatsCommonDimension().getKpiDimension());
            }
            int new_install_users = ((IntWritable)reduceOutputWritable.getValue().get(new Text(KpiTypeEnum.NEW_USER.kpiName))).get();
            for (Writable ele:reduceOutputWritable.getValue().keySet()) {
                String ele_key = ele.toString();
                int ele_value = ((IntWritable) reduceOutputWritable.getValue().get(ele)).get();
                if (ele_key.equals(KpiTypeEnum.NEW_USER)) {
                    new_install_users = ele_value;
                }
            }
            try {
                if (date_dimension_id !=0 && platform_dimension_id != 0){
                    int i = 0;
                    String insertsql = "insert into stats_user(date_dimension_id,platform_dimension_id,new_install_users) value (?,?,?) ";
                    PreparedStatement ps = conn.prepareStatement(insertsql);
                    ps.setInt(++i,date_dimension_id);
                    ps.setInt(++i,platform_dimension_id);
                    ps.setInt(++i,new_install_users);
                    ps.executeUpdate();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }
}
