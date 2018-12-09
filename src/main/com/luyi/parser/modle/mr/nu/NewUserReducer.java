package com.luyi.parser.modle.mr.nu;

import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewUserReducer extends Reducer<StatsUserDimension,TimeOutputWritable,StatsUserDimension,ReduceOutputWritable> {
    private static final Logger logger = Logger.getLogger(NewUserReducer.class);
    private StatsUserDimension k = new StatsUserDimension();
    private ReduceOutputWritable v = new ReduceOutputWritable();
    private Set<String> unique = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {
        for (TimeOutputWritable to : values){
            this.unique.add(to.getId());
        }
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(map);
        //this.v.setKpi(KpiTypeEnum.NEW_USER);
        this.v.setKpi(KpiTypeEnum.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));

        context.write(key,this.v);
    }
}
