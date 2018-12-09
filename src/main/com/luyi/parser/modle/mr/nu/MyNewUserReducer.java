package com.luyi.parser.modle.mr.nu;

import com.luyi.common.KpiTypeEnum;
import com.luyi.parser.modle.dim.StatsUserDimension;
import com.luyi.parser.modle.dim.base.value.map.TimeOutputWritable;
import com.luyi.parser.modle.dim.base.value.reduce.ReduceOutputWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MyNewUserReducer extends Reducer<StatsUserDimension,TimeOutputWritable,StatsUserDimension,ReduceOutputWritable> {
    Set<String> uuid=new HashSet<String>();
    ReduceOutputWritable reduceOutputWritable = new ReduceOutputWritable();
    MapWritable mapWritable = new MapWritable();
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputWritable> values, Context context) throws IOException, InterruptedException {

            for (TimeOutputWritable t:values){
                if (!uuid.contains(t.getId())){
                    uuid.add(t.getId());
                }
            }
            reduceOutputWritable.setKpi(KpiTypeEnum.NEW_USER);
            mapWritable.put(new Text(KpiTypeEnum.NEW_USER.kpiName),new IntWritable(uuid.size()));
            reduceOutputWritable.setValue(mapWritable);

            context.write(key,reduceOutputWritable);
    }
}
